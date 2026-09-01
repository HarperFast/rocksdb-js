#ifndef __LOCAL_STAMP_STATE_H__
#define __LOCAL_STAMP_STATE_H__

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <memory>
#include <thread>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include "rocksdb/db.h"
#include "core/local_stamp.h"

namespace rocksdb_js {

/**
 * Durable state and allocator for commit-time local mutation stamping
 * (docs/design/local-mutation-stamping.md §3.2/§3.7). Owned by the
 * DBDescriptor through a shared_ptr; the reserve-extension task and the
 * transaction-log stores hold their own references, so nothing here inflates
 * the descriptor's use_count (the HarperFast/rocksdb-js#672 purge hazard).
 *
 * All durable rows live in the hidden metadata column family
 * (STAMP_META_CF_NAME), so backup, backup stream, checkpoint, and restore
 * inherit them as ordinary CF data.
 */

constexpr const char* STAMP_META_CF_NAME = "__rocksdbjs.meta";

// Row keys within the metadata CF.
constexpr const char* STAMP_META_KEY_SCHEMA = "schema";
constexpr const char* STAMP_META_KEY_RESERVE = "stamp.reserve";
constexpr const char* STAMP_META_KEY_CLEAN_FLOOR = "stamp.cleanFloor";
constexpr const char* STAMP_META_KEY_LOG_DOMAIN = "stamp.logDomain";
// Per stamped column family, keyed by the CF's persistent RocksDB ID (never
// deleted; a recreated same-name CF has a new ID and starts unmarked).
constexpr const char* STAMP_META_KEY_CF_PREFIX = "stamp.cf.";
// Per log store: the domain generation the store last rotated for.
constexpr const char* STAMP_META_KEY_LOG_PREFIX = "stamp.log.";

// Schema row value: magic + schema version. A CF named STAMP_META_CF_NAME
// whose schema row is absent (and the CF non-empty) or different fails the
// open closed — collision detection, not authentication (the metadata CF is
// trusted content, same trust class as the database bytes).
constexpr const char* STAMP_META_SCHEMA_VALUE = "rocksdbjs.stamp.v1";

// Backup floor artifact: written into the transaction-log snapshot (and by
// restore as a pending copy in the destination) so a restored store never
// re-mints a stamp its logs carry. 4-byte token + BE double ceiling + BE
// uint64 complement of the ceiling bits (a torn write fails validation).
constexpr const char* STAMP_FLOOR_ARTIFACT_NAME = "STAMP_FLOOR";
constexpr const char* STAMP_FLOOR_ARTIFACT_PENDING_NAME = ".stamp-floor-pending";
constexpr const char* STAMP_FLOOR_ARTIFACT_TOKEN = "RJSF";
constexpr size_t STAMP_FLOOR_ARTIFACT_SIZE = 20;

// Reserve ceiling extension window and the proactive-extension margin.
constexpr double STAMP_RESERVE_WINDOW_MS = 300000.0; // 5 minutes
constexpr double STAMP_RESERVE_MARGIN_MS = 60000.0;  // extend when within 1 minute

struct LocalStampState final : std::enable_shared_from_this<LocalStampState> {
	// Claim state (float64 bit patterns; see core/local_stamp.h).
	std::atomic<uint64_t> watermark{0};
	std::atomic<uint64_t> reserve{0};
	// 0 = log key domain never flipped (origin/dormant); >0 = receiver domain.
	std::atomic<uint64_t> logDomainGeneration{0};

	// Durability plumbing. `db` is held so the metadata CF handle can never
	// outlive the database (destruction order: members destroyed in reverse
	// declaration order, so metaCf drops before db).
	std::shared_ptr<rocksdb::DB> db;
	std::shared_ptr<rocksdb::ColumnFamilyHandle> metaCf;

	// Serializes durable reserve extension (single-flight) and gates it against
	// close. shutdown() sets `closed` then acquires the mutex and joins the
	// extender thread, so an in-flight extension drains before the descriptor
	// tears down.
	std::mutex extendMutex;
	std::atomic<bool> closed{false};
	std::atomic<bool> extensionScheduled{false};

	// Proactive margin-triggered extension runs on this state-owned thread (a
	// plain std::thread like ParkTimeoutRegistry's — libuv structs are not part
	// of the N-API stable surface), holding only a shared_ptr to this state, so
	// nothing here touches the descriptor's use_count. Guarded by extendMutex.
	std::thread extenderThread;

	// IDs of stamped column families. Guarded by cfsMutex; only cold paths
	// consult it (hot write paths read ColumnFamilyDescriptor::commitStamping,
	// set once at open/creation). Mutated only when a new CF is created with
	// stamping on an already-activated database.
	std::mutex cfsMutex;
	std::unordered_map<uint32_t, std::string> stampedCfIds;

	struct LogGenerationRow {
		uint64_t generation;
		// The store's current sequence number recorded AFTER the rotation, so a
		// crash between the in-memory rotation and the first append cannot leave
		// a durable row certifying a rotation that never materialized: on reload
		// the on-disk sequence is below this and the store rotates again.
		uint64_t sequenceAfterRotation;
	};

	// Per log store: the domain generation each store last rotated for, loaded
	// at open; consumed once per store under its writeMutex (cold).
	std::unordered_map<std::string, LogGenerationRow> logGenerations;

	bool activated() const {
		return this->logDomainGeneration.load(std::memory_order_acquire) > 0;
	}

	bool isStampedCf(uint32_t cfId) {
		std::lock_guard<std::mutex> lock(this->cfsMutex);
		return this->stampedCfIds.find(cfId) != this->stampedCfIds.end();
	}

	void addStampedCf(uint32_t cfId, const std::string& name) {
		std::lock_guard<std::mutex> lock(this->cfsMutex);
		this->stampedCfIds.emplace(cfId, name);
	}

	/**
	 * Durably writes the marker row for a newly created stamped CF (sync).
	 */
	void persistCfMarker(uint32_t cfId, const std::string& name);

	/**
	 * Persists a log store's rotated-for domain generation together with the
	 * post-rotation sequence number (non-sync: a crash before durability only
	 * causes one spurious re-rotation on next load). Throws on an invalid store
	 * name — the loader rejects rows it could not have written.
	 */
	void persistLogGeneration(
		const std::string& storeName, uint64_t generation, uint64_t sequenceAfterRotation);

	/**
	 * Claims a stamp, durably extending the reserve if needed (never call while
	 * holding a transaction-log writeMutex — use ensureHeadroom() first there).
	 * Throws DBException on clock exhaustion or reserve-persistence failure.
	 */
	double claim(double candidate, bool candidateIsReceiverTime);

	/**
	 * Claim variant for use under a log store's writeMutex: never performs
	 * durable I/O. The caller must have called ensureHeadroom() outside the
	 * lock; if a concurrent claimer consumed the headroom anyway, this throws
	 * (the caller unlocks, re-ensures, and retries).
	 */
	StampClaim tryClaimNoExtend(double candidate, bool candidateIsReceiverTime);

	/**
	 * Ensures the durable ceiling covers what a claim of `candidate` could
	 * actually produce — applying the same provenance/skew rule as the claim
	 * itself, so a caller-supplied far-future timestamp (which the claim would
	 * re-stamp at receiver time) can never durably poison the ceiling — plus
	 * the margin; extends synchronously when required, schedules a proactive
	 * extension when merely near the margin. Safe to call from any thread NOT
	 * holding a log store's writeMutex.
	 */
	void ensureHeadroom(double candidate, bool candidateIsReceiverTime);

	/** Margin-triggered single-flight proactive reserve extension. */
	void scheduleProactiveExtensionIfNearMargin();

	/**
	 * Durably persists a new ceiling >= target. Synchronous write
	 * (WriteOptions.sync). Throws on failure or after close.
	 */
	void extendReserve(double target);

	/**
	 * Proactive renewal: pushes the ceiling a full window past
	 * max(now, watermark) even though no claim needs it yet — extendReserve's
	 * ensure-coverage semantics would no-op (its target sits below the current
	 * ceiling by construction on the margin path). Never throws.
	 */
	void renewReserve();

	/**
	 * Writes the clean-close floor row (the exact watermark) — called from
	 * DBDescriptor::finishClose after commits have drained.
	 */
	void persistCleanCloseFloor();

	/**
	 * Marks close: no further durable writes; drains any in-flight extension.
	 */
	void shutdown();
};

/**
 * Parsed metadata-CF contents, loaded at open.
 */
struct StampMetaContents {
	bool schemaValid = false;
	bool schemaPresent = false;
	bool empty = true;
	double reserve = 0.0;
	std::optional<double> cleanFloor;
	uint64_t logDomainGeneration = 0;
	std::unordered_map<uint32_t, std::string> stampedCfIds;
	std::unordered_map<std::string, LocalStampState::LogGenerationRow> logGenerations;
};

/**
 * Loads and validates the metadata CF. Values failing shape validation
 * (non-finite, out of domain) throw — a crafted or corrupt row must fail the
 * open closed rather than seed a bogus clock.
 */
StampMetaContents loadStampMeta(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* metaCf);

/**
 * Reads the backup floor artifacts (STAMP_FLOOR and the restore's pending
 * copy) from the given transaction-log directories, returning the highest
 * valid ceiling found. A present-but-corrupt artifact throws (fail closed:
 * it exists precisely to stop a restored store from re-minting stamps).
 */
double readStampFloorArtifacts(const std::vector<std::string>& logDirs);

} // namespace rocksdb_js

#endif
