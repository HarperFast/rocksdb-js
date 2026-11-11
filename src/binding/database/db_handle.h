#ifndef __DB_HANDLE_H__
#define __DB_HANDLE_H__

#include <atomic>
#include <memory>
#include <vector>
#include <utility>
#include <string>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <node_api.h>
#include "rocksdb/db.h"
#include "database/db_descriptor.h"
#include "options/db_options.h"
#include "transaction_log/transaction_log_store.h"
#include "core/platform.h"
#include "napi/helpers.h"
#include "napi/async.h"

namespace rocksdb_js {

// forward declarations
struct ColumnFamilyDescriptor;
struct DBDescriptor;

/**
 * Handle for a RocksDB database and the selected column family. This handle is
 * returned by the Registry and is used by the `Database` class.
 *
 * This handle is for convenience since passing around a shared pointer is a
 * pain.
 */
struct DBHandle final : Closable, AsyncWorkHandle, public std::enable_shared_from_this<DBHandle> {
	/**
	 * The RocksDB database descriptor
	 */
	std::shared_ptr<DBDescriptor> descriptor;

	// Accessed only by this handle's env thread; reset on reopen, never from
	// cross-env close. The registry owns TSFN release independently of this cache.
	std::shared_ptr<DBDescriptor::CommitCompletion> commitCompletion;

	/**
	 * The RocksDB column family descriptor.
	 */
	std::shared_ptr<ColumnFamilyDescriptor> columnDescriptor;

	/**
	 * The path of the database.
	 */
	std::string path;

	/**
	 * The database's resolved filesystem identity, copied from the descriptor at
	 * open and kept after close: `destroy()` on a closed handle must delete the
	 * database this handle actually opened, and re-resolving `path` then would
	 * follow a mapping that may have moved since (a repointed symlink, a
	 * relative path whose CWD changed). Empty only for a handle never opened.
	 */
	std::string identityPath;

	/**
	 * The mode of the last requested open, retained after close or open failure so destructive
	 * operations cannot bypass a read-only handle's guard.
	 */
	bool readOnly = false;

	/**
	 * Whether to disable WAL.
	 */
	bool disableWAL = false;

	/**
	 * The column family's verification-table participation, copied at open and
	 * replaced only by the next open, so this env's reads of it stay valid while
	 * a foreign close resets `columnDescriptor`. Null until the first open.
	 */
	std::shared_ptr<VerificationParticipation> participation;

	/** Opened with an explicit `verificationTable: false`. */
	bool verificationTableOptOut = false;

	/** Whether writes through this handle must take write intents. */
	bool guardsVerifiedWrites() const {
		return this->participation && this->participation->guardsWrites();
	}

	/**
	 * Whether a read through this handle may publish its version. Capture it
	 * before observing the slot: a read that started while the column family was
	 * converting must not publish after it completes, since a write without an
	 * intent may have committed under the slot it observed. Requires the
	 * caller's own `OperationGuard` (counted in `heldOperations`) or no
	 * conversion attempt (`heldOperations` < 0).
	 */
	bool publishesVerifiedReads(int heldOperations) const {
		if (!this->participation) {
			return false;
		}
		if (this->participation->publishing()) {
			return true;
		}
		return heldOperations >= 0 && this->participation->converting() &&
			this->descriptor->activateVerification(*this->participation, static_cast<uint32_t>(heldOperations));
	}

	/** Whether a slot may answer FRESH or take an explicit seed through this handle. */
	bool verifiesThroughTable() const {
		return this->participation && this->participation->verifies();
	}

	/**
	 * Immutable VerificationTable address components for this open lifecycle.
	 * These let VT-only fast paths avoid dereferencing teardown-owned native
	 * descriptors or registering as in-flight database operations.
	 */
	uint64_t verificationTableDbId = 0;
	uint32_t verificationTableColumnFamilyId = 0;

	/**
	 * Cancellation token handed to `rocksdb::CompactRangeOptions::canceled` by
	 * an async `compact()`/`clear()` issued through this handle -- the twin of
	 * `DBDescriptor::compactCancelRequested`, which serves the synchronous
	 * callers. Armed by `close()` (self-close) and, for a foreign close, by
	 * `finishClose()` before its first blocking step, via
	 * `cancelBlockingWork()`. Safe to arm from a thread that does not own this
	 * handle because this token is cleared by `DBRegistry::OpenDB()`; the
	 * descriptor's is never cleared.
	 *
	 * Why there are two tokens, why each is armed where it is, and what each of
	 * `test/fixtures/fork-compact-cancel-{sync,async,close,destroy}.mts` does
	 * and does not pin down: AGENTS.md invariant 6.
	 */
	std::atomic<bool> compactCancelRequested{false};

	void cancelBlockingWork() override { this->compactCancelRequested.store(true); }

	/**
	 * The node environment.
	 */
	napi_env env;
	std::thread::id ownerThreadId;

	/**
	 * A reference to the main `rocksdb_js` exports object. This is needed to
	 * get the `TransactionLog` class.
	 */
	napi_ref exportsRef;
	std::mutex closeMutex;

	/**
	 * The default transaction log store.
	 */
	std::weak_ptr<TransactionLogStore> defaultLog;

	/**
	 * A map of transaction log store names to `TransactionLog` JavaScript
	 * instances.
	 */
	std::unordered_map<std::string, napi_ref> logRefs;

	/**
	 * The shared default value buffer and its length.
	 */
	char* defaultValueBufferPtr = nullptr;
	size_t defaultValueBufferLength = 0;

	/**
	 * The shared default key buffer and its length.
	 */
	char* defaultKeyBufferPtr = nullptr;
	size_t defaultKeyBufferLength = 0;

	/**
	 * Pointer to a shared Uint32Array buffer used by iterators to communicate
	 * key and value lengths back to JavaScript without per-iteration NAPI
	 * property accesses. Layout: [keyLength, valueLength].
	 */
	char* iteratorStatePtr = nullptr;
	size_t iteratorStateLength = 0;

	DBHandle(napi_env env, napi_ref exportsRef);
	~DBHandle();

	/**
	 * Clears the handle's column family, compacting afterwards to reclaim space.
	 * `compactCanceled` is the token that compaction hands RocksDB; pass the
	 * descriptor's for a synchronous caller and this handle's for an async one
	 * (see `compactCancelRequested`).
	 */
	rocksdb::Status clear(std::atomic<bool>* compactCanceled);
	void close() override;
	napi_value get(
		napi_env env,
		rocksdb::Slice& key,
		napi_value resolve,
		napi_value reject,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr
	);

	rocksdb::ColumnFamilyHandle* getColumnFamilyHandle() const;
	std::string getColumnFamilyName() const;

	napi_value getStat(napi_env env, const std::string& statName);
	napi_value getStats(napi_env env, bool all);

	/**
	 * Aggregates the summarized `txnlog.*` statistics across all of this
	 * database's transaction logs into `total` (a sum of per-store
	 * TransactionLogStoreStats fields) and `logCount` (the number of logs).
	 */
	void collectTransactionLogSummary(TransactionLogStoreStats& total, uint64_t& logCount);

	void open(const std::string& path, const DBOptions& options);
	bool opened() const;
	void unrefLog(const std::string& name);
	napi_value useLog(napi_env env, napi_value jsDatabase, std::string& name);

	/**
	 * Releases every `logRefs` napi_ref and clears the map. Takes
	 * `closeMutex` itself; see `DBDescriptor::releaseLogRefsByEnv` for why
	 * this exists as a separate, env-cleanup-hook-driven path rather than
	 * folding into `close()`'s existing `ownerThreadId` guard (AGENTS.md
	 * invariant 18).
	 */
	void releaseLogRefs();

	// Closable override: releases `logRefs` only if `env` is this handle's own.
	void releaseEnvRefs(void* env) override {
		if (static_cast<napi_env>(env) == this->env) {
			this->releaseLogRefs();
		}
	}

private:
	// Same as `releaseLogRefs()`, but assumes `closeMutex` is already held --
	// `close()` calls this directly to avoid re-locking its own mutex.
	void releaseLogRefsLocked();

public:
};

} // namespace rocksdb_js

#endif
