#ifndef __DB_SETTINGS_H__
#define __DB_SETTINGS_H__

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <node_api.h>
#include <thread>
#include "rocksdb/cache.h"
#include "rocksdb/write_buffer_manager.h"
#include "core/verification_table.h"
#include "core/wbm_stall_watchdog.h"

namespace rocksdb_js {

/**
 * Live state of the process-wide `WriteBufferManager`, as reported by
 * `db.getStats()` / `db.getStat()` and `RocksDatabase.getWriteBufferManagerStats()`.
 *
 * The column-family inventory is filled in only when the caller asks for it: it
 * walks the database registry, while everything above it is a handful of atomic
 * loads. `getStats()` is a scrape path and takes the cheap half.
 */
struct WriteBufferManagerStats final {
	bool enabled = false;
	uint64_t bufferSize = 0;
	uint64_t memoryUsage = 0;
	uint64_t mutableMemoryUsage = 0;
	bool allowStall = false;
	bool costToCache = false;
	bool stallActive = false;
	uint64_t stallActiveMs = 0;
	bool watchdogRunning = false;
	uint64_t columnFamilies = 0;
	std::map<int64_t, uint64_t> maxWriteBufferSizeToMaintain;
	/**
	 * False when the registry lock was held by a close that is itself wedged on
	 * the stall being diagnosed, so the two fields above are empty rather than
	 * measured. A diagnostic that blocks on that lock answers nothing at all.
	 */
	bool inventoryAvailable = true;
};

/**
 * Stores the global settings for RocksDB databases as well as various global
 * state.
 */
class DBSettings final {
private:
	DBSettings(); // private constructor

	size_t blockCacheSize;
	std::shared_ptr<rocksdb::Cache> blockCache;

	// Total memory limit (bytes) shared across all databases for active and
	// immutable memtables. 0 disables the manager (each database uses its own
	// unbounded memtable budget).
	//
	// Atomic so JS-thread writes in Config() are safely visible to libuv
	// worker threads that read it from getWriteBufferManager() during async
	// database open. Not protected by writeBufferManagerMutex because the
	// "should I attach a WBM at all?" check needs to be lock-free fast path.
	std::atomic<size_t> writeBufferManagerSize;

	// When true, memtable memory is "charged" against the shared block cache.
	// Active memtables and the block cache then draw from a single pool — the
	// cache shrinks during write bursts and reclaims room as memtables flush.
	// Has no effect when the block cache is disabled (size 0).
	//
	// Atomic for the same reason as writeBufferManagerSize — concurrent
	// reads from worker threads vs writes from the JS thread.
	std::atomic<bool> writeBufferManagerCostToCache;

	// When true, writes stall once the manager's buffer_size is exceeded
	// instead of allowing memtables to grow past the limit. Off by default to
	// favor write throughput over hard memory bounding.
	std::atomic<bool> writeBufferManagerAllowStall;

	std::shared_ptr<rocksdb::WriteBufferManager> writeBufferManager;
	std::mutex writeBufferManagerMutex;

	// The manager, published with release ordering once it is fully constructed
	// and never reset — a runtime `writeBufferManagerSize: 0` is a "no new
	// attachments" signal, not a teardown, so the raw pointer stays valid for the
	// process lifetime. Read paths take one acquire load instead of
	// writeBufferManagerMutex: a metrics scrape must not contend with config()
	// or a database open.
	std::atomic<rocksdb::WriteBufferManager*> writeBufferManagerPtr{nullptr};

	std::atomic<uint64_t> writeBufferManagerStallActiveMs{0};
	std::atomic<bool> writeBufferManagerWatchdogRunning{false};

	// Watchdog lifecycle. The start path runs under databasesMutex ->
	// writeBufferManagerMutex (DBRegistry::OpenDB holds the former across
	// DBDescriptor::open), so watchdogMutex is the innermost lock and the
	// watchdog thread must never hold it while taking either of the other two —
	// it drops watchdogMutex before every sample, and the sample's inventory walk
	// is the one place it takes databasesMutex.
	std::thread watchdogThread;
	std::mutex watchdogMutex;
	std::condition_variable watchdogCv;
	bool watchdogStarted = false;
	bool watchdogStopRequested = false;
	// Bumped on every start. A thread whose generation is stale exits even if a
	// new start has already cleared `watchdogStopRequested`: the joiner releases
	// watchdogMutex before `join()`, so without this the retiring thread can
	// observe the reset flag and loop forever with its joiner blocked on it.
	uint64_t watchdogGeneration = 0;

	void ensureWriteBufferManagerWatchdog();
	void runWriteBufferManagerWatchdog(uint64_t generation);
	void sampleWriteBufferManagerStall(WbmStallWatchdogState& state, uint64_t thresholdMs);

	bool compactOnClose;

	// Number of slots requested for the verification table. Default 128K
	// (1 MB at 8 bytes per slot). 0 disables the table. Configurable via
	// RocksDatabase.config({ verificationTableEntries }) only before the
	// table is first materialized; after that, attempts to change it throw.
	size_t verificationTableEntries;

	// Random hash seed mixed into verification-table slot indices.
	uint64_t verificationTableSeed;

	std::unique_ptr<VerificationTable> verificationTable;
	std::mutex verificationTableMutex;

public:
	/**
	 * Returns the process-wide DBSettings singleton.
	 *
	 * Uses C++11 magic-static initialization so concurrent first-callers
	 * from libuv worker threads don't race during construction.
	 */
	static DBSettings& getInstance() {
		static DBSettings instance;
		return instance;
	}

	size_t getBlockCacheSize() const {
		return blockCacheSize;
	}

	std::shared_ptr<rocksdb::Cache> getBlockCache();

	size_t getWriteBufferManagerSize() const {
		return writeBufferManagerSize.load(std::memory_order_relaxed);
	}

	bool getWriteBufferManagerCostToCache() const {
		return writeBufferManagerCostToCache.load(std::memory_order_relaxed);
	}

	bool getWriteBufferManagerAllowStall() const {
		return writeBufferManagerAllowStall.load(std::memory_order_relaxed);
	}

	std::shared_ptr<rocksdb::WriteBufferManager> getWriteBufferManager();

	/**
	 * Returns the manager if one has already been created, without creating it.
	 * Lock-free and safe from any thread — a stats read must never materialize
	 * the manager as a side effect (the peer of `getVerificationTableRaw()`,
	 * without its mutex).
	 */
	rocksdb::WriteBufferManager* getWriteBufferManagerRaw() const {
		return this->writeBufferManagerPtr.load(std::memory_order_acquire);
	}

	/**
	 * Samples the manager's live state. `includeColumnFamilies` additionally walks
	 * the database registry for the attached, writable column-family inventory,
	 * which is O(open column families) — leave it off on scrape paths.
	 */
	WriteBufferManagerStats getWriteBufferManagerStats(bool includeColumnFamilies);

	/**
	 * Signals the stall watchdog to stop without waiting for it. Split from the
	 * join so teardown can shut databases down (and flush) while the watchdog is
	 * still winding down: its warn line goes to stderr, which can block on a full
	 * pipe, and a logging stall must never sit in front of the flush path.
	 */
	void requestWriteBufferManagerWatchdogStop();

	/** Joins the stall watchdog. Idempotent, and restartable afterwards. */
	void joinWriteBufferManagerWatchdog();

	/**
	 * Joins the watchdog if the module's cleanup hook never ran. `process.exit()`
	 * skips N-API env cleanup, and destroying a joinable `std::thread` calls
	 * `std::terminate()` — an observability feature must not turn a clean exit
	 * into SIGABRT. Mirrors `~CommitWorker`.
	 */
	~DBSettings();

	inline bool getCompactOnClose() const {
		return compactOnClose;
	}

	/**
	 * Returns the global verification table, materializing it on first call.
	 * After the first call, the table size is fixed for the process lifetime.
	 * Returns null when the table is disabled (entries == 0).
	 */
	VerificationTable* getVerificationTable();

	/**
	 * Returns the verification table if it has already been materialized,
	 * without creating it. Safe to call from any thread. Returns null when
	 * the table has not yet been created or is disabled.
	 *
	 * Use this in hot paths (e.g. transaction commit) where materializing
	 * the table would trigger the config-freeze check unexpectedly.
	 */
	VerificationTable* getVerificationTableRaw();

	static napi_value Config(napi_env env, napi_callback_info info);

	static napi_value GetWriteBufferManagerStats(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);
};

} // namespace rocksdb_js

#endif