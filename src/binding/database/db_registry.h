#ifndef __DB_REGISTRY_H__
#define __DB_REGISTRY_H__

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include "database/db_descriptor.h"
#include "database/db_handle.h"
#include "transaction/transaction.h"

namespace rocksdb_js {

/**
 * Lightweight key for the registry map. Composed of the fields that uniquely
 * identify a database instance: path, whether it was opened read-only, and the
 * secondary workspace path (empty for non-secondary opens — a plain read-only
 * open and a secondary open of the same path are different RocksDB instance
 * kinds and must not share a descriptor, while two secondary opens with
 * different workspaces are distinct secondary instances by design; the
 * workspace is stored resolved — `Database::Open` runs it through
 * `resolveIdentityPath` — so two spellings of one directory are one key).
 * Using a dedicated key type lets callers look up entries before a
 * `DBDescriptor` has been opened. Every purge/close path must reconstruct the FULL key (use
 * `descriptorKey()`) — a partial key silently misses the entry and leaks the
 * descriptor and its open RocksDB.
 */
struct DBKey {
	/**
	 * The database's resolved filesystem identity (`DBDescriptor::identityPath`),
	 * never a caller's raw spelling: two spellings of one directory would
	 * otherwise hold two descriptors for it, and a second secondary instance
	 * would open one workspace twice (invariant 19).
	 */
	std::string path;
	bool readOnly;
	std::string secondaryPath;

	bool operator==(const DBKey& other) const {
		return path == other.path && readOnly == other.readOnly &&
			secondaryPath == other.secondaryPath;
	}
};

struct DBKeyHash {
	size_t operator()(const DBKey& key) const {
		return std::hash<std::string>()(key.path) ^
		       std::hash<bool>()(key.readOnly) ^
		       (std::hash<std::string>()(key.secondaryPath) << 1);
	}
};

/**
 * The one construction point for a live descriptor's registry key. Purge and
 * close paths must use this rather than hand-building a partial key.
 */
inline DBKey descriptorKey(const DBDescriptor& descriptor) {
	return DBKey{descriptor.identityPath, descriptor.readOnly, descriptor.secondaryPath};
}

/**
 * Entry in the database registry containing both the descriptor and a condition
 * variable for coordinating access to that specific path.
 */
struct DBRegistryEntry final {
	std::shared_ptr<DBDescriptor> descriptor;
	std::shared_ptr<std::condition_variable> condition;
	// Set when a close (self, foreign, or destroy-driven) fails. The entry is
	// left in the map instead of erased -- "quarantined" -- so the failure is
	// visible (registryStatus(), a `database:closeFailed` event) and a caller
	// can retry via shutdown()/destroy() rather than the path silently
	// reopening over unflushed data. `closeRetrying` is true while a retry
	// attempt is in flight, so a second concurrent retry does not double-claim
	// the same finishClose() stage.
	std::string closeError;
	bool closeRetrying = false;
	// The opening caller's spelling of the path (`DBDescriptor::path`), kept on
	// the entry so a tombstone -- an entry whose descriptor is gone because a
	// destroy's physical cleanup failed -- can still report the spelling the
	// caller supplied rather than the resolved identity the key carries. A
	// caller matching `registryStatus().path` against the path it opened would
	// otherwise miss wherever the two spell the same directory differently
	// (macOS `/var` vs `/private/var`, a symlink, a relative path). Empty only
	// for a path no descriptor in this process ever opened.
	std::string reportedPath;

	// Default constructor
	DBRegistryEntry() : condition(std::make_shared<std::condition_variable>()) {}

	DBRegistryEntry(std::shared_ptr<DBDescriptor> desc)
		: descriptor(std::move(desc)), condition(std::make_shared<std::condition_variable>()) {}
};


struct DBHandleParams final {
	std::shared_ptr<DBDescriptor> descriptor;
	std::shared_ptr<ColumnFamilyDescriptor> columnDescriptor;

	DBHandleParams(std::shared_ptr<DBDescriptor> descriptor, std::shared_ptr<ColumnFamilyDescriptor> columnDescriptor)
		: descriptor(std::move(descriptor)), columnDescriptor(std::move(columnDescriptor)) {}
};

/**
 * Outcome of a close-family call (`CloseDB`, `PurgeIfUnreferenced`). `error` is
 * empty on a clean close. `quarantined` means the entry was left in the
 * registry (with `error` recorded) instead of erased, so it can be retried via
 * `shutdown()`/`destroy()`.
 */
struct CloseResult final {
	std::string error;
	bool quarantined = false;
};

/**
 * Tracks all RocksDB databases instances using a RocksDBDescriptor that
 * contains a weak reference to the database and column families.
 */
class DBRegistry final {
private:
	/**
	 * Private constructor.
	 */
	DBRegistry() = default;

	/**
	 * Map of database path to registry entry containing both the descriptor
	 * and condition variable for that path.
	 */
	std::unordered_map<DBKey, DBRegistryEntry, DBKeyHash> databases;

	/**
	 * Mutex to protect the databases map.
	 */
	std::mutex databasesMutex;

	/**
	 * Serializes concurrent `Shutdown()` calls (an explicit JS `shutdown()`
	 * racing another, or the env-cleanup hook racing a JS caller) so two
	 * threads never run the claim/close/retry loop over the same entries at
	 * once. Bounded by `DBSettings::lifecycleWaitSeconds` like every other
	 * lifecycle wait below -- see `DestroyDB`.
	 */
	std::timed_mutex shutdownMutex;

	/**
	 * Paths currently mid-`DestroyDB`, from the moment every descriptor for the
	 * path is claimed+closed+erased until physical deletion (which can be
	 * artificially slow, or genuinely slow for a large directory) finishes.
	 * That window runs WITHOUT `databasesMutex` held -- deleting files is I/O,
	 * and a plain lock_guard across it would serialize every open/close in the
	 * process behind one directory's removal -- so by the time it starts, the
	 * registry already has no entry for this path to gate a new OpenDB on.
	 * This set is the substitute gate: OpenDB/Shutdown check it and wait on
	 * `lifecycleCondition` (a single condvar for all paths -- contention here
	 * is rare enough that a per-path one is not worth the bookkeeping) rather
	 * than proceeding as if the path were free.
	 */
	std::condition_variable lifecycleCondition;
	std::unordered_set<std::string> destroyingPaths;

	/**
	 * The singleton instance of the registry.
	 */
	static std::unique_ptr<DBRegistry> instance;

public:
	static CloseResult CloseDB(const std::shared_ptr<DBHandle> handle);

	/**
	 * Counts the live column families that draw on `wbm`, grouped by their
	 * effective `max_write_buffer_size_to_maintain` — the inventory the stall
	 * report needs to explain a full budget.
	 *
	 * Only descriptors that attached this exact manager are counted. A dropped
	 * column family still charging the manager through a live handle is counted
	 * too, and stops being counted once that last handle closes.
	 *
	 * Reads only cached integers under `databasesMutex` -> `columnsMutex` (the
	 * order `OpenDB` already establishes) and copies no `shared_ptr` out, so it
	 * cannot inflate a descriptor's use count and make a racing close skip its
	 * purge. Safe to call from a non-JS thread.
	 *
	 * Both mutex levels are acquired with `try_lock`, returning false rather than
	 * waiting: registry teardown or column-family creation can hold them across
	 * RocksDB work that waits out a write stall, so blocking here would silence
	 * the stall alarm during exactly the incident it exists to report.
	 */
	static bool CollectWriteBufferManagerInventory(
		const rocksdb::WriteBufferManager* wbm,
		uint64_t& columnFamilies,
		std::map<int64_t, uint64_t>& maxWriteBufferSizeToMaintain
	);
#ifdef DEBUG
	static void DebugLogDescriptorRefs();
#endif
	static void DestroyDB(const std::string& path);
	static void Init(napi_env env, napi_value exports);
	static void OpenDB(
		const std::shared_ptr<DBHandle>& handle,
		const std::string& path,
		const DBOptions& options
	);
	static void PurgeAll();
	static CloseResult PurgeIfUnreferenced(const DBKey& key);
	static napi_value RegistryStatus(napi_env env, napi_callback_info info);
	static void CloseTransactionsByEnv(napi_env env);
	static void RemoveListenersByEnv(napi_env env);
	static void ReleaseCommitCompletionsByEnv(napi_env env);
	static void ReleaseParkTimeoutsByEnv(napi_env env);
	static void ReleaseLogRefsByEnv(napi_env env);
	static void Shutdown();
	/**
	 * Releases every remaining registry entry. Called from the module env
	 * cleanup hook after `Shutdown()`, i.e. while the process is still running
	 * normally. Nothing may keep a `rocksdb::DB` alive past that point: the
	 * registry singleton is a namespace-scope static, so anything still in the
	 * map is destroyed from an `atexit` handler, and closing a RocksDB database
	 * there runs `DBImpl::CancelAllBackgroundWork()` after RocksDB's own
	 * function-local statics (the `PeriodicTaskScheduler` timer and its
	 * `port::Mutex`) have already been destroyed -- which aborts the process in
	 * `port::Mutex::Lock()` with `pthread lock: Invalid argument`.
	 *
	 * `Shutdown()` normally empties the map on its own; a descriptor whose
	 * close-time flush failed is deliberately quarantined instead, and at
	 * process exit there is no later `shutdown()`/`destroy()` to retry it.
	 */
	static void Teardown();
	static size_t Size();
};

} // namespace rocksdb_js

#endif
