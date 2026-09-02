#ifndef __DB_REGISTRY_H__
#define __DB_REGISTRY_H__

#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include "database/db_descriptor.h"
#include "database/db_handle.h"
#include "transaction/transaction.h"

namespace rocksdb_js {

/**
 * Lightweight key for the registry map. Composed of the two fields that
 * uniquely identify a database instance: path and whether it was opened
 * read-only. Using a dedicated key type lets callers look up entries before
 * a `DBDescriptor` has been opened.
 */
struct DBKey {
	std::string path;
	bool readOnly;

	bool operator==(const DBKey& other) const {
		return path == other.path && readOnly == other.readOnly;
	}
};

struct DBKeyHash {
	size_t operator()(const DBKey& key) const {
		return std::hash<std::string>()(key.path) ^
		       std::hash<bool>()(key.readOnly);
	}
};

/**
 * Entry in the database registry containing both the descriptor and a condition
 * variable for coordinating access to that specific path.
 */
struct DBRegistryEntry final {
	std::shared_ptr<DBDescriptor> descriptor;
	std::shared_ptr<std::condition_variable> condition;
	std::string closeError;
	bool closeRetrying = false;

	// Default constructor
	DBRegistryEntry() : condition(std::make_shared<std::condition_variable>()) {}

	DBRegistryEntry(std::shared_ptr<DBDescriptor> desc)
		: descriptor(std::move(desc)), condition(std::make_shared<std::condition_variable>()) {}
};

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
	std::timed_mutex shutdownMutex;
	bool shutdownInProgress = false;
	// Destruction owns a physical path across every (path, readOnly) entry.
	// Waiters must re-resolve databases after every wake because the closer can
	// erase the node while the mutex is released.
	std::condition_variable lifecycleCondition;
	std::unordered_set<std::string> destroyingPaths;

	/**
	 * The singleton instance of the registry.
	 */
	static std::unique_ptr<DBRegistry> instance;

public:
	static CloseResult CloseDB(const std::shared_ptr<DBHandle> handle);
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
	static CloseResult PurgeIfUnreferenced(const std::string& path, bool readOnly);
	static napi_value RegistryStatus(napi_env env, napi_callback_info info);
	static void CloseTransactionsByEnv(napi_env env);
	static void RemoveListenersByEnv(napi_env env);
	static void ReleaseCommitCompletionsByEnv(napi_env env);
	static void ReleaseParkTimeoutsByEnv(napi_env env);
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
