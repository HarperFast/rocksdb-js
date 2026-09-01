#ifndef __DB_REGISTRY_H__
#define __DB_REGISTRY_H__

#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
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
 * different workspaces are distinct secondary instances by design). Using a
 * dedicated key type lets callers look up entries before a `DBDescriptor` has
 * been opened. Every purge/close path must reconstruct the FULL key (use
 * `descriptorKey()`) — a partial key silently misses the entry and leaks the
 * descriptor and its open RocksDB.
 */
struct DBKey {
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
	return DBKey{descriptor.path, descriptor.readOnly, descriptor.secondaryPath};
}

/**
 * Entry in the database registry containing both the descriptor and a condition
 * variable for coordinating access to that specific path.
 */
struct DBRegistryEntry final {
	std::shared_ptr<DBDescriptor> descriptor;
	std::shared_ptr<std::condition_variable> condition;

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
	 * The singleton instance of the registry.
	 */
	static std::unique_ptr<DBRegistry> instance;

public:
	static void CloseDB(const std::shared_ptr<DBHandle> handle);
#ifdef DEBUG
	static void DebugLogDescriptorRefs();
#endif
	static void DestroyDB(const std::string& path);
	static void Init(napi_env env, napi_value exports);
	static std::unique_ptr<DBHandleParams> OpenDB(const std::string& path, const DBOptions& options);
	static void PurgeAll();
	static void PurgeIfUnreferenced(const DBKey& key);
	static napi_value RegistryStatus(napi_env env, napi_callback_info info);
	static void CloseTransactionsByEnv(napi_env env);
	static void RemoveListenersByEnv(napi_env env);
	static void ReleaseCommitCompletionsByEnv(napi_env env);
	static void ReleaseParkTimeoutsByEnv(napi_env env);
	static void Shutdown();
	static size_t Size();
};

} // namespace rocksdb_js

#endif
