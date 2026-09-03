#ifndef __DB_REGISTRY_H__
#define __DB_REGISTRY_H__

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
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

	/**
	 * Counts the live column families that draw on `wbm`, grouped by their
	 * effective `max_write_buffer_size_to_maintain` — the inventory the stall
	 * report needs to explain a full budget.
	 *
	 * Only descriptors that attached this exact manager and are writable are
	 * counted: a database opened before the manager was configured, or read-only,
	 * cannot account for its memory, and including it would report a retention
	 * distribution that is not the one holding the budget.
	 *
	 * Reads only cached integers under `databasesMutex` -> `columnsMutex` (the
	 * order `OpenDB` already establishes) and copies no `shared_ptr` out, so it
	 * cannot inflate a descriptor's use count and make a racing close skip its
	 * purge. Safe to call from a non-JS thread.
	 */
	static void CollectWriteBufferManagerInventory(
		const rocksdb::WriteBufferManager* wbm,
		uint64_t& columnFamilies,
		std::map<int64_t, uint64_t>& maxWriteBufferSizeToMaintain
	);
#ifdef DEBUG
	static void DebugLogDescriptorRefs();
#endif
	static void DestroyDB(const std::string& path);
	static void Init(napi_env env, napi_value exports);
	static std::unique_ptr<DBHandleParams> OpenDB(const std::string& path, const DBOptions& options);
	static void PurgeAll();
	static void PurgeIfUnreferenced(const std::string& path, bool readOnly);
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
