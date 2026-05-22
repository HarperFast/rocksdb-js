#ifndef __TRANSACTION_LOG_STORE_REGISTRY_H__
#define __TRANSACTION_LOG_STORE_REGISTRY_H__

#include <chrono>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <string>
#include <node_api.h>
#include "transaction_log_store.h"

namespace rocksdb_js {

/**
 * Configuration for transaction log stores associated with a database path.
 */
struct TransactionLogStoreConfig final {
	/**
	 * The path to the transaction logs directory.
	 */
	std::string transactionLogsPath;

	/**
	 * The threshold for the transaction log file's last modified time to be
	 * older than the retention period before it is rotated to the next sequence
	 * number. A threshold of 0 means ignore age check.
	 */
	float transactionLogMaxAgeThreshold;

	/**
	 * The maximum size of a transaction log file in bytes before it is rotated
	 * to the next sequence number. A max size of 0 means no limit.
	 */
	uint32_t transactionLogMaxSize;

	/**
	 * The retention period of transaction logs in milliseconds.
	 */
	std::chrono::milliseconds transactionLogRetentionMs;
};

/**
 * Entry in the transaction log store registry containing the stores map,
 * configuration, and reference count.
 */
struct TransactionLogStoreRegistryEntry final {
	/**
	 * Map of transaction log store name to store instance.
	 */
	std::map<std::string, std::shared_ptr<TransactionLogStore>> stores;

	/**
	 * Mutex to protect the stores map.
	 */
	std::mutex storesMutex;

	/**
	 * Configuration for this database path's transaction log stores.
	 */
	TransactionLogStoreConfig config;

	/**
	 * Reference count tracking how many DBDescriptors are using this entry.
	 */
	size_t refCount = 0;

	TransactionLogStoreRegistryEntry() = default;

	TransactionLogStoreRegistryEntry(const TransactionLogStoreConfig& cfg)
		: config(cfg), refCount(1) {}
};

/**
 * Global registry that manages transaction log stores by database path.
 * This ensures that transaction log stores are shared across multiple
 * DBDescriptors (e.g., write and read-only) for the same database path.
 */
class TransactionLogStoreRegistry final {
private:
	/**
	 * Private constructor for singleton.
	 */
	TransactionLogStoreRegistry() = default;

	/**
	 * Map of database path to registry entry. Uses shared_ptr so entries can
	 * be safely accessed after releasing entriesMutex by taking a copy.
	 */
	std::unordered_map<std::string, std::shared_ptr<TransactionLogStoreRegistryEntry>> entries;

	/**
	 * Mutex to protect the entries map.
	 */
	std::mutex entriesMutex;

	/**
	 * The singleton instance of the registry.
	 */
	static std::unique_ptr<TransactionLogStoreRegistry> instance;

public:
	/**
	 * Initializes the singleton instance.
	 */
	static void Init();

	/**
	 * Shuts down the registry and closes all stores.
	 */
	static void Shutdown();

	/**
	 * Registers a DBDescriptor for the given database path. Increments the
	 * reference count for the path. If this is the first descriptor for the
	 * path, creates a new entry with the given configuration.
	 *
	 * @param dbPath The database path.
	 * @param config The transaction log store configuration.
	 */
	static void Register(const std::string& dbPath, const TransactionLogStoreConfig& config);

	/**
	 * Unregisters a DBDescriptor for the given database path. Decrements the
	 * reference count. If the reference count reaches zero, closes and removes
	 * all transaction log stores for that path.
	 *
	 * @param dbPath The database path.
	 */
	static void Unregister(const std::string& dbPath);

	/**
	 * Discovers existing transaction log stores in the transaction logs
	 * directory for the given database path.
	 *
	 * @param dbPath The database path.
	 */
	static void DiscoverStores(const std::string& dbPath);

	/**
	 * Resolves (finds or creates) a transaction log store by name for the
	 * given database path.
	 *
	 * @param dbPath The database path.
	 * @param name The name of the transaction log store.
	 * @returns The transaction log store.
	 */
	static std::shared_ptr<TransactionLogStore> ResolveStore(
		const std::string& dbPath,
		const std::string& name
	);

	/**
	 * Lists all transaction log store names for the given database path.
	 *
	 * @param env The N-API environment.
	 * @param dbPath The database path.
	 * @returns A JavaScript array of store names.
	 */
	static napi_value ListStores(napi_env env, const std::string& dbPath);

	/**
	 * Purges transaction logs for the given database path.
	 *
	 * @param env The N-API environment.
	 * @param dbPath The database path.
	 * @param options Purge options (before, destroy, name).
	 * @returns A JavaScript array of removed file paths.
	 */
	static napi_value PurgeStores(napi_env env, const std::string& dbPath, napi_value options);

	/**
	 * Gets all stores for the given database path. Used by flush event
	 * listeners to notify stores of flush events.
	 *
	 * @param dbPath The database path.
	 * @returns A vector of transaction log stores (may be empty if path not found).
	 */
	static std::vector<std::shared_ptr<TransactionLogStore>> GetStores(const std::string& dbPath);

	/**
	 * Gets the number of entries in the registry (for debugging/testing).
	 */
	static size_t Size();
};

} // namespace rocksdb_js

#endif
