#include "transaction_log_store_registry.h"
#include "macros.h"
#include "util.h"
#include <filesystem>

namespace rocksdb_js {

// Initialize the static instance
std::unique_ptr<TransactionLogStoreRegistry> TransactionLogStoreRegistry::instance;

/**
 * Initializes the singleton instance.
 */
void TransactionLogStoreRegistry::Init() {
	if (!instance) {
		instance = std::unique_ptr<TransactionLogStoreRegistry>(new TransactionLogStoreRegistry());
		DEBUG_LOG("%p TransactionLogStoreRegistry::Init Initialized\n", instance.get());
	}
}

/**
 * Shuts down the registry and closes all stores.
 */
void TransactionLogStoreRegistry::Shutdown() {
	if (instance) {
		DEBUG_LOG("%p TransactionLogStoreRegistry::Shutdown Shutting down\n", instance.get());

		std::lock_guard<std::mutex> lock(instance->entriesMutex);
		for (auto& [path, entry] : instance->entries) {
			std::lock_guard<std::mutex> storeLock(entry->storesMutex);
			for (auto& [name, store] : entry->stores) {
				store->close();
			}
			entry->stores.clear();
		}
		instance->entries.clear();

		DEBUG_LOG("%p TransactionLogStoreRegistry::Shutdown Complete\n", instance.get());
	}
}

/**
 * Registers a DBDescriptor for the given database path.
 */
void TransactionLogStoreRegistry::Register(const std::string& dbPath, const TransactionLogStoreConfig& config) {
	if (!instance) {
		DEBUG_LOG("TransactionLogStoreRegistry::Register Registry not initialized\n");
		return;
	}

	std::lock_guard<std::mutex> lock(instance->entriesMutex);

	auto it = instance->entries.find(dbPath);
	if (it == instance->entries.end()) {
		// Create new entry
		auto entry = std::make_unique<TransactionLogStoreRegistryEntry>(config);
		DEBUG_LOG("%p TransactionLogStoreRegistry::Register Created entry for \"%s\" (refCount=1)\n",
			instance.get(), dbPath.c_str());
		instance->entries.emplace(dbPath, std::move(entry));
	} else {
		// Increment reference count
		it->second->refCount++;
		DEBUG_LOG("%p TransactionLogStoreRegistry::Register Incremented refCount for \"%s\" (refCount=%zu)\n",
			instance.get(), dbPath.c_str(), it->second->refCount);
	}
}

/**
 * Unregisters a DBDescriptor for the given database path.
 */
void TransactionLogStoreRegistry::Unregister(const std::string& dbPath) {
	if (!instance) {
		DEBUG_LOG("TransactionLogStoreRegistry::Unregister Registry not initialized\n");
		return;
	}

	std::vector<std::shared_ptr<TransactionLogStore>> storesToClose;

	{
		std::lock_guard<std::mutex> lock(instance->entriesMutex);

		auto it = instance->entries.find(dbPath);
		if (it == instance->entries.end()) {
			DEBUG_LOG("%p TransactionLogStoreRegistry::Unregister Entry not found for \"%s\"\n",
				instance.get(), dbPath.c_str());
			return;
		}

		auto& entry = it->second;
		entry->refCount--;
		DEBUG_LOG("%p TransactionLogStoreRegistry::Unregister Decremented refCount for \"%s\" (refCount=%zu)\n",
			instance.get(), dbPath.c_str(), entry->refCount);

		if (entry->refCount == 0) {
			DEBUG_LOG("%p TransactionLogStoreRegistry::Unregister Removing entry for \"%s\"\n",
				instance.get(), dbPath.c_str());

			// Collect stores to close outside the lock
			{
				std::lock_guard<std::mutex> storeLock(entry->storesMutex);
				for (auto& [name, store] : entry->stores) {
					storesToClose.push_back(store);
				}
				entry->stores.clear();
			}

			instance->entries.erase(it);
		}
	}

	// Close stores outside the entriesMutex lock to avoid deadlocks
	for (auto& store : storesToClose) {
		store->close();
	}
}

/**
 * Discovers existing transaction log stores in the transaction logs directory.
 */
void TransactionLogStoreRegistry::DiscoverStores(const std::string& dbPath) {
	if (!instance) {
		DEBUG_LOG("TransactionLogStoreRegistry::DiscoverStores Registry not initialized\n");
		return;
	}

	TransactionLogStoreRegistryEntry* entry = nullptr;
	std::string transactionLogsPath;
	TransactionLogStoreConfig config;

	{
		std::lock_guard<std::mutex> lock(instance->entriesMutex);

		auto it = instance->entries.find(dbPath);
		if (it == instance->entries.end()) {
			DEBUG_LOG("%p TransactionLogStoreRegistry::DiscoverStores Entry not found for \"%s\"\n",
				instance.get(), dbPath.c_str());
			return;
		}

		entry = it->second.get();
		transactionLogsPath = entry->config.transactionLogsPath;
		config = entry->config;
	}

	if (transactionLogsPath.empty() || !std::filesystem::exists(transactionLogsPath)) {
		DEBUG_LOG("%p TransactionLogStoreRegistry::DiscoverStores No transaction logs path or directory does not exist for \"%s\"\n",
			instance.get(), dbPath.c_str());
		return;
	}

	std::lock_guard<std::mutex> storeLock(entry->storesMutex);

	for (const auto& dirEntry : std::filesystem::directory_iterator(transactionLogsPath)) {
		if (dirEntry.is_directory()) {
			auto store = TransactionLogStore::load(
				dirEntry.path(),
				config.transactionLogMaxSize,
				config.transactionLogRetentionMs,
				config.transactionLogMaxAgeThreshold
			);
			if (store) {
				DEBUG_LOG("%p TransactionLogStoreRegistry::DiscoverStores Found store \"%s\" for \"%s\"\n",
					instance.get(), store->name.c_str(), dbPath.c_str());
				entry->stores.emplace(store->name, store);
			}
		}
	}
}

/**
 * Resolves (finds or creates) a transaction log store by name.
 */
std::shared_ptr<TransactionLogStore> TransactionLogStoreRegistry::ResolveStore(
	const std::string& dbPath,
	const std::string& name
) {
	if (!instance) {
		DEBUG_LOG("TransactionLogStoreRegistry::ResolveStore Registry not initialized\n");
		return nullptr;
	}

	TransactionLogStoreRegistryEntry* entry = nullptr;
	TransactionLogStoreConfig config;

	{
		std::lock_guard<std::mutex> lock(instance->entriesMutex);

		auto it = instance->entries.find(dbPath);
		if (it == instance->entries.end()) {
			DEBUG_LOG("%p TransactionLogStoreRegistry::ResolveStore Entry not found for \"%s\"\n",
				instance.get(), dbPath.c_str());
			return nullptr;
		}

		entry = it->second.get();
		config = entry->config;
	}

	std::lock_guard<std::mutex> storeLock(entry->storesMutex);

	auto storeIt = entry->stores.find(name);
	if (storeIt != entry->stores.end()) {
		// Check if the store is closing - if so, we need to create a new one
		if (!storeIt->second->isClosing.load(std::memory_order_relaxed)) {
			DEBUG_LOG("%p TransactionLogStoreRegistry::ResolveStore Found store \"%s\" for \"%s\"\n",
				instance.get(), name.c_str(), dbPath.c_str());
			return storeIt->second;
		}
		DEBUG_LOG("%p TransactionLogStoreRegistry::ResolveStore Found closing store \"%s\" for \"%s\", creating new one\n",
			instance.get(), name.c_str(), dbPath.c_str());
	}

	// Create new store
	auto logDirectory = std::filesystem::path(config.transactionLogsPath) / name;
	DEBUG_LOG("%p TransactionLogStoreRegistry::ResolveStore Creating new store \"%s\" for \"%s\"\n",
		instance.get(), name.c_str(), dbPath.c_str());

	// Ensure the directory exists
	rocksdb_js::tryCreateDirectory(logDirectory);

	auto txnLogStore = std::make_shared<TransactionLogStore>(
		name,
		logDirectory,
		config.transactionLogMaxSize,
		config.transactionLogRetentionMs,
		config.transactionLogMaxAgeThreshold
	);

	// Use insert_or_assign to replace any closing store with the same name
	entry->stores.insert_or_assign(txnLogStore->name, txnLogStore);
	return txnLogStore;
}

/**
 * Lists all transaction log store names for the given database path.
 */
napi_value TransactionLogStoreRegistry::ListStores(napi_env env, const std::string& dbPath) {
	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_array(env, &result));

	if (!instance) {
		DEBUG_LOG("TransactionLogStoreRegistry::ListStores Registry not initialized\n");
		return result;
	}

	TransactionLogStoreRegistryEntry* entry = nullptr;

	{
		std::lock_guard<std::mutex> lock(instance->entriesMutex);

		auto it = instance->entries.find(dbPath);
		if (it == instance->entries.end()) {
			DEBUG_LOG("%p TransactionLogStoreRegistry::ListStores Entry not found for \"%s\"\n",
				instance.get(), dbPath.c_str());
			return result;
		}

		entry = it->second.get();
	}

	std::lock_guard<std::mutex> storeLock(entry->storesMutex);

	size_t i = 0;
	NAPI_STATUS_THROWS(::napi_create_array_with_length(env, entry->stores.size(), &result));

	DEBUG_LOG("%p TransactionLogStoreRegistry::ListStores Returning %zu stores for \"%s\"\n",
		instance.get(), entry->stores.size(), dbPath.c_str());

	for (auto& [name, store] : entry->stores) {
		napi_value nameValue;
		NAPI_STATUS_THROWS(::napi_create_string_utf8(env, store->name.c_str(), store->name.length(), &nameValue));
		NAPI_STATUS_THROWS(::napi_set_element(env, result, i++, nameValue));
	}

	return result;
}

/**
 * Purges transaction logs for the given database path.
 */
napi_value TransactionLogStoreRegistry::PurgeStores(napi_env env, const std::string& dbPath, napi_value options) {
	napi_value removed;
	NAPI_STATUS_THROWS(::napi_create_array(env, &removed));

	if (!instance) {
		DEBUG_LOG("TransactionLogStoreRegistry::PurgeStores Registry not initialized\n");
		return removed;
	}

	uint64_t before = 0;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "before", before));

	bool destroy = false;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "destroy", destroy));

	std::string name;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "name", name));

	TransactionLogStoreRegistryEntry* entry = nullptr;

	{
		std::lock_guard<std::mutex> lock(instance->entriesMutex);

		auto it = instance->entries.find(dbPath);
		if (it == instance->entries.end()) {
			DEBUG_LOG("%p TransactionLogStoreRegistry::PurgeStores Entry not found for \"%s\"\n",
				instance.get(), dbPath.c_str());
			return removed;
		}

		entry = it->second.get();
	}

	size_t i = 0;
	std::vector<std::shared_ptr<TransactionLogStore>> storesToPurge;

	// Phase 1: Collect stores to process while holding the lock
	{
		std::lock_guard<std::mutex> storeLock(entry->storesMutex);
		for (auto& [storeName, store] : entry->stores) {
			if (name.empty() || store->name == name) {
				storesToPurge.push_back(store);
			}
		}
	}

	// Phase 2: Process stores WITHOUT holding storesMutex
	for (auto& store : storesToPurge) {
		store->purge([&](const std::filesystem::path& filePath) -> void {
			napi_value logFileValue;
			auto path = filePath.string();
			NAPI_STATUS_THROWS_VOID(::napi_create_string_utf8(env, path.c_str(), path.length(), &logFileValue));
			NAPI_STATUS_THROWS_VOID(::napi_set_element(env, removed, i++, logFileValue));
		}, destroy, before);

		if (destroy) {
			store->tryClose();
		}
	}

	// Phase 3: Remove closed stores from the registry while holding the lock
	std::vector<std::shared_ptr<TransactionLogStore>> storesActuallyRemoved;
	if (destroy) {
		std::lock_guard<std::mutex> storeLock(entry->storesMutex);
		for (auto& store : storesToPurge) {
			if (!store->isClosing.load(std::memory_order_relaxed)) {
				continue;
			}
			auto storeIt = entry->stores.find(store->name);
			if (storeIt != entry->stores.end() && storeIt->second.get() == store.get()) {
				entry->stores.erase(storeIt);
				storesActuallyRemoved.push_back(store);
			}
		}
	}

	// Phase 4: Delete directories outside the lock
	for (auto& store : storesActuallyRemoved) {
		try {
			std::filesystem::remove_all(store->path);
		} catch (const std::filesystem::filesystem_error& e) {
			DEBUG_LOG("%p TransactionLogStoreRegistry::PurgeStores Failed to remove log directory %s: %s\n",
				instance.get(), store->path.string().c_str(), e.what());
		} catch (...) {
			DEBUG_LOG("%p TransactionLogStoreRegistry::PurgeStores Unknown error removing log directory %s\n",
				instance.get(), store->path.string().c_str());
		}
	}

	return removed;
}

/**
 * Gets all stores for the given database path.
 */
std::vector<std::shared_ptr<TransactionLogStore>> TransactionLogStoreRegistry::GetStores(const std::string& dbPath) {
	std::vector<std::shared_ptr<TransactionLogStore>> result;

	if (!instance) {
		DEBUG_LOG("TransactionLogStoreRegistry::GetStores Registry not initialized\n");
		return result;
	}

	TransactionLogStoreRegistryEntry* entry = nullptr;

	{
		std::lock_guard<std::mutex> lock(instance->entriesMutex);

		auto it = instance->entries.find(dbPath);
		if (it == instance->entries.end()) {
			return result;
		}

		entry = it->second.get();
	}

	std::lock_guard<std::mutex> storeLock(entry->storesMutex);
	result.reserve(entry->stores.size());
	for (auto& [name, store] : entry->stores) {
		result.push_back(store);
	}

	return result;
}

/**
 * Gets the number of entries in the registry.
 */
size_t TransactionLogStoreRegistry::Size() {
	if (instance) {
		std::lock_guard<std::mutex> lock(instance->entriesMutex);
		return instance->entries.size();
	}
	return 0;
}

} // namespace rocksdb_js
