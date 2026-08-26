#include "transaction_log_store_registry.h"
#include "transaction_log_file.h"
#include "napi/macros.h"
#include "core/platform.h"
#include "napi/helpers.h"
#include "napi/async.h"
#include <atomic>
#include <filesystem>
#include <vector>

namespace rocksdb_js {

// Initialize the static instance
std::unique_ptr<TransactionLogStoreRegistry> TransactionLogStoreRegistry::instance;

namespace {
std::atomic<uint64_t> nextDeletionId { 0 };
}

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
		auto entry = std::make_shared<TransactionLogStoreRegistryEntry>(config);
		DEBUG_LOG("%p TransactionLogStoreRegistry::Register Created entry for \"%s\" (refCount=1)\n",
			instance.get(), dbPath.c_str());
		instance->entries.emplace(dbPath, entry);
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

	std::shared_ptr<TransactionLogStoreRegistryEntry> entry;
	std::string transactionLogsPath;
	const TransactionLogStoreConfig* config = nullptr;

	{
		std::lock_guard<std::mutex> lock(instance->entriesMutex);

		auto it = instance->entries.find(dbPath);
		if (it == instance->entries.end()) {
			DEBUG_LOG("%p TransactionLogStoreRegistry::DiscoverStores Entry not found for \"%s\"\n",
				instance.get(), dbPath.c_str());
			return;
		}

		// Take a shared_ptr copy to keep the entry alive after releasing the lock
		entry = it->second;
		transactionLogsPath = entry->config.transactionLogsPath;
		config = &entry->config;
	}

	if (transactionLogsPath.empty()) {
		return;
	}

	if (!std::filesystem::exists(transactionLogsPath)) {
		DEBUG_LOG("%p TransactionLogStoreRegistry::DiscoverStores No transaction logs path or directory does not exist for \"%s\"\n",
			instance.get(), dbPath.c_str());
		return;
	}

	// A process can exit after atomically detaching a destroyed store but before
	// its files are removed. Keep the root itself so concurrent detaches remain valid.
	auto deletionRoot = std::filesystem::path(transactionLogsPath);
	deletionRoot += ".deleting";
	rocksdb_js::tryCreateDirectory(deletionRoot);
	std::error_code cleanupError;
	for (std::filesystem::directory_iterator it(deletionRoot, cleanupError), end; !cleanupError && it != end; it.increment(cleanupError)) {
		std::error_code removeError;
		std::filesystem::remove_all(it->path(), removeError);
	}

	std::lock_guard<std::mutex> storeLock(entry->storesMutex);

	for (const auto& dirEntry : std::filesystem::directory_iterator(transactionLogsPath)) {
		if (dirEntry.is_directory()) {
			auto store = TransactionLogStore::load(
				dirEntry.path(),
				config->transactionLogMaxSize,
				config->transactionLogRetentionMs,
				config->transactionLogMaxAgeThreshold
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

	std::shared_ptr<TransactionLogStoreRegistryEntry> entry;
	const TransactionLogStoreConfig* config = nullptr;

	{
		std::lock_guard<std::mutex> lock(instance->entriesMutex);

		auto it = instance->entries.find(dbPath);
		if (it == instance->entries.end()) {
			DEBUG_LOG("%p TransactionLogStoreRegistry::ResolveStore Entry not found for \"%s\"\n",
				instance.get(), dbPath.c_str());
			return nullptr;
		}

		// Take a shared_ptr copy to keep the entry alive after releasing the lock
		entry = it->second;
		config = &entry->config;
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
	auto logDirectory = std::filesystem::path(config->transactionLogsPath) / name;
	DEBUG_LOG("%p TransactionLogStoreRegistry::ResolveStore Creating new store \"%s\" for \"%s\"\n",
		instance.get(), name.c_str(), dbPath.c_str());

	// Ensure the directory exists
	rocksdb_js::tryCreateDirectory(logDirectory);

	auto txnLogStore = std::make_shared<TransactionLogStore>(
		name,
		logDirectory,
		config->transactionLogMaxSize,
		config->transactionLogRetentionMs,
		config->transactionLogMaxAgeThreshold
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

	std::shared_ptr<TransactionLogStoreRegistryEntry> entry;

	{
		std::lock_guard<std::mutex> lock(instance->entriesMutex);

		auto it = instance->entries.find(dbPath);
		if (it == instance->entries.end()) {
			DEBUG_LOG("%p TransactionLogStoreRegistry::ListStores Entry not found for \"%s\"\n",
				instance.get(), dbPath.c_str());
			return result;
		}

		// Take a shared_ptr copy to keep the entry alive after releasing the lock
		entry = it->second;
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

	bool includeEntryCounts = false;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "includeEntryCounts", includeEntryCounts));

	std::string name;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "name", name));

	std::shared_ptr<TransactionLogStoreRegistryEntry> entry;

	{
		std::lock_guard<std::mutex> lock(instance->entriesMutex);

		auto it = instance->entries.find(dbPath);
		if (it == instance->entries.end()) {
			DEBUG_LOG("%p TransactionLogStoreRegistry::PurgeStores Entry not found for \"%s\"\n",
				instance.get(), dbPath.c_str());
			return removed;
		}

		// Take a shared_ptr copy to keep the entry alive after releasing the lock
		entry = it->second;
	}

	std::filesystem::path deletionRoot;
	if (destroy) {
		deletionRoot = std::filesystem::path(entry->config.transactionLogsPath);
		deletionRoot += ".deleting";
		rocksdb_js::tryCreateDirectory(deletionRoot);
		std::error_code rootError;
		if (!std::filesystem::is_directory(deletionRoot, rootError)) {
			DEBUG_LOG("%p TransactionLogStoreRegistry::PurgeStores Cannot create deletion directory %s: %s\n",
				instance.get(), deletionRoot.string().c_str(), rootError.message().c_str());
			return removed;
		}
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
		store->purge([&](const std::filesystem::path& filePath, uint32_t entryCount) -> void {
			auto path = filePath.string();
			napi_value pathValue;
			NAPI_STATUS_THROWS_VOID(::napi_create_string_utf8(env, path.c_str(), path.length(), &pathValue));

			napi_value element = pathValue;
			if (includeEntryCounts) {
				// opt-in shape: { path: string, entries: number }
				NAPI_STATUS_THROWS_VOID(::napi_create_object(env, &element));
				NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, element, "path", pathValue));
				napi_value entriesValue;
				NAPI_STATUS_THROWS_VOID(::napi_create_uint32(env, entryCount, &entriesValue));
				NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, element, "entries", entriesValue));
			}

			NAPI_STATUS_THROWS_VOID(::napi_set_element(env, removed, i++, element));
		}, destroy, before, includeEntryCounts);

		if (destroy) {
			store->tryClose();
		}
	}

	// Phase 3: Atomically detach closed store directories while holding the registry
	// lock, then remove them without blocking flush callbacks or store resolution.
	if (destroy) {
		std::vector<std::filesystem::path> pathsToRemove;
		{
			std::lock_guard<std::mutex> storeLock(entry->storesMutex);
			for (auto& store : storesToPurge) {
				if (!store->isClosing.load(std::memory_order_relaxed)) {
					continue;
				}
				auto storeIt = entry->stores.find(store->name);
				if (storeIt == entry->stores.end() || storeIt->second.get() != store.get()) {
					continue;
				}

				auto deletionPath = deletionRoot /
					(store->name + "-" + std::to_string(nextDeletionId.fetch_add(1, std::memory_order_relaxed)));
				std::error_code renameError;
				std::filesystem::rename(store->path, deletionPath, renameError);
				if (renameError == std::errc::no_such_file_or_directory) {
					std::error_code sourceError;
					if (std::filesystem::exists(store->path, sourceError) || sourceError) {
						DEBUG_LOG("%p TransactionLogStoreRegistry::PurgeStores Failed to detach log directory %s: %s\n",
							instance.get(), store->path.string().c_str(), renameError.message().c_str());
						continue;
					}
				} else if (renameError) {
					DEBUG_LOG("%p TransactionLogStoreRegistry::PurgeStores Failed to detach log directory %s: %s\n",
						instance.get(), store->path.string().c_str(), renameError.message().c_str());
					continue;
				}
				entry->stores.erase(storeIt);
				if (!renameError) {
					pathsToRemove.push_back(std::move(deletionPath));
				}
			}
		}
		for (const auto& deletionPath : pathsToRemove) {
			std::error_code removeError;
			std::filesystem::remove_all(deletionPath, removeError);
			if (removeError) {
				DEBUG_LOG("%p TransactionLogStoreRegistry::PurgeStores Failed to remove detached log directory %s: %s\n",
					instance.get(), deletionPath.string().c_str(), removeError.message().c_str());
			}
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

	std::shared_ptr<TransactionLogStoreRegistryEntry> entry;

	{
		std::lock_guard<std::mutex> lock(instance->entriesMutex);

		auto it = instance->entries.find(dbPath);
		if (it == instance->entries.end()) {
			return result;
		}

		entry = it->second;
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

TransactionLogCoolResult TransactionLogStoreRegistry::CoolTransactionLogs() {
	TransactionLogCoolResult result;
	if (!instance) {
		return result;
	}

	// Snapshot the structure level-by-level, releasing each mutex before
	// descending, so we never hold a registry/store lock across the madvise()
	// syscall in adviseCold(). The shared_ptr copies keep the entries/stores/
	// files alive for the duration of the pass.
	std::vector<std::shared_ptr<TransactionLogStoreRegistryEntry>> entriesCopy;
	{
		std::lock_guard<std::mutex> lock(instance->entriesMutex);
		entriesCopy.reserve(instance->entries.size());
		for (auto& [path, entry] : instance->entries) {
			entriesCopy.push_back(entry);
		}
	}

	for (auto& entry : entriesCopy) {
		std::vector<std::shared_ptr<TransactionLogStore>> storesCopy;
		{
			std::lock_guard<std::mutex> lock(entry->storesMutex);
			storesCopy.reserve(entry->stores.size());
			for (auto& [name, store] : entry->stores) {
				storesCopy.push_back(store);
			}
		}

		for (auto& store : storesCopy) {
			std::vector<std::shared_ptr<TransactionLogFile>> filesCopy;
			{
				std::lock_guard<std::mutex> lock(store->dataSetsMutex);
				filesCopy.reserve(store->sequenceFiles.size());
				for (auto& [seq, file] : store->sequenceFiles) {
					filesCopy.push_back(file);
				}
			}

			for (auto& file : filesCopy) {
				size_t bytes = file->adviseCold();
				if (bytes > 0) {
					result.maps++;
					result.bytes += bytes;
				}
			}
		}
	}

	DEBUG_LOG("TransactionLogStoreRegistry::CoolTransactionLogs cooled %u maps (%llu bytes)\n",
		result.maps, static_cast<unsigned long long>(result.bytes));
	return result;
}

} // namespace rocksdb_js
