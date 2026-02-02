#include "db_registry.h"
#include "macros.h"
#include "util.h"
#include "rocksdb/table.h"

namespace rocksdb_js {

// Initialize the static instance
std::unique_ptr<DBRegistry> DBRegistry::instance;

/**
 * Close a RocksDB database handle.
 */
void DBRegistry::CloseDB(const std::shared_ptr<DBHandle> handle) {
	if (!instance) {
		DEBUG_LOG("%p DBRegistry::CloseDB Registry not initialized\n", instance.get());
		return;
	}

	if (!handle) {
		DEBUG_LOG("%p DBRegistry::CloseDB Invalid handle\n", instance.get());
		return;
	}

	DBRegistry::DebugLogDescriptorRefs();

	if (!handle->descriptor) {
		DEBUG_LOG("%p DBRegistry::CloseDB Database not opened\n", instance.get());
		return;
	}

	std::string path = handle->descriptor->path;
	DBRegistryEntry* entry = nullptr;

	{
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		auto entryIterator = instance->databases.find(path);
		if (entryIterator != instance->databases.end()) {
			entry = &entryIterator->second;
			DEBUG_LOG("%p DBRegistry::CloseDB Found DBDescriptor for \"%s\" (ref count = %ld)\n", instance.get(), path.c_str(), entry->descriptor.use_count());
		} else {
			DEBUG_LOG("%p DBRegistry::CloseDB DBDescriptor not found! \"%s\"\n", instance.get(), path.c_str());
		}
	}

	// close the handle, decrements the descriptor ref count
	handle->close();

	DEBUG_LOG("%p DBRegistry::CloseDB Closed DBHandle %p for \"%s\" (ref count = %ld)\n", instance.get(), handle.get(), path.c_str(), entry->descriptor.use_count());

	// re-acquire the mutex to check and potentially remove the descriptor
	{
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		// since the registry itself always has a ref, we need to check for ref count 1
		if (entry->descriptor.use_count() <= 1) {
			DEBUG_LOG("%p DBRegistry::CloseDB Purging descriptor for \"%s\"\n", instance.get(), path.c_str());
			entry->descriptor->close();
			instance->databases.erase(path);

			// notify only waiters for this specific path
			if (entry->condition) {
				entry->condition->notify_all();
			}
		} else {
			DEBUG_LOG("%p DBRegistry::CloseDB DBDescriptor is still active (ref count = %ld)\n", instance.get(), entry->descriptor.use_count());
		}
	}
}

/**
 * Debug log the reference count of all descriptors in the registry.
 */
void DBRegistry::DebugLogDescriptorRefs() {
#ifdef DEBUG
	std::lock_guard<std::mutex> lock(instance->databasesMutex);
	DEBUG_LOG("DBRegistry::DebugLogDescriptorRefs %d descriptor%s in registry:\n", instance->databases.size(), instance->databases.size() == 1 ? "" : "s");
	for (auto& [path, entry] : instance->databases) {
		DEBUG_LOG("  %p for \"%s\" (ref count = %ld)\n", entry.descriptor.get(), path.c_str(), entry.descriptor.use_count());
	}
#endif
}

/**
 * Destroy a RocksDB database.
 *
 * @param path - The path to the database to destroy.
 */
void DBRegistry::DestroyDB(const std::string& path) {
	if (!instance) {
		DEBUG_LOG("%p DBRegistry::DestroyDB Registry not initialized\n", instance.get());
		return;
	}

	DEBUG_LOG("%p DBRegistry::DestroyDB Destroying \"%s\"\n", instance.get(), path.c_str());

	std::shared_ptr<DBDescriptor> descriptor;

	// Find and remove the descriptor from the registry
	{
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		auto it = instance->databases.find(path);
		if (it != instance->databases.end()) {
			descriptor = it->second.descriptor;
			instance->databases.erase(it);
			DEBUG_LOG("%p DBRegistry::DestroyDB Found and removed descriptor from registry (ref count = %ld)\n",
				instance.get(), descriptor ? descriptor.use_count() : 0);
		}
	}

	if (descriptor) {
		// Close all closables (iterators, transactions, handles) attached to this descriptor
		// This should release all DBHandle references
		DEBUG_LOG("%p DBRegistry::DestroyDB Closing descriptor and all attached resources (ref count = %zu)\n",
			instance.get(), descriptor.use_count());
		descriptor->close();

		// After closing, check if there are still lingering references
		// Should only be our local reference (= 1) at this point
		size_t refCountAfterClose = descriptor.use_count();
		if (refCountAfterClose > 1) {
			std::string errorMsg = "Cannot destroy database: " + std::to_string(refCountAfterClose - 1) +
				" reference(s) still held after closing all handles. This may indicate handles not properly closed or JavaScript objects not yet garbage collected.";
			DEBUG_LOG("%p DBRegistry::DestroyDB Error: %s\n", instance.get(), errorMsg.c_str());
			throw std::runtime_error(errorMsg);
		}

		// Release our reference to the descriptor
		// This will trigger the destructor which properly closes the DB
		DEBUG_LOG("%p DBRegistry::DestroyDB Releasing descriptor reference\n", instance.get());
		descriptor.reset();
	}

	// Now the database lock should be released, safe to destroy
	DEBUG_LOG("%p DBRegistry::DestroyDB Calling rocksdb::DestroyDB for \"%s\"\n", instance.get(), path.c_str());
	rocksdb::Status status = rocksdb::DestroyDB(path, rocksdb::Options());
	if (!status.ok()) {
		throw std::runtime_error(status.ToString().c_str());
	}

	DEBUG_LOG("%p DBRegistry::DestroyDB Successfully destroyed database at \"%s\"\n", instance.get(), path.c_str());
}

/**
 * Initialize the singleton instance of the registry.
 */
 void DBRegistry::Init() {
	if (!instance) {
		instance = std::unique_ptr<DBRegistry>(new DBRegistry());
		DEBUG_LOG("%p DBRegistry::Initialize Initialized DBRegistry\n", instance.get());
	}
}

/**
 * Open a RocksDB database with column family, caches it in the registry, and
 * return a handle to it.
 *
 * @param path - The filesystem path to the database.
 * @param options - The options for the database.
 * @return A handle to the RocksDB database including the transaction db and
 * column family handle.
 */
std::unique_ptr<DBHandleParams> DBRegistry::OpenDB(const std::string& path, const DBOptions& options) {
	// ensure the registry has already been initialized
	if (!instance) {
		DEBUG_LOG("DBRegistry::OpenDB Registry not initialized!\n");
		throw std::runtime_error("DBRegistry not initialized!");
	}

	DEBUG_LOG("%p DBRegistry::OpenDB Using registry\n", instance.get());

	std::unordered_map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> columns;
	std::string name = options.name.empty() ? "default" : options.name;
	std::shared_ptr<DBDescriptor> descriptor;
	std::unique_lock<std::mutex> lock(instance->databasesMutex);

	// get or create entry for this path
	auto entryIterator = instance->databases.find(path);
	if (entryIterator == instance->databases.end()) {
		// create entry with empty descriptor and new condition variable
		auto [it, inserted] = instance->databases.emplace(path, DBRegistryEntry());
		entryIterator = it;
	}

	auto& entry = entryIterator->second;

	// wait for any closing database on this specific path to be fully removed before proceeding
	entry.condition->wait(lock, [&]() {
		if (entry.descriptor) {
			if (entry.descriptor->isClosing()) {
				DEBUG_LOG("%p DBRegistry::OpenDB Database \"%s\" is closing, waiting for removal\n", instance.get(), path.c_str());
				entry.descriptor.reset();
				return false; // keep waiting
			}
			return true; // database exists and is not closing
		}
		return true; // database doesn't exist, can proceed
	});

	// at this point, either:
	// 1. descriptor is set to a valid, non-closing database, or
	// 2. descriptor is nullptr (database doesn't exist)

	if (entry.descriptor) {
		// database exists and is not closing, proceed with existing logic
		// check if the database is already open with a different mode
		if (options.mode != entry.descriptor->mode) {
			throw std::runtime_error(
				"Database already open in '" +
				(entry.descriptor->mode == DBMode::Optimistic ? std::string("optimistic") : std::string("pessimistic")) +
				"' mode"
			);
		}

		DEBUG_LOG("%p DBRegistry::OpenDB Database \"%s\" already open\n", instance.get(), path.c_str());
		DEBUG_LOG("%p DBRegistry::OpenDB Checking for column family \"%s\"\n", instance.get(), name.c_str());

		// manually copy the columns because we don't know which ones are valid
		bool columnExists = false;
		for (auto& column : entry.descriptor->columns) {
			columns[column.first] = column.second;
			if (column.first == name) {
				DEBUG_LOG("%p DBRegistry::OpenDB Column family \"%s\" already exists\n", instance.get(), name.c_str());
				columnExists = true;
			}
		}
		if (!columnExists) {
			DEBUG_LOG("%p DBRegistry::OpenDB Creating column family \"%s\"\n", instance.get(), name.c_str());
			columns[name] = rocksdb_js::createRocksDBColumnFamily(entry.descriptor->db, name);
			entry.descriptor->columns[name] = columns[name];
		}
	} else {
		entry.descriptor = DBDescriptor::open(path, options);	// store the descriptor in the existing entry
		DEBUG_LOG("%p DBRegistry::OpenDB Stored DBDescriptor %p for \"%s\" (ref count = %ld)\n", instance.get(), entry.descriptor.get(), path.c_str(), entry.descriptor.use_count());
		columns = entry.descriptor->columns;
	}

	// handle the column family
	std::shared_ptr<rocksdb::ColumnFamilyHandle> column;
	auto colIterator = columns.find(name);
	if (colIterator != columns.end()) {
		// column family already exists
		DEBUG_LOG("%p DBRegistry::OpenDB Column family \"%s\" found\n", instance.get(), name.c_str());
		column = colIterator->second;
	} else {
		// use the default column family
		DEBUG_LOG("%p DBRegistry::OpenDB Column family \"%s\" not found, using \"default\"\n", instance.get(), name.c_str());
		column = columns[rocksdb::kDefaultColumnFamilyName];
	}

	std::unique_ptr<DBHandleParams> handle = std::make_unique<DBHandleParams>(entry.descriptor, column);
	DEBUG_LOG("%p DBRegistry::OpenDB Created DBHandleParams %p for \"%s\" (ref count = %ld)\n", instance.get(), handle.get(), path.c_str(), entry.descriptor.use_count());
	return handle;
}

/**
 * Purge expired database descriptors from the registry.
 */
void DBRegistry::PurgeAll() {
	if (instance) {
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
#ifdef DEBUG
		size_t initialSize = instance->databases.size();
#endif
		for (auto it = instance->databases.begin(); it != instance->databases.end();) {
			DEBUG_LOG("%p DBRegistry::PurgeAll Purging \"%s\"\n", instance.get(), it->first.c_str());
			it = instance->databases.erase(it);
		}
#ifdef DEBUG
		size_t currentSize = instance->databases.size();
		DEBUG_LOG(
			"%p DBRegistry::PurgeAll Purged %zu unused descriptors (size=%zu)\n",
			instance.get(),
			initialSize - currentSize,
			currentSize
		);
#endif
	}
}

/**
 * Shutdown will force all databases to flush in-memory data to disk and purge the registry.
 */
void DBRegistry::Shutdown() {
	if (instance) {
		std::vector<std::shared_ptr<DBDescriptor>> descriptorsToClose;

		{
			std::lock_guard<std::mutex> lock(instance->databasesMutex);
			DEBUG_LOG("%p DBRegistry::Shutdown Shutting down %zu databases\n", instance.get(), instance->databases.size());

			// Collect all descriptors to close
			for (auto& [path, entry] : instance->databases) {
				if (entry.descriptor) {
					descriptorsToClose.push_back(entry.descriptor);
				}
			}
		}

		// Close all descriptors without holding the lock
		for (auto& descriptor : descriptorsToClose) {
			DEBUG_LOG("%p DBRegistry::Shutdown Closing database: %s\n", instance.get(), descriptor->path.c_str());
			descriptor->close();
		}

		// Purge the registry
		PurgeAll();

		DEBUG_LOG("%p DBRegistry::Shutdown Shutdown complete\n", instance.get());
	}
}

/**
 * Get the number of databases in the registry.
 */
size_t DBRegistry::Size() {
	if (instance) {
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		return instance->databases.size();
	}
	return 0;
}

} // namespace rocksdb_js
