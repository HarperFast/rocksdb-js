#include "database/db_registry.h"
#include "napi/macros.h"
#include "core/platform.h"
#include "napi/helpers.h"
#include "napi/async.h"
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

#ifdef DEBUG
	DBRegistry::DebugLogDescriptorRefs();
#endif

	if (!handle->descriptor) {
		DEBUG_LOG("%p DBRegistry::CloseDB Database not opened\n", instance.get());
		return;
	}

	DBKey key{handle->descriptor->path, handle->descriptor->readOnly};

	handle->descriptor->detach(handle);

	// close the handle, decrements the descriptor ref count
	handle->close();

	DBRegistry::PurgeIfUnreferenced(key.path, key.readOnly);
}

/**
 * Purges (closes and erases) the registry entry for `path` if no DBHandle
 * references its descriptor anymore; a no-op otherwise. This is the tail of
 * every close: CloseDB calls it after detaching the handle, and the async
 * operations that hold their own `shared_ptr<DBDescriptor>` for the duration
 * of a copy (backup, backup stream, checkpoint) call it when they release that
 * reference — a close that raced such an operation saw use_count() > 1 and
 * skipped the purge, so the releasing operation must retry it or the entry
 * (and the open RocksDB) would linger in the registry forever.
 *
 * The decision is made, and ownership of the descriptor taken, all under
 * databasesMutex. Multiple threads can race here for one path (worker envs
 * tearing down concurrently, or an async op's release racing a CloseDB), so
 * the decision MUST be atomic:
 *
 *   - We never hold a raw pointer into the map across the unlocked
 *     finishClose() below. An earlier implementation cached `&entry` under the
 *     lock and dereferenced it afterward; a concurrent purge that erased the
 *     map node freed that storage, so the survivor called close() on a freed
 *     DBDescriptor and locked its destroyed mutex (manifests on glibc as
 *     "malloc(): unaligned tcache chunk detected").
 *   - The registry always holds one ref, so use_count() <= 1 means no open
 *     DBHandles remain. OpenDB bumps use_count under this same lock, so the
 *     check serializes with it: if an open raced ahead it already pushed the
 *     count past 1 and we skip; if we win, beginClose() publishes the closing
 *     state while we still hold the lock, so a subsequent OpenDB observes
 *     isClosing() and waits instead of being handed a descriptor we then
 *     close out from under it. beginClose() also makes the claim single-shot.
 *   - The entry stays in the map (descriptor non-null and isClosing()) for
 *     the duration of finishClose(), so a concurrent OpenDB keeps waiting on
 *     the condition rather than re-opening the path mid-close.
 */
void DBRegistry::PurgeIfUnreferenced(const std::string& path, bool readOnly) {
	if (!instance) {
		return;
	}

	DBKey key{path, readOnly};
	std::shared_ptr<DBDescriptor> descriptor;
	std::shared_ptr<std::condition_variable> condition;
	{
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		auto entryIterator = instance->databases.find(key);
		if (entryIterator != instance->databases.end()) {
			DBRegistryEntry& entry = entryIterator->second;
			DEBUG_LOG("%p DBRegistry::PurgeIfUnreferenced Found DBDescriptor for \"%s\" (ref count = %ld)\n", instance.get(), key.path.c_str(), entry.descriptor.use_count());
			if (entry.descriptor && entry.descriptor.use_count() <= 1 && entry.descriptor->beginClose()) {
				DEBUG_LOG("%p DBRegistry::PurgeIfUnreferenced Claiming descriptor purge for \"%s\"\n", instance.get(), key.path.c_str());
				descriptor = entry.descriptor;
				condition = entry.condition;
			}
		} else {
			DEBUG_LOG("%p DBRegistry::PurgeIfUnreferenced DBDescriptor not found! \"%s\"\n", instance.get(), key.path.c_str());
		}
	}

	if (descriptor) {
		// We claimed the close under the lock via beginClose(); run the actual
		// teardown now. The local copy keeps the descriptor alive throughout.
		descriptor->finishClose();

		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		auto eraseIt = instance->databases.find(key);
		// Only erase the entry we claimed. OpenDB's wait predicate may have
		// reset the map's descriptor ref to null while we closed; a brand-new
		// descriptor cannot appear because OpenDB blocks until we notify below.
		if (eraseIt != instance->databases.end()
			&& (!eraseIt->second.descriptor || eraseIt->second.descriptor == descriptor)) {
			instance->databases.erase(eraseIt);
		}
	}

	// notify only waiters for this specific path
	if (condition) {
		condition->notify_all();
	}
}

/**
 * Debug log the reference count of all descriptors in the registry.
 */
#ifdef DEBUG
void DBRegistry::DebugLogDescriptorRefs() {
	std::lock_guard<std::mutex> lock(instance->databasesMutex);
	DEBUG_LOG("DBRegistry::DebugLogDescriptorRefs %zu descriptor%s in registry:\n", instance->databases.size(), instance->databases.size() == 1 ? "" : "s");
	for (auto& [key, entry] : instance->databases) {
		DEBUG_LOG("  %p for \"%s\" (ref count = %ld)\n", entry.descriptor.get(), key.path.c_str(), entry.descriptor.use_count());
	}
}
#endif

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
		for (auto it = instance->databases.begin(); it != instance->databases.end(); ) {
			if (it->first.path == path) {
				descriptor = it->second.descriptor;
				it = instance->databases.erase(it);
				DEBUG_LOG("%p DBRegistry::DestroyDB Found and removed descriptor from registry (ref count = %ld)\n",
					instance.get(), descriptor ? descriptor.use_count() : 0);
			} else {
				++it;
			}
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
			throw rocksdb_js::DBException(errorMsg);
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
		throw rocksdb_js::DBException(status.ToString());
	}

	// remove the database directory including transaction logs
	std::filesystem::remove_all(path);

	DEBUG_LOG("%p DBRegistry::DestroyDB Successfully destroyed database at \"%s\"\n", instance.get(), path.c_str());
}

/**
 * Initialize the singleton instance of the registry.
 */
void DBRegistry::Init(napi_env env, napi_value exports) {
	if (!instance) {
		instance = std::unique_ptr<DBRegistry>(new DBRegistry());
		DEBUG_LOG("%p DBRegistry::Initialize Initialized DBRegistry\n", instance.get());
	}

	napi_value registryStatusFn;
	NAPI_STATUS_THROWS_VOID(::napi_create_function(env, "registryStatus", NAPI_AUTO_LENGTH, DBRegistry::RegistryStatus, nullptr, &registryStatusFn));
	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, "registryStatus", registryStatusFn));
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
		throw rocksdb_js::DBException("DBRegistry not initialized!");
	}

	DEBUG_LOG("%p DBRegistry::OpenDB Opening database \"%s\" (mode=%s read-only=%s column family=\"%s\")\n", instance.get(), path.c_str(), options.mode == DBMode::Optimistic ? "optimistic" : "pessimistic", options.readOnly ? "true" : "false", options.name.empty() ? "default" : options.name.c_str());

	std::unordered_map<std::string, std::shared_ptr<ColumnFamilyDescriptor>> columns;
	std::string name = options.name.empty() ? "default" : options.name;
	std::shared_ptr<DBDescriptor> descriptor;
	std::unique_lock<std::mutex> lock(instance->databasesMutex);

	// get or create entry for this path + mode + readOnly combination
	DBKey key{path, options.readOnly};
	auto entryIterator = instance->databases.find(key);
	if (entryIterator == instance->databases.end()) {
		// create entry with empty descriptor and new condition variable
		auto [it, inserted] = instance->databases.emplace(key, DBRegistryEntry());
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
			throw rocksdb_js::DBException(
				"Database already open in '" +
				(entry.descriptor->mode == DBMode::Optimistic ? std::string("optimistic") : std::string("pessimistic")) +
				"' mode"
			);
		}

		DEBUG_LOG("%p DBRegistry::OpenDB Database already open \"%s\"\n", instance.get(), path.c_str());
		DEBUG_LOG("%p DBRegistry::OpenDB Checking for column family \"%s\"\n", instance.get(), name.c_str());

		// manually copy the columns because we don't know which ones are valid.
		// Hold the descriptor's columns mutex across the copy-check-insert so a
		// concurrent drop (which erases its entry via unregisterColumnFamily)
		// cannot interleave and let us reuse a just-dropped column family.
		std::lock_guard<std::mutex> columnsLock(entry.descriptor->columnsMutex);
		bool columnExists = false;
		for (auto& it : entry.descriptor->columns) {
			columns[it.first] = it.second;
			if (it.first == name) {
				DEBUG_LOG("%p DBRegistry::OpenDB Column family \"%s\" already exists\n", instance.get(), name.c_str());
				columnExists = true;
			}
		}
		if (!columnExists) {
			if (entry.descriptor->readOnly) {
				throw rocksdb_js::DBException("Column family \"" + name + "\" not found: cannot create column family in read-only mode");
			}
			DEBUG_LOG("%p DBRegistry::OpenDB Creating column family \"%s\"\n", instance.get(), name.c_str());
			auto column = rocksdb_js::createRocksDBColumnFamily(entry.descriptor->db, name);
			auto columnDescriptor = std::make_shared<ColumnFamilyDescriptor>(column);
			columns[name] = columnDescriptor;
			entry.descriptor->columns[name] = columnDescriptor;
		}
	} else {
		try {
			entry.descriptor = DBDescriptor::open(path, options);
		} catch (...) {
			// Remove the stale entry (null descriptor) so it does not pollute the
			// registry and cause null-dereference crashes in callers such as
			// RegistryStatus that iterate every entry without guarding for null.
			instance->databases.erase(entryIterator);
			throw;
		}
		DEBUG_LOG("%p DBRegistry::OpenDB Stored DBDescriptor %p for \"%s\" (ref count = %ld)\n", instance.get(), entry.descriptor.get(), path.c_str(), entry.descriptor.use_count());
		columns = entry.descriptor->columns;
	}

	// handle the column family
	std::shared_ptr<ColumnFamilyDescriptor> columnDescriptor;
	auto colIterator = columns.find(name);
	if (colIterator != columns.end()) {
		// column family already exists
		DEBUG_LOG("%p DBRegistry::OpenDB Column family \"%s\" found\n", instance.get(), name.c_str());
		columnDescriptor = colIterator->second;
	} else {
		// use the default column family
		DEBUG_LOG("%p DBRegistry::OpenDB Column family \"%s\" not found, using \"default\"\n", instance.get(), name.c_str());
		columnDescriptor = columns[rocksdb::kDefaultColumnFamilyName];
	}

	std::unique_ptr<DBHandleParams> handle = std::make_unique<DBHandleParams>(entry.descriptor, columnDescriptor);
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
		DEBUG_LOG("%p DBRegistry::PurgeAll Purging %zu databases:\n", instance.get(), instance->databases.size());
		uint32_t i = 0;
#endif
		for (auto it = instance->databases.begin(); it != instance->databases.end();) {
			auto descriptor = it->second.descriptor;
			if (descriptor) {
				DEBUG_LOG("%p DBRegistry::PurgeAll %u) Purging \"%s\" (ref count = %ld)\n", instance.get(), i, it->first.path.c_str(), descriptor.use_count());
				descriptor->close();
			}
			it = instance->databases.erase(it);
#ifdef DEBUG
			++i;
#endif
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
 * Get the status of the database registry.
 *
 * @param env - The environment of the Node.js process.
 * @param info - The callback info.
 * @return A JavaScript object with the database registry status.
 */
napi_value DBRegistry::RegistryStatus(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_array(env, &result));

	if (instance) {
		std::unique_lock<std::mutex> lock(instance->databasesMutex);

		size_t i = 0;
		for (auto& [key, entry] : instance->databases) {
			if (!entry.descriptor) {
				continue;
			}
			napi_value database;
			NAPI_STATUS_THROWS(::napi_create_object(env, &database));
			napi_value pathValue;
			NAPI_STATUS_THROWS(::napi_create_string_utf8(env, key.path.c_str(), key.path.size(), &pathValue));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "path", pathValue));
			napi_value modeValue;
			std::string mode = entry.descriptor->mode == DBMode::Optimistic ? "optimistic" : "pessimistic";
			NAPI_STATUS_THROWS(::napi_create_string_utf8(env, mode.c_str(), mode.size(), &modeValue));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "mode", modeValue));
			napi_value refCount;
			NAPI_STATUS_THROWS(::napi_create_uint32(env, static_cast<uint32_t>(entry.descriptor.use_count()), &refCount));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "refCount", refCount));
			napi_value columnFamilies;
			NAPI_STATUS_THROWS(::napi_create_object(env, &columnFamilies));
			for (auto& [name, columnDescriptor] : entry.descriptor->columns) {
				napi_value columnDescriptorValue;
				NAPI_STATUS_THROWS(::napi_create_object(env, &columnDescriptorValue));

				napi_value userSharedBuffers;
				NAPI_STATUS_THROWS(::napi_create_uint32(env, static_cast<uint32_t>(columnDescriptor->userSharedBuffers.size()), &userSharedBuffers));
				NAPI_STATUS_THROWS(::napi_set_named_property(env, columnDescriptorValue, "userSharedBuffers", userSharedBuffers));

				NAPI_STATUS_THROWS(::napi_set_named_property(env, columnFamilies, name.c_str(), columnDescriptorValue));
			}
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "columnFamilies", columnFamilies));
			napi_value transactions;
			NAPI_STATUS_THROWS(::napi_create_uint32(env, static_cast<uint32_t>(entry.descriptor->transactions.size()), &transactions));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "transactions", transactions));
			napi_value closables;
			NAPI_STATUS_THROWS(::napi_create_uint32(env, static_cast<uint32_t>(entry.descriptor->closables.size()), &closables));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "closables", closables));
			napi_value locks;
			NAPI_STATUS_THROWS(::napi_create_uint32(env, static_cast<uint32_t>(entry.descriptor->locks.size()), &locks));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "locks", locks));
			napi_value listenerCallbacks;
			NAPI_STATUS_THROWS(::napi_create_uint32(env, static_cast<uint32_t>(entry.descriptor->events.size()), &listenerCallbacks));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "listenerCallbacks", listenerCallbacks));
			NAPI_STATUS_THROWS(::napi_set_element(env, result, i, database));
			i++;
		}
	}

	return result;
}

/**
 * Scrub per-descriptor event listeners owned by the given env. Called from the
 * env-cleanup hook so a worker thread exiting does not leave threadsafe-fn
 * pointers in shared descriptors that the main thread (or a surviving worker)
 * would later dereference via notify().
 *
 * Snapshots the descriptors under databasesMutex, then drops the lock before
 * calling into each EventEmitter. This keeps the registry lock window short
 * and avoids establishing a new databasesMutex -> events.mutex ordering that
 * isn't already exercised by other call paths.
 */
void DBRegistry::RemoveListenersByEnv(napi_env env) {
	if (!instance) {
		return;
	}

	std::vector<std::shared_ptr<DBDescriptor>> descriptors;
	{
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		descriptors.reserve(instance->databases.size());
		for (auto& [_key, entry] : instance->databases) {
			if (entry.descriptor) {
				descriptors.push_back(entry.descriptor);
			}
		}
	}

	for (auto& descriptor : descriptors) {
		descriptor->removeListenersByEnv(env);
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
			for (auto& [_key, entry] : instance->databases) {
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
