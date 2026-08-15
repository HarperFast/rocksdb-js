#include <chrono>
#include <vector>
#include "database/db_registry.h"
#include "transaction/transaction_handle.h"
#include "database/db_settings.h"
#include "core/test_seam.h"
#include "napi/macros.h"
#include "core/platform.h"
#include "core/compression.h"
#include "napi/helpers.h"
#include "napi/async.h"
#include "rocksdb/table.h"
#include <chrono>
#include <exception>
#include <thread>

namespace rocksdb_js {

namespace {

class DestroyPathGuard final {
public:
	DestroyPathGuard(
		std::mutex& mutex,
		std::condition_variable& condition,
		std::unordered_set<std::string>& destroyingPaths,
		const std::string& path
	) : mutex(mutex), condition(condition), destroyingPaths(destroyingPaths), path(path) {}

	~DestroyPathGuard() {
		{
			std::lock_guard<std::mutex> lock(this->mutex);
			this->destroyingPaths.erase(this->path);
		}
		this->condition.notify_all();
	}

private:
	std::mutex& mutex;
	std::condition_variable& condition;
	std::unordered_set<std::string>& destroyingPaths;
	const std::string& path;
};

struct ClosingDescriptor final {
	DBKey key;
	std::shared_ptr<DBDescriptor> descriptor;
	std::shared_ptr<std::condition_variable> condition;
	bool closed = false;
};

} // namespace

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
		// Only erase the entry we claimed. A brand-new descriptor cannot appear
		// because OpenDB blocks until we notify below.
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
	const auto deadline = std::chrono::steady_clock::now() +
		std::chrono::seconds(DBSettings::getInstance().getLifecycleWaitSeconds());
	{
		std::unique_lock<std::mutex> lock(instance->databasesMutex);
		if (!instance->lifecycleCondition.wait_until(lock, deadline, [&]() {
			return instance->destroyingPaths.find(path) == instance->destroyingPaths.end();
		})) {
			throw rocksdb_js::DBException("Timed out waiting to destroy database \"" + path + "\": another destroy is still in progress");
		}
		instance->destroyingPaths.insert(path);
	}
	// A physical destroy has several throwing stages; the path gate must never
	// survive one of them and permanently block every later open.
	DestroyPathGuard pathGuard(
		instance->databasesMutex,
		instance->lifecycleCondition,
		instance->destroyingPaths,
		path
	);

	while (true) {
		std::vector<ClosingDescriptor> claimed;
		std::vector<ClosingDescriptor> alreadyClosing;
		{
			std::lock_guard<std::mutex> lock(instance->databasesMutex);
			claimed.reserve(instance->databases.size());
			alreadyClosing.reserve(instance->databases.size());
			for (auto& [key, entry] : instance->databases) {
				if (key.path != path || !entry.descriptor) {
					continue;
				}
				ClosingDescriptor closing{key, entry.descriptor, entry.condition};
				if (entry.descriptor->beginClose()) {
					claimed.push_back(std::move(closing));
				} else {
					alreadyClosing.push_back(std::move(closing));
				}
			}
		}

		// Keep entries discoverable while finishClose runs: env cleanup uses the
		// registry to remove callbacks owned by a worker that exits mid-close.
		std::exception_ptr closeError;
		for (auto& closing : claimed) {
			DEBUG_LOG("%p DBRegistry::DestroyDB Closing descriptor %p for \"%s\" (ref count = %ld)\n",
				instance.get(), closing.descriptor.get(), path.c_str(), closing.descriptor.use_count());
			try {
				closing.descriptor->finishClose();
				closing.closed = true;
			} catch (...) {
				if (!closeError) {
					closeError = std::current_exception();
				}
			}
		}

		{
			std::lock_guard<std::mutex> lock(instance->databasesMutex);
			for (const auto& closing : claimed) {
				if (!closing.closed) {
					continue;
				}
				auto entry = instance->databases.find(closing.key);
				if (entry != instance->databases.end() && entry->second.descriptor == closing.descriptor) {
					instance->databases.erase(entry);
				}
			}
		}
		for (const auto& closing : claimed) {
			if (closing.closed) {
				closing.condition->notify_all();
			}
		}
		if (closeError) {
			std::rethrow_exception(closeError);
		}
		for (const auto& closing : claimed) {
			if (!closing.closed) {
				continue;
			}
			const size_t refCountAfterClose = closing.descriptor.use_count();
			if (refCountAfterClose > 1) {
				std::string errorMsg = "Cannot destroy database: " + std::to_string(refCountAfterClose - 1) +
					" reference(s) still held after closing all handles. This may indicate handles not properly closed or JavaScript objects not yet garbage collected.";
				DEBUG_LOG("%p DBRegistry::DestroyDB Error: %s\n", instance.get(), errorMsg.c_str());
				throw rocksdb_js::DBException(errorMsg);
			}
		}

		if (alreadyClosing.empty()) {
			break;
		}
		for (const auto& closing : alreadyClosing) {
			std::unique_lock<std::mutex> lock(instance->databasesMutex);
			if (!closing.condition->wait_until(lock, deadline, [&]() {
				auto entry = instance->databases.find(closing.key);
				return entry == instance->databases.end() || entry->second.descriptor != closing.descriptor;
			})) {
				throw rocksdb_js::DBException("Timed out waiting to destroy database \"" + path + "\": an open descriptor is still closing");
			}
		}
	}

	{
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		for (auto it = instance->databases.begin(); it != instance->databases.end();) {
			if (it->first.path == path) {
				it = instance->databases.erase(it);
			} else {
				++it;
			}
		}
	}

	// Now the database lock should be released, safe to destroy
	const int destroyDelayMs = testDelayMs("ROCKSDB_JS_DESTROY_DELAY_MS");
	if (destroyDelayMs > 0) {
		std::this_thread::sleep_for(std::chrono::milliseconds(destroyDelayMs));
	}
	if (testFailureEnabled("ROCKSDB_JS_DESTROY_FAILURE")) {
		throw rocksdb_js::DBException("Injected database destruction failure");
	}
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
	const auto deadline = std::chrono::steady_clock::now() +
		std::chrono::seconds(DBSettings::getInstance().getLifecycleWaitSeconds());
	DBKey key{path, options.readOnly};
	decltype(instance->databases)::iterator entryIterator;
	while (true) {
		if (!instance->destroyingPaths.empty() &&
			instance->destroyingPaths.find(path) != instance->destroyingPaths.end()
		) {
			DEBUG_LOG("%p DBRegistry::OpenDB Database \"%s\" is being destroyed, waiting\n", instance.get(), path.c_str());
			if (!instance->lifecycleCondition.wait_until(lock, deadline, [&]() {
				return instance->destroyingPaths.find(path) == instance->destroyingPaths.end();
			})) {
				throw rocksdb_js::DBException("Timed out opening database \"" + path + "\": destruction is still in progress");
			}
			continue;
		}

		entryIterator = instance->databases.find(key);
		if (entryIterator == instance->databases.end()) {
			entryIterator = instance->databases.emplace(key, DBRegistryEntry()).first;
		}
		if (!entryIterator->second.descriptor || !entryIterator->second.descriptor->isClosing()) {
			break;
		}

		DEBUG_LOG("%p DBRegistry::OpenDB Database \"%s\" is closing, waiting for removal\n", instance.get(), path.c_str());
		auto condition = entryIterator->second.condition;
		if (!condition->wait_until(lock, deadline, [&]() {
			auto current = instance->databases.find(key);
			return current == instance->databases.end() ||
				!current->second.descriptor || !current->second.descriptor->isClosing();
		})) {
			throw rocksdb_js::DBException("Timed out opening database \"" + path + "\": the previous instance is still closing");
		}
	}

	auto& entry = entryIterator->second;

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

		// max_log_file_size and info_log_level are DB-wide (`DBOptions`) settings
		// fixed at first open; the process-global descriptor is reused across
		// handles/envs, so a second open can't change them. Reject an explicitly
		// different request rather than silently ignore it — but let a plain
		// reopen (non-explicit default / unset) inherit the live value, so a
		// default-carrying reopen after a custom first open does NOT falsely
		// reject (mirrors the compression discipline below).
		{
			rocksdb::DBOptions current = entry.descriptor->db->GetDBOptions();
			// Widen the live size_t to uint64_t rather than narrowing the request to
			// size_t: on a 32-bit build narrowing would truncate a >4GB request and
			// could falsely compare equal (skipping a real conflict).
			if (options.maxLogFileSizeExplicit &&
				static_cast<uint64_t>(current.max_log_file_size) != options.maxLogFileSize
			) {
				throw rocksdb_js::DBException(
					"Database \"" + path + "\" is already open with maxLogFileSize " +
					std::to_string(current.max_log_file_size) + " bytes; cannot reopen it with " +
					std::to_string(options.maxLogFileSize) + " bytes"
				);
			}
			if (options.infoLogLevel.has_value() &&
				static_cast<int>(current.info_log_level) != static_cast<int>(*options.infoLogLevel)
			) {
				throw rocksdb_js::DBException(
					"Database \"" + path + "\" is already open with infoLogLevel " +
					std::to_string(static_cast<int>(current.info_log_level)) + "; cannot reopen it with " +
					std::to_string(static_cast<int>(*options.infoLogLevel))
				);
			}
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
			// Preserve retained settings while applying every per-CF option from
			// the handle creating this family.
			auto cfOptions = buildColumnFamilyOptions(options, entry.descriptor->cfOptions);
			if (options.compression) {
				cfOptions.compression = *options.compression;
				cfOptions.blob_compression_type = *options.compression;
				cfOptions.compression_opts.level = options.compressionLevel
					? *options.compressionLevel
					: rocksdb::CompressionOptions::kDefaultCompressionLevel;
			}
			auto column = rocksdb_js::createRocksDBColumnFamily(
				entry.descriptor->db, name, cfOptions
			);
			auto columnDescriptor = std::make_shared<ColumnFamilyDescriptor>(column);
			columns[name] = columnDescriptor;
			entry.descriptor->columns[name] = columnDescriptor;
		} else if (options.compressionExplicit && options.compression) {
			// The column family is already open in this process (the DBDescriptor
			// is process-global and shared across handles/envs). Compression is
			// fixed per column family at creation, so a second open explicitly
			// asking for a different algorithm or level cannot take effect on the
			// reused handle — reject it rather than silently ignore the request. A
			// plain reopen (compression defaulted, not explicit) inherits the live
			// setting and skips this check.
			rocksdb::ColumnFamilyHandle* cf = columns[name]->column.get();
			rocksdb::Options current = entry.descriptor->db->GetOptions(cf);
			// The effective request omitting a level is "the algorithm's default
			// level" (see applyCompression in db_descriptor.cpp), so compare against
			// the default sentinel rather than skipping the level check — otherwise
			// reopening a zstd-level-19 CF as plain zstd would silently inherit 19.
			int requestedLevel = options.compressionLevel
				? *options.compressionLevel
				: rocksdb::CompressionOptions::kDefaultCompressionLevel;
			bool algorithmDiffers = current.compression != *options.compression;
			// The request applies the algorithm to blob files too, so a live CF whose
			// blobs are at a different algorithm (e.g. a legacy CF opened plainly with
			// block=snappy but blob=none) is also a conflict — otherwise values at the
			// 2KB blob threshold would stay uncompressed while the open appears to succeed.
			bool blobDiffers = current.blob_compression_type != *options.compression;
			bool levelDiffers = current.compression_opts.level != requestedLevel;
			if (algorithmDiffers || blobDiffers || levelDiffers) {
				std::string requested = rocksdb_js::compressionNameFromType(*options.compression);
				if (options.compressionLevel) {
					requested += " (level " + std::to_string(*options.compressionLevel) + ")";
				}
				throw rocksdb_js::DBException(
					"Column family \"" + name + "\" is already open with compression \"" +
					rocksdb_js::compressionNameFromType(current.compression) + " (blob " +
					rocksdb_js::compressionNameFromType(current.blob_compression_type) + ", level " +
					std::to_string(current.compression_opts.level) + ")\"; cannot reopen it with \"" +
					requested + "\""
				);
			}
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
			if (instance->destroyingPaths.find(it->first.path) != instance->destroyingPaths.end()) {
				++it;
				continue;
			}
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
			// A bare count cannot tell a request in flight from a database that can never reclaim
			// again, so report each handle's id and age. Deliberately NOT its snapshotSet/state:
			// those are written by the owning thread and by the commit-completion callback without
			// any lock, so reading them from another environment here would be a data race —
			// txnsMutex covers the map's membership, not a handle's mutable fields. id and age are
			// fixed before the handle is published to the registry.
			struct TxnSummary {
				uint32_t id;
				double ageMs;
			};
			std::vector<TxnSummary> txnSummaries;
			size_t closablesCount;
			{
				auto now = std::chrono::steady_clock::now();
				std::lock_guard<std::mutex> txnsLock(entry.descriptor->txnsMutex);
				txnSummaries.reserve(entry.descriptor->transactions.size());
				for (auto& [txnId, txnHandle] : entry.descriptor->transactions) {
					if (!txnHandle) {
						continue;
					}
					txnSummaries.push_back({
						txnId,
						std::chrono::duration<double, std::milli>(now - txnHandle->createdAt).count()
					});
				}
				closablesCount = entry.descriptor->closables.size();
			}

			napi_value transactions;
			NAPI_STATUS_THROWS(::napi_create_uint32(env, static_cast<uint32_t>(txnSummaries.size()), &transactions));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "transactions", transactions));
			napi_value transactionDetails;
			NAPI_STATUS_THROWS(::napi_create_array(env, &transactionDetails));
			for (size_t t = 0; t < txnSummaries.size(); t++) {
				const auto& summary = txnSummaries[t];
				napi_value detail;
				NAPI_STATUS_THROWS(::napi_create_object(env, &detail));
				napi_value value;
				NAPI_STATUS_THROWS(::napi_create_uint32(env, summary.id, &value));
				NAPI_STATUS_THROWS(::napi_set_named_property(env, detail, "id", value));
				NAPI_STATUS_THROWS(::napi_create_double(env, summary.ageMs, &value));
				NAPI_STATUS_THROWS(::napi_set_named_property(env, detail, "ageMs", value));
				NAPI_STATUS_THROWS(::napi_set_element(env, transactionDetails, static_cast<uint32_t>(t), detail));
			}
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "transactionDetails", transactionDetails));
			napi_value closables;
			NAPI_STATUS_THROWS(::napi_create_uint32(env, static_cast<uint32_t>(closablesCount), &closables));
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
 * Close each descriptor's transactions owned by handles created on the given
 * env. Called from the module env-cleanup hook, on the dying env's own thread,
 * before Node frees the env — so a worker that exits with a pending
 * transaction does not leak it into the process-global descriptor for a later
 * Shutdown to walk with a dangling env (HarperFast/rocksdb-js#741). Mirrors
 * RemoveListenersByEnv: snapshot descriptors under databasesMutex, close
 * outside the lock.
 */
void DBRegistry::CloseTransactionsByEnv(napi_env env) {
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
		descriptor->closeTransactionsByEnv(env);
	}
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
 * Release each descriptor's commit-completion tsfn owned by the given env.
 * Called from the module env-cleanup hook so a worker thread exiting does not
 * leave a threadsafe-fn the shared commit thread would later call into a
 * torn-down env. Mirrors RemoveListenersByEnv: snapshot the descriptors under
 * databasesMutex, then release outside the lock (releaseCommitCompletionsByEnv
 * takes each descriptor's own commitMutex).
 */
void DBRegistry::ReleaseCommitCompletionsByEnv(napi_env env) {
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
		descriptor->releaseCommitCompletionsByEnv(env);
	}
}

/**
 * Cancels each descriptor's pending park timeouts owned by the given env.
 * Called from the module env-cleanup hook so a worker thread exiting does not
 * leave a coordinated-retry park's timeout thread calling into a torn-down
 * env ~ROCKSDB_JS_PARK_TIMEOUT_MS later. Mirrors ReleaseCommitCompletionsByEnv.
 */
void DBRegistry::ReleaseParkTimeoutsByEnv(napi_env env) {
	if (!instance) {
		return;
	}

	std::vector<std::shared_ptr<ParkTimeoutRegistry>> registries;
	{
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		registries.reserve(instance->databases.size());
		for (auto& [_key, entry] : instance->databases) {
			if (entry.descriptor) {
				registries.push_back(entry.descriptor->parkTimeouts);
			}
		}
	}

	for (auto& registry : registries) {
		registry->releaseByEnv(env);
	}
}

/**
 * Shutdown will force all databases to flush in-memory data to disk and purge the registry.
 */
void DBRegistry::Shutdown() {
	if (instance) {
		std::vector<ClosingDescriptor> descriptorsToClose;

		{
			std::lock_guard<std::mutex> lock(instance->databasesMutex);
			DEBUG_LOG("%p DBRegistry::Shutdown Shutting down %zu databases\n", instance.get(), instance->databases.size());
			descriptorsToClose.reserve(instance->databases.size());

			// Claim each close while holding the registry lock so DestroyDB can
			// safely wait on the matching erase-and-notify below.
			for (auto& [key, entry] : instance->databases) {
				if (!entry.descriptor ||
					instance->destroyingPaths.find(key.path) != instance->destroyingPaths.end()
				) {
					continue;
				}
				ClosingDescriptor closing{key, entry.descriptor, entry.condition};
				if (entry.descriptor->beginClose()) {
					descriptorsToClose.push_back(std::move(closing));
				}
			}
		}

		// Close all descriptors without holding the lock
		std::exception_ptr closeError;
		for (auto& closing : descriptorsToClose) {
			DEBUG_LOG("%p DBRegistry::Shutdown Closing database: %s\n", instance.get(), closing.descriptor->path.c_str());
			try {
				closing.descriptor->finishClose();
				closing.closed = true;
			} catch (...) {
				if (!closeError) {
					closeError = std::current_exception();
				}
				continue;
			}
			{
				std::lock_guard<std::mutex> lock(instance->databasesMutex);
				auto entry = instance->databases.find(closing.key);
				if (entry != instance->databases.end() && entry->second.descriptor == closing.descriptor) {
					instance->databases.erase(entry);
				}
			}
			closing.condition->notify_all();
		}
		if (closeError) {
			std::rethrow_exception(closeError);
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
