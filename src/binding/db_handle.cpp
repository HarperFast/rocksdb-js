#include <algorithm>
#include "db_handle.h"
#include "db_descriptor.h"
#include "db_registry.h"

namespace rocksdb_js {

// forward declarations
static void userSharedBufferFinalize(napi_env env, void* data, void* hint);

/**
 * Creates a new DBHandle.
 */
DBHandle::DBHandle(napi_env env, napi_ref exportsRef)
	: descriptor(nullptr), env(env), exportsRef(exportsRef) {}

/**
 * Close the DBHandle and destroy it.
 */
DBHandle::~DBHandle() {
	DEBUG_LOG("%p DBHandle::~DBHandle\n", this);
	this->close();
}

/**
 * Clears all data in the database's column family.
 */
rocksdb::Status DBHandle::clear() {
	if (!this->opened() || this->isCancelled()) {
		DEBUG_LOG("%p Database closed during clear operation\n", this);
		return rocksdb::Status::Aborted("Database closed during clear operation");
	}

	// compact the database to reclaim space
	rocksdb::Status status = this->descriptor->db->CompactRange(
		rocksdb::CompactRangeOptions(),
		this->column.get(),
		nullptr,
		nullptr
	);
	if (!status.ok()) {
		return status;
	}
	// it appears we do not need to call WaitForCompact for this to work
	return rocksdb::DeleteFilesInRange(this->descriptor->db.get(), this->column.get(), nullptr, nullptr);
}

/**
 * Closes the DBHandle.
 */
void DBHandle::close() {
	DEBUG_LOG("%p DBHandle::close dbDescriptor=%p (ref count = %ld)\n", this, this->descriptor.get(), this->descriptor.use_count());

	// cancel all active async work before closing
	this->cancelAllAsyncWork();

	// wait for all async work to complete before closing
	this->waitForAsyncWorkCompletion();

	// clean up user shared buffers
	std::lock_guard<std::mutex> lock(this->userSharedBuffersMutex);
	for (auto& [key, sharedData] : this->userSharedBuffers) {
		DEBUG_LOG("%p DBHandle::close Removing user shared buffer for key:", this);
		DEBUG_LOG_KEY_LN(key);
		sharedData.reset();
	}
	this->userSharedBuffers.clear();

	// decrement the reference count on the column and descriptor
	if (this->column) {
		this->column.reset();
	}

	if (this->descriptor) {
		// clean up listeners owned by this handle before releasing locks
		this->descriptor->removeListenersByOwner(this);
		this->descriptor->lockReleaseByOwner(this);

		// release our reference to the descriptor
		this->descriptor.reset();
	}

	// clean up transaction log references
	for (auto& [name, ref] : this->logRefs) {
		DEBUG_LOG("%p DBHandle::close Releasing transaction log JS reference \"%s\"\n", this, name.c_str());
		::napi_delete_reference(this->env, ref);
	}
	this->logRefs.clear();

	DEBUG_LOG("%p DBHandle::close Handle closed\n", this);
}

/**
 * Creates a new user shared buffer or returns an existing one.
 *
 * @param env The environment of the current callback.
 * @param key The key of the user shared buffer.
 * @param defaultBuffer The default buffer to use if the user shared buffer does
 * not exist.
 * @param callbackRef An optional callback reference to remove the listener when
 * the user shared buffer is garbage collected.
 * @returns The user shared buffer.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const userSharedBuffer = db.getUserSharedBuffer('foo', new ArrayBuffer(10));
 * ```
 */
napi_value DBHandle::getUserSharedBuffer(
	napi_env env,
	std::string& key,
	napi_value defaultBuffer,
	napi_ref callbackRef
) {
	bool isArrayBuffer;
	NAPI_STATUS_THROWS(::napi_is_arraybuffer(env, defaultBuffer, &isArrayBuffer));
	if (!isArrayBuffer) {
		::napi_throw_error(env, nullptr, "Default buffer must be an ArrayBuffer");
		return nullptr;
	}

	std::lock_guard<std::mutex> lock(this->userSharedBuffersMutex);

	auto it = this->userSharedBuffers.find(key);
	if (it == this->userSharedBuffers.end()) {
		// shared buffer does not exist, create it
		void* data;
		size_t size;

		NAPI_STATUS_THROWS(::napi_get_arraybuffer_info(
			env,
			defaultBuffer,
			&data,
			&size
		));

		DEBUG_LOG("%p DBDescriptor::getUserSharedBuffer Initializing user shared buffer with default buffer size: %zu\n", this, size);
		it = this->userSharedBuffers.emplace(key, std::make_shared<UserSharedBufferData>(data, size)).first;
	}

	DEBUG_LOG("%p DBDescriptor::getUserSharedBuffer Creating external ArrayBuffer with size %zu for key:", this, it->second->size);
	DEBUG_LOG_KEY_LN(key);

	// create finalize data that holds the key, a weak reference to this
	// descriptor, and a shared_ptr to keep the data alive
	auto* finalizeData = new UserSharedBufferFinalizeData(key, weak_from_this(), it->second, callbackRef);

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_external_arraybuffer(
		env,
		it->second->data,         // data
		it->second->size,         // size
		userSharedBufferFinalize, // finalize_cb
		finalizeData,             // finalize_hint
		&result                   // [out] result
	));
	return result;
}

/**
 * Has the DBRegistry open a RocksDB database and then move it's handle properties
 * to this DBHandle.
 *
 * @param path - The filesystem path to the database.
 * @param options - The options for the database.
 */
void DBHandle::open(const std::string& path, const DBOptions& options) {
	auto handleParams = DBRegistry::OpenDB(path, options);
	this->column = std::move(handleParams->column);
	this->descriptor = std::move(handleParams->descriptor);
	this->disableWAL = options.disableWAL;
	this->path = path;

	// Note: We cannot attach this handle to the descriptor because we don't
	// have the smart pointer to the dbHandle instance, so the caller needs to
	// do it.

	// at this point, the DBDescriptor has at least 2 refs: the registry and this handle
}

/**
 * Checks if the referenced database is opened.
 */
bool DBHandle::opened() const {
	if (this->descriptor && this->descriptor->db) {
		return true;
	}
	return false;
}

/**
 * Unreferences a transaction log instance.
 */
void DBHandle::unrefLog(const std::string& name) {
	auto it = this->logRefs.find(name);
	if (it == this->logRefs.end()) {
		DEBUG_LOG("%p DBHandle::unrefLog Transaction log \"%s\" not found (size=%zu)\n", this, name.c_str(), this->logRefs.size());
		return;
	}

	DEBUG_LOG("%p DBHandle::unrefLog Unreferencing transaction log \"%s\" (size=%zu)\n", this, name.c_str(), this->logRefs.size());
	::napi_delete_reference(this->env, it->second);
	this->logRefs.erase(it);
}

/**
 * Get or create a transaction log instance.
 */
napi_value DBHandle::useLog(napi_env env, napi_value jsDatabase, std::string& name) {
	napi_value instance;

	// check if we already have it cached
	auto existingRef = this->logRefs.find(name);
	if (existingRef != this->logRefs.end()) {
		napi_status status = ::napi_get_reference_value(env, existingRef->second, &instance);

		if (status == napi_ok && instance != nullptr) {
			// DEBUG_LOG("%p DBHandle::useLog Returning existing transaction log \"%s\"\n", this, name.c_str());
			return instance;
		}

		DEBUG_LOG("%p DBHandle::useLog Removing stale reference to transaction log \"%s\"\n", this, name.c_str());
		::napi_delete_reference(env, existingRef->second);
		this->logRefs.erase(name);
	}

	DEBUG_LOG("%p DBHandle::useLog Creating new transaction log \"%s\"\n", this, name.c_str());

	napi_value exports;
	NAPI_STATUS_THROWS(::napi_get_reference_value(env, this->exportsRef, &exports));

	napi_value args[2];
	args[0] = jsDatabase;

	napi_value transactionLogCtor;
	NAPI_STATUS_THROWS(::napi_get_named_property(env, exports, "TransactionLog", &transactionLogCtor));

	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, name.c_str(), name.size(), &args[1]));

	NAPI_STATUS_THROWS(::napi_new_instance(env, transactionLogCtor, 2, args, &instance));

	napi_ref ref;
	NAPI_STATUS_THROWS(::napi_create_reference(env, instance, 0, &ref));
	this->logRefs.emplace(name, ref);

	return instance;
}

/**
 * Finalize callback for when the user shared ArrayBuffer is garbage collected.
 * It removes the corresponding entry from the `userSharedBuffers` map to and
 * calls the finalize function, which removes the event listener, if applicable.
 */
static void userSharedBufferFinalize(napi_env env, void* data, void* hint) {
	DEBUG_LOG("userSharedBufferFinalize data=%p hint=%p\n", data, hint);
	auto* finalizeData = static_cast<UserSharedBufferFinalizeData*>(hint);

	if (auto dbHandle = finalizeData->dbHandle.lock()) {
		DEBUG_LOG("%p userSharedBufferFinalize for key:", dbHandle.get());
		DEBUG_LOG_KEY(finalizeData->key);
		DEBUG_LOG_MSG(" (use_count: %ld)\n", finalizeData->sharedData ? finalizeData->sharedData.use_count() : 0);

		if (finalizeData->callbackRef) {
			napi_value callback;
			if (::napi_get_reference_value(env, finalizeData->callbackRef, &callback) == napi_ok) {
				DEBUG_LOG("%p userSharedBufferFinalize removing listener", dbHandle.get());
				dbHandle->descriptor->removeListener(env, finalizeData->key, callback);
			}
		}

		std::string key = finalizeData->key;

		std::lock_guard<std::mutex> lock(dbHandle->userSharedBuffersMutex);
		auto it = dbHandle->userSharedBuffers.find(key);
		if (it != dbHandle->userSharedBuffers.end() && it->second == finalizeData->sharedData) {
			// check if this shared_ptr is about to become the last reference
			// (map entry + this finalizer's copy = 2, after finalizer exits only map = 1)
			if (finalizeData->sharedData.use_count() <= 2) {
				dbHandle->userSharedBuffers.erase(key);
				DEBUG_LOG("%p userSharedBufferFinalize removed user shared buffer for key:", dbHandle.get());
				DEBUG_LOG_KEY_LN(key);
			}
		}
	} else {
		DEBUG_LOG("userSharedBufferFinalize dbHandle was already destroyed for key:");
		DEBUG_LOG_KEY_LN(finalizeData->key);
	}

	delete finalizeData;
}

} // namespace rocksdb_js
