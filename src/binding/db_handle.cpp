#include "db_handle.h"
#include "db_registry.h"

namespace rocksdb_js {

/**
 * Creates a new DBHandle.
 */
DBHandle::DBHandle()
	: descriptor(nullptr) {}

/**
 * Creates a new DBHandle from a DBDescriptor.
 */
DBHandle::DBHandle(std::shared_ptr<DBDescriptor> descriptor)
	: descriptor(descriptor) {}

/**
 * Close the DBHandle and destroy it.
 */
DBHandle::~DBHandle() {
	DEBUG_LOG("%p DBHandle::~DBHandle\n", this)
	this->close();
}

/**
 * Clears all data in the database's column family.
 */
rocksdb::Status DBHandle::clear(uint32_t batchSize, uint64_t& deleted) {
	ASSERT_OPENED_AND_NOT_CANCELLED(this, "clear")

	// create a write batch and iterator
	rocksdb::WriteBatch batch;
	std::unique_ptr<rocksdb::Iterator> it = std::unique_ptr<rocksdb::Iterator>(
		this->descriptor->db->NewIterator(
			rocksdb::ReadOptions(),
			this->column.get()
		)
	);

	DEBUG_LOG("%p DBHandle::Clear Starting clear with batch size %u\n", this, batchSize)

	// iterate over the database and add each key to the write batch
	rocksdb::Status status;
	deleted = 0;
	it->SeekToFirst();
	bool valid = it->Valid();
	while (valid) {
		ASSERT_OPENED_AND_NOT_CANCELLED(this, "clear")

		batch.Delete(it->key());
		++deleted;
		it->Next();
		valid = it->Valid();

		// if we've reached the end of the iterator or the batch is full, write the batch
		if (!valid || batch.Count() >= batchSize) {
			ASSERT_OPENED_AND_NOT_CANCELLED(this, "clear")
			DEBUG_LOG("%p DBHandle::Clear Writing batch with %zu keys\n", this, batch.Count())

			status = this->descriptor->db->Write(rocksdb::WriteOptions(), &batch);
			if (!status.ok()) {
				return status;
			}

			batch.Clear();
		}
	}

	// Check one final time before compaction
	ASSERT_OPENED_AND_NOT_CANCELLED(this, "clear")

	// compact the database to reclaim space
	return this->descriptor->db->CompactRange(
		rocksdb::CompactRangeOptions(),
		this->column.get(),
		nullptr,
		nullptr
	);
}

/**
 * Closes the DBHandle.
 */
void DBHandle::close() {
	DEBUG_LOG("%p DBHandle::close dbDescriptor=%p (ref count = %ld)\n", this, this->descriptor.get(), this->descriptor.use_count())

	// cancel all active async work before closing
	this->cancelAllAsyncWork();

	// wait for all async work to complete before closing
	this->waitForAsyncWorkCompletion();

	// decrement the reference count on the column and descriptor
	if (this->column) {
		this->column.reset();
	}

	if (this->descriptor) {
		this->descriptor->lockReleaseByOwner(this);
		this->descriptor.reset();
	}

	DEBUG_LOG("%p DBHandle::close Handle closed\n", this)
}

/**
 * Has the DBRegistry open a RocksDB database and then move it's handle properties
 * to this DBHandle.
 *
 * @param path - The filesystem path to the database.
 * @param options - The options for the database.
 */
void DBHandle::open(const std::string& path, const DBOptions& options) {
	auto handle = DBRegistry::OpenDB(path, options);
	this->column = std::move(handle->column);
	this->descriptor = std::move(handle->descriptor);
	// at this point, the DBDescriptor has at least 2 refs: the registry and this handle
	DEBUG_LOG("%p DBHandle::open dbhandle %p is no longer needed, moved DBDescriptor %p to this handle (ref count = %ld)\n", this, handle.get(), this->descriptor.get(), this->descriptor.use_count())
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

#define NAPI_STATUS_THROWS_FREE_DATA(call) \
	{ \
		napi_status status = (call); \
		if (status != napi_ok) { \
			std::string errorStr = rocksdb_js::getNapiExtendedError(env, status); \
			::napi_throw_error(env, nullptr, errorStr.c_str()); \
			DEBUG_LOG("NAPI_STATUS_THROWS_FREE_DATA error: %s\n", errorStr.c_str()) \
			if (listenerData) { \
				delete listenerData; \
			} \
			return; \
		} \
	}

/**
 * Custom wrapper used by `napi_call_threadsafe_function()` to call user-
 * defined event listener callback functions.
 */
static void callListenerCallback(napi_env env, napi_value jsCallback, void* context, void* data) {
	if (env == nullptr || jsCallback == nullptr) {
		return;
	}

	ListenerData* listenerData = static_cast<ListenerData*>(data);
	uint32_t argc = 0;
	napi_value* argv = nullptr;
	napi_value global;

	NAPI_STATUS_THROWS_FREE_DATA(::napi_get_global(env, &global))

	if (listenerData) {
		// only deserialize the emitted data if it exists
		napi_value json;
		napi_value parse;
		napi_value jsonString;
		napi_value arrayArgs;
		NAPI_STATUS_THROWS_FREE_DATA(::napi_get_named_property(env, global, "JSON", &json))
		NAPI_STATUS_THROWS_FREE_DATA(::napi_get_named_property(env, json, "parse", &parse))
		NAPI_STATUS_THROWS_FREE_DATA(::napi_create_string_utf8(env, listenerData->args.c_str(), listenerData->args.length(), &jsonString))
		NAPI_STATUS_THROWS_FREE_DATA(::napi_call_function(env, json, parse, 1, &jsonString, &arrayArgs))
		NAPI_STATUS_THROWS_FREE_DATA(::napi_get_array_length(env, arrayArgs, &argc))

		// need to convert from a js array to an array of napi values
		argv = new napi_value[argc];
		for (uint32_t i = 0; i < argc; i++) {
			NAPI_STATUS_THROWS_FREE_DATA(::napi_get_element(env, arrayArgs, i, &argv[i]))
		}

		delete listenerData;
		listenerData = nullptr;
	}

	// call the listener
	napi_value result;
	NAPI_STATUS_THROWS_FREE_DATA(::napi_call_function(env, global, jsCallback, argc, argv, &result))
}

/**
 * Adds an listener to the database descriptor.
 *
 * @param env The environment of the current callback.
 * @param key The key.
 * @param callback The callback to call when the event is emitted.
 */
napi_ref DBHandle::addListener(napi_env env, std::string key, napi_value callback) {
	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, callback, &type))
	if (type != napi_function) {
		::napi_throw_error(env, nullptr, "Callback must be a function");
		return nullptr;
	}

	napi_value resource_name;
	NAPI_STATUS_THROWS(::napi_create_string_latin1(
		env,
		"rocksdb-js.listener",
		NAPI_AUTO_LENGTH,
		&resource_name
	))

	napi_threadsafe_function threadsafeCallback;
	NAPI_STATUS_THROWS(::napi_create_threadsafe_function(
		env,                  // env
		callback,             // func
		nullptr,              // async_resource
		resource_name,        // async_resource_name
		0,                    // max_queue_size
		1,                    // initial_thread_count
		nullptr,              // thread_finalize_data
		nullptr,              // thread_finalize_callback
		nullptr,              // context
		callListenerCallback, // call_js_cb
		&threadsafeCallback   // [out] callback
	))

	NAPI_STATUS_THROWS(::napi_unref_threadsafe_function(env, threadsafeCallback))

	std::lock_guard<std::mutex> lock(this->listenerCallbacksMutex);
	auto it = this->listenerCallbacks.find(key);
	if (it == this->listenerCallbacks.end()) {
		it = this->listenerCallbacks.emplace(key, std::vector<ListenerCallback>()).first;
	}

	napi_ref callbackRef;
	NAPI_STATUS_THROWS(::napi_create_reference(env, callback, 1, &callbackRef))

	it->second.emplace_back(env, threadsafeCallback, callbackRef);

	DEBUG_LOG("%p DBHandle::addListener added listener for key:", this)
	DEBUG_LOG_KEY(key);
	DEBUG_LOG(" (listeners=%zu)\n", it->second.size())

	return callbackRef;
}

/**
 * Emits an event from the database descriptor.
 *
 * @param env The environment of the current callback.
 * @param key The key.
 * @returns `true` if there were at least one listener, `false` otherwise.
 *
 * @example
 * ```ts
 * const db = new NativeDatabase();
 * db.addListener('foo', () => {
 *   console.log('foo');
 * });
 *
 * db.emit('foo'); // returns `true` if there were listeners
 * db.emit('bar'); // returns `false` if there were no listeners
 * ```
 */
napi_value DBHandle::emit(napi_env env, std::string key, napi_value args) {
	napi_value result;
	ListenerData* data = nullptr;
	std::lock_guard<std::mutex> lock(this->listenerCallbacksMutex);
	auto it = this->listenerCallbacks.find(key);

	bool isArray = false;
	NAPI_STATUS_THROWS(::napi_is_array(env, args, &isArray))
	if (isArray) {
		uint32_t argc = 0;
		NAPI_STATUS_THROWS(::napi_get_array_length(env, args, &argc))
		if (argc > 0) {
			napi_value global;
			napi_value json;
			napi_value stringify;
			napi_value jsonString;
			size_t len;
			NAPI_STATUS_THROWS(::napi_get_global(env, &global));
			NAPI_STATUS_THROWS(::napi_get_named_property(env, global, "JSON", &json));
			NAPI_STATUS_THROWS(::napi_get_named_property(env, json, "stringify", &stringify));
			NAPI_STATUS_THROWS(::napi_call_function(env, json, stringify, 1, &args, &jsonString));
			NAPI_STATUS_THROWS(::napi_get_value_string_utf8(env, jsonString, nullptr, 0, &len));
			data = new ListenerData(len);
			NAPI_STATUS_THROWS(::napi_get_value_string_utf8(env, jsonString, &data->args[0], len + 1, nullptr));
		}
	}

	if (it == this->listenerCallbacks.end()) {
		DEBUG_LOG("%p DBHandle::emit key has no listeners:", this)
		DEBUG_LOG_KEY_LN(key)
		NAPI_STATUS_THROWS(::napi_get_boolean(env, false, &result));
		return result;
	}

	DEBUG_LOG("%p DBHandle::emit calling %zu listener%s for key:", this, it->second.size(), it->second.size() == 1 ? "" : "s")
	DEBUG_LOG_KEY_LN(key)

	for (auto& listener : it->second) {
		// create a separate copy of data for each listener to avoid double-delete
		ListenerData* listenerData = data ? new ListenerData(*data) : nullptr;
		::napi_call_threadsafe_function(listener.threadsafeCallback, listenerData, napi_tsfn_blocking);
	}

	// clean up the original data since we made copies
	if (data) {
		delete data;
	}

	NAPI_STATUS_THROWS(::napi_get_boolean(env, true, &result));
	return result;
}

napi_value DBHandle::listeners(napi_env env, std::string key) {
	size_t count = 0;
	std::lock_guard<std::mutex> lock(this->listenerCallbacksMutex);
	auto it = this->listenerCallbacks.find(key);

	if (it != this->listenerCallbacks.end()) {
		count = it->second.size();
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_uint32(env, static_cast<uint32_t>(count), &result));
	return result;
}

/**
 * Removes an listener from the database descriptor.
 *
 * @param env The environment of the current callback.
 * @param key The key.
 * @param callback The callback to remove.
 */
napi_value DBHandle::removeListener(napi_env env, std::string key, napi_value callback) {
	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, callback, &type))
	if (type != napi_function) {
		::napi_throw_error(env, nullptr, "Callback must be a function");
		return nullptr;
	}

	bool found = false;
	std::lock_guard<std::mutex> lock(this->listenerCallbacksMutex);
	auto it = this->listenerCallbacks.find(key);

	if (it != this->listenerCallbacks.end()) {
		for (auto listener = it->second.begin(); listener != it->second.end();) {
			if (env != listener->env) {
				++listener;
				continue;
			}

			napi_value fn;
			NAPI_STATUS_THROWS(::napi_get_reference_value(listener->env, listener->callbackRef, &fn))
			bool isEqual = false;
			NAPI_STATUS_THROWS(::napi_strict_equals(env, fn, callback, &isEqual))
			if (isEqual) {
				listener = it->second.erase(listener);
				DEBUG_LOG("%p DBHandle::removeListener removed listener for key:", this)
				DEBUG_LOG_KEY(key);
				DEBUG_LOG(" (listeners=%zu)\n", it->second.size())
				found = true;
				break;
			}

			++listener;
		}
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_get_boolean(env, found, &result));
	return result;
}

} // namespace rocksdb_js
