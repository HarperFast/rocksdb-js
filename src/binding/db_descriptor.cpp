#include "db_descriptor.h"
#include <algorithm>

namespace rocksdb_js {

// Custom callback wrapper for threadsafe function with completion support
static void callJsWithCompletion(napi_env env, napi_value js_callback, void* context, void* data) {
	if (env == nullptr || js_callback == nullptr) {
		return;
	}

	CallbackCompletionData* callbackData = static_cast<CallbackCompletionData*>(data);
	if (callbackData == nullptr) {
		DEBUG_LOG("callJsWithCompletion() callbackData is nullptr - calling callback without completion\n")
		// This is a tryLock callback - call it without completion callback
		napi_value global;
		napi_status status = ::napi_get_global(env, &global);
		if (status == napi_ok) {
			napi_value result;
			::napi_call_function(env, global, js_callback, 0, nullptr, &result);
		}
		return;
	}

	DEBUG_LOG("callJsWithCompletion() callbackData is %p\n", callbackData)

	// Create a completion callback function
	napi_value completionCallback;
	napi_status status = ::napi_create_function(
		env,
		"complete",
		NAPI_AUTO_LENGTH,
		[](napi_env env, napi_callback_info info) -> napi_value {
			napi_value result;
			::napi_get_undefined(env, &result);

			// Get the callback data from the function's data
			void* data;
			::napi_get_cb_info(env, info, nullptr, nullptr, nullptr, &data);
			CallbackCompletionData* callbackData = static_cast<CallbackCompletionData*>(data);

			if (callbackData) {
				// Check if this callback is still valid
				if (!callbackData->valid || !callbackData->valid->load()) {
					DEBUG_LOG("callJsWithCompletion() completion callback was invalidated\n");
					delete callbackData;
				} else if (callbackData->descriptor) {
					// Call the completion handler
					DEBUG_LOG("callJsWithCompletion() calling onCallbackComplete for key \"%s\"\n", callbackData->key.c_str());
					callbackData->descriptor->onCallbackComplete(callbackData->key);
					delete callbackData;
				} else {
					delete callbackData;
				}
			}

			return result;
		},
		callbackData,
		&completionCallback
	);

	if (status != napi_ok) {
		DEBUG_LOG("callJsWithCompletion() napi_create_function() failed with status %d\n", status)
		delete callbackData;
		return;
	}

	// Call the original callback with the completion function as parameter
	napi_value global;
	status = ::napi_get_global(env, &global);
	if (status != napi_ok) {
		DEBUG_LOG("callJsWithCompletion() napi_get_global() failed with status %d\n", status)
		delete callbackData;
		return;
	}

	napi_value args[] = { completionCallback };
	napi_value result;
	DEBUG_LOG("callJsWithCompletion() calling completion callback\n")
	status = ::napi_call_function(env, global, js_callback, 1, args, &result);

	if (status != napi_ok) {
		DEBUG_LOG("callJsWithCompletion() napi_call_function() failed with status %d\n", status)
		// If the call failed, clean up and complete the callback anyway
		callbackData->descriptor->onCallbackComplete(callbackData->key);
		delete callbackData;
	} else {
		DEBUG_LOG("callJsWithCompletion() completion callback called successfully\n")
	}
}

/**
 * Creates a new database descriptor.
 */
DBDescriptor::DBDescriptor(
	std::string path,
	DBMode mode,
	std::shared_ptr<rocksdb::DB> db,
	std::unordered_map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> columns
):
	path(path),
	mode(mode),
	db(db),
	valid(std::make_shared<std::atomic<bool>>(true))
{
	for (auto& column : columns) {
		this->columns[column.first] = column.second;
	}
}

/**
 * Destroy the database descriptor and any resources associated to it
 * (transactions, iterators, etc).
 */
DBDescriptor::~DBDescriptor() {
	DEBUG_LOG("%p DBDescriptor::~DBDescriptor() %ld closables\n", this, this->closables.size())

	// Invalidate all pending completion callbacks first
	this->valid->store(false);

	if (this->closables.size()) {
		while (!this->closables.empty()) {
			Closable* handle = *this->closables.begin();
			DEBUG_LOG("%p DBDescriptor::~DBDescriptor() closing closable %p\n", this, handle)
			this->closables.erase(handle);
			handle->close();
		}
	}

	// Clear everything else after all closables are done
	{
		std::lock_guard<std::mutex> lock(this->mutex);
		this->transactions.clear();
		this->columns.clear();
		this->db.reset();
	}
}

/**
 * Registers a database resource to be closed when the descriptor is closed.
 */
void DBDescriptor::attach(Closable* closable) {
	std::lock_guard<std::mutex> lock(this->mutex);
	this->closables.insert(closable);
}

/**
 * Unregisters a database resource from being closed when the descriptor is
 * closed.
 */
void DBDescriptor::detach(Closable* closable) {
	std::lock_guard<std::mutex> lock(this->mutex);
	this->closables.erase(closable);
}

void DBDescriptor::lockCall(
	napi_env env,
	std::string key,
	napi_value callback,
	std::shared_ptr<DBHandle> owner
) {
	bool isNewLock = false;
	this->lockEnqueueCallback(env, key, callback, owner, &isNewLock, false);

	if (isNewLock) {
		// If this is a new lock, start executing immediately in this thread context
		DEBUG_LOG("%p DBDescriptor::lockCall() firing next callback immediately for key \"%s\"\n", this, key.c_str())
		this->fireNextCallbackImmediate(env, key);
	} else {
		DEBUG_LOG("%p DBDescriptor::lockCall() callback queued for key \"%s\"\n", this, key.c_str())
	}
}

/**
 * Enqueues a callback to be called when a lock is acquired.
 */
void DBDescriptor::lockEnqueueCallback(
	napi_env env,
	std::string key,
	napi_value callback,
	std::shared_ptr<DBHandle> owner,
	bool* isNewLock,
	bool skipEnqueueIfExists
) {
	std::lock_guard<std::mutex> lock(this->locksMutex);
	auto lockHandle = this->locks.find(key);

	if (lockHandle == this->locks.end()) {
		// no lock found
		DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback() no lock found for key \"%s\"\n", this, key.c_str())
		auto newLockHandle = std::make_shared<LockHandle>(owner);
		this->locks.emplace(key, newLockHandle);
		lockHandle = this->locks.find(key);
		if (isNewLock != nullptr) {
			*isNewLock = true;
		}
		if (skipEnqueueIfExists) {
			DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback() skipping enqueue because lock already exists\n", this)
			return;
		}
	} else {
		DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback() lock found for key %s\n", this, key.c_str())
	}

	// lock found
	napi_valuetype type;
	NAPI_STATUS_THROWS_VOID(::napi_typeof(env, callback, &type))
	if (type == napi_function) {
		napi_value resource_name;
		NAPI_STATUS_THROWS_VOID(::napi_create_string_latin1(
			env,
			"rocksdb-js.lock",
			NAPI_AUTO_LENGTH,
			&resource_name
		))

		napi_threadsafe_function threadsafeCallback;
		NAPI_STATUS_THROWS_VOID(::napi_create_threadsafe_function(
			env,           // env
			callback,      // func
			nullptr,       // async_resource
			resource_name, // async_resource_name
			0,             // max_queue_size
			1,             // initial_thread_count
			nullptr,       // thread_finalize_data
			nullptr,       // thread_finalize_callback
			nullptr,       // context
			callJsWithCompletion, // call_js_cb
			&threadsafeCallback // [out] callback
		))

		DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback() enqueuing onUnlocked callback\n", this)
		NAPI_STATUS_THROWS_VOID(::napi_unref_threadsafe_function(env, threadsafeCallback))

		// Create a reference to the original callback for immediate execution
		napi_ref js_callback_ref;
		NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, callback, 1, &js_callback_ref))

		lockHandle->second->callbacks.push(threadsafeCallback);
		lockHandle->second->js_callbacks.push(js_callback_ref);
	}
}

bool DBDescriptor::lockExists(std::string key) {
	std::lock_guard<std::mutex> lock(this->locksMutex);
	auto lockHandle = this->locks.find(key);
	bool exists = lockHandle != this->locks.end();
	DEBUG_LOG("%p DBDescriptor::lockExists() %s lock for key \"%s\"\n", this, exists ? "found" : "not found", key.c_str())
	return exists;
}

bool DBDescriptor::lockRelease(std::string key) {
	std::queue<napi_threadsafe_function> callbacks;
	std::queue<napi_ref> js_callbacks;

	{
		std::lock_guard<std::mutex> lock(this->locksMutex);
		auto lockHandle = this->locks.find(key);

		if (lockHandle == this->locks.end()) {
			// no lock found
			DEBUG_LOG("%p DBDescriptor::lockRelease() no lock found\n", this)
			return false;
		}

		// lock found, remove it
		callbacks = std::move(lockHandle->second->callbacks);
		js_callbacks = std::move(lockHandle->second->js_callbacks);
		DEBUG_LOG("%p DBDescriptor::lockRelease() removing lock\n", this)
		this->locks.erase(key);
	}

	DEBUG_LOG("%p DBDescriptor::lockRelease() calling %zu unlock callbacks\n", this, callbacks.size())

	// call the callbacks in order, but stop if any callback fails
	while (!callbacks.empty()) {
		auto callback = callbacks.front();
		callbacks.pop();
		if (!js_callbacks.empty()) {
			js_callbacks.pop(); // Remove corresponding JS callback reference
		}
		DEBUG_LOG("%p DBDescriptor::lockRelease() calling callback %p\n", this, callback)
		napi_status status = ::napi_call_threadsafe_function(callback, nullptr, napi_tsfn_blocking);
		if (status == napi_closing) {
			continue;
		}
		::napi_release_threadsafe_function(callback, napi_tsfn_release);
	}

	return true;
}

/**
 * Adds a transaction to the registry.
 */
void DBDescriptor::transactionAdd(std::shared_ptr<TransactionHandle> txnHandle) {
	uint32_t id = txnHandle->id;
	std::lock_guard<std::mutex> lock(this->mutex);
	this->transactions[id] = txnHandle;
	this->closables.insert(txnHandle.get());
}

/**
 * Retrieves a transaction from the registry.
 */
std::shared_ptr<TransactionHandle> DBDescriptor::transactionGet(uint32_t id) {
	std::lock_guard<std::mutex> lock(this->mutex);
	return this->transactions[id];
}

/**
 * Removes a transaction from the registry.
 */
void DBDescriptor::transactionRemove(uint32_t id) {
	std::lock_guard<std::mutex> lock(this->mutex);
	this->transactions.erase(id);
}

void DBDescriptor::fireNextCallback(const std::string& key) {
	std::unique_lock<std::mutex> lock(this->locksMutex);
	auto lockHandle = this->locks.find(key);

	if (lockHandle == this->locks.end()) {
		DEBUG_LOG("%p DBDescriptor::fireNextCallback() no lock found for key \"%s\"\n", this, key.c_str());
		return;
	}

	auto& handle = lockHandle->second;

	// Try to acquire the "lock" atomically
	bool expected = false;
	if (!handle->isRunning.compare_exchange_strong(expected, true)) {
		// Another callback is already running
		DEBUG_LOG("%p DBDescriptor::fireNextCallback() another callback is already running for key \"%s\"\n", this, key.c_str())
		return;
	}

	// We now "own" the execution for this key
	if (handle->callbacks.empty()) {
		handle->isRunning = false;
		DEBUG_LOG("%p DBDescriptor::fireNextCallback() no callbacks left for key \"%s\", removing lock\n", this, key.c_str())
		// Remove the empty lock handle from the map
		this->locks.erase(key);
		return;
	}

	auto callback = handle->callbacks.front();
	handle->callbacks.pop();

	// Also remove the corresponding JS callback reference
	if (!handle->js_callbacks.empty()) {
		handle->js_callbacks.pop();
	}

	// Release the mutex before calling the callback to avoid holding locks during callback execution
	lock.unlock();

	DEBUG_LOG("%p DBDescriptor::fireNextCallback() calling callback %p for key \"%s\"\n", this, callback, key.c_str())

	// Create callback data that includes the key for completion
	auto* callbackData = new CallbackCompletionData(key, this, this->valid);

	// Call the callback with completion data - use blocking to execute immediately
	napi_status status = ::napi_call_threadsafe_function(callback, callbackData, napi_tsfn_blocking);
	if (status != napi_closing) {
		::napi_release_threadsafe_function(callback, napi_tsfn_release);
	} else {
		// If the function is closing, clean up and continue with next callback
		delete callbackData;
		this->onCallbackComplete(key);
	}
}

void DBDescriptor::fireNextCallbackImmediate(napi_env env, const std::string& key) {
	std::unique_lock<std::mutex> lock(this->locksMutex);
	auto lockHandle = this->locks.find(key);

	if (lockHandle == this->locks.end()) {
		DEBUG_LOG("%p DBDescriptor::fireNextCallbackImmediate() no lock found for key \"%s\"\n", this, key.c_str());
		return;
	}

	auto& handle = lockHandle->second;

	// Try to acquire the "lock" atomically
	bool expected = false;
	if (!handle->isRunning.compare_exchange_strong(expected, true)) {
		// Another callback is already running
		DEBUG_LOG("%p DBDescriptor::fireNextCallbackImmediate() another callback is already running for key \"%s\"\n", this, key.c_str())
		return;
	}

	// We now "own" the execution for this key
	if (handle->callbacks.empty()) {
		handle->isRunning = false;
		DEBUG_LOG("%p DBDescriptor::fireNextCallbackImmediate() no callbacks left for key \"%s\", removing lock\n", this, key.c_str())
		// Remove the empty lock handle from the map
		this->locks.erase(key);
		return;
	}

	auto callback = handle->callbacks.front();
	handle->callbacks.pop();

	// Get the corresponding JS callback reference
	napi_ref js_callback_ref = nullptr;
	if (!handle->js_callbacks.empty()) {
		js_callback_ref = handle->js_callbacks.front();
		handle->js_callbacks.pop();
	}

	// Release the mutex before calling the callback to avoid holding locks during callback execution
	lock.unlock();

	DEBUG_LOG("%p DBDescriptor::fireNextCallbackImmediate() calling callback immediately for key \"%s\"\n", this, key.c_str())

	// Create callback data that includes the key for completion
	auto* callbackData = new CallbackCompletionData(key, this, this->valid);

	if (js_callback_ref != nullptr) {
		// Get the JS callback from the reference
		napi_value js_callback;
		napi_status status = napi_get_reference_value(env, js_callback_ref, &js_callback);
		if (status == napi_ok) {
			// Call the JavaScript function directly using the current environment
			callJsWithCompletion(env, js_callback, nullptr, callbackData);
		} else {
			DEBUG_LOG("%p DBDescriptor::fireNextCallbackImmediate() failed to get JS callback from reference\n", this);
			delete callbackData;
			this->onCallbackComplete(key);
		}
		// Delete the reference
		napi_delete_reference(env, js_callback_ref);
	} else {
		DEBUG_LOG("%p DBDescriptor::fireNextCallbackImmediate() no JS callback reference available\n", this);
		delete callbackData;
		this->onCallbackComplete(key);
	}

	// Release the threadsafe function
	::napi_release_threadsafe_function(callback, napi_tsfn_release);
}

void DBDescriptor::onCallbackComplete(const std::string& key) {
	// Try to mark the current callback as complete and fire the next one
	// Use a try-catch to handle the case where mutexes might be invalid
	try {
		std::lock_guard<std::mutex> lock(this->locksMutex);
		auto lockHandle = this->locks.find(key);
		if (lockHandle != this->locks.end()) {
			lockHandle->second->isRunning = false;
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete() marking as complete for key \"%s\"\n", this, key.c_str());
		} else {
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete() lock already removed for key \"%s\"\n", this, key.c_str());
			return; // Lock was already cleaned up, nothing to do
		}
	} catch (const std::system_error& e) {
		DEBUG_LOG("%p DBDescriptor::onCallbackComplete() failed to acquire lock for key \"%s\": %s\n", this, key.c_str(), e.what());
		return; // Mutex is invalid, descriptor is likely being destroyed
	}

	// Fire the next callback in the queue
	DEBUG_LOG("%p DBDescriptor::onCallbackComplete() firing next callback for key \"%s\"\n", this, key.c_str());
	try {
		this->fireNextCallback(key);
	} catch (const std::system_error& e) {
		DEBUG_LOG("%p DBDescriptor::onCallbackComplete() failed to fire next callback for key \"%s\": %s\n", this, key.c_str(), e.what());
	}
}

} // namespace rocksdb_js
