#include "db_descriptor.h"
#include <algorithm>
#include <memory>

namespace rocksdb_js {

// forward declarations
static void callJsCallback(napi_env env, napi_value jsCallback, void* context, void* data);
static void userSharedBufferFinalize(napi_env env, void* data, void* hint);

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
	db(db)
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
	DEBUG_LOG("%p DBDescriptor::~DBDescriptor Closing \"%s\" (%ld closables)\n", this, this->path.c_str(), this->closables.size())

	std::unique_lock<std::mutex> lock(this->txnsMutex);

	while (!this->closables.empty()) {
		Closable* handle = *this->closables.begin();
		DEBUG_LOG("%p DBDescriptor::~DBDescriptor closing closable %p\n", this, handle)
		this->closables.erase(handle);

		// release the mutex before calling close() to avoid a deadlock
		lock.unlock();
		handle->close();
		lock.lock();
	}

	this->transactions.clear();
	this->columns.clear();
	this->db.reset();
}

/**
 * Registers a database resource to be closed when the descriptor is closed.
 */
void DBDescriptor::attach(Closable* closable) {
	std::lock_guard<std::mutex> lock(this->txnsMutex);
	this->closables.insert(closable);
}

/**
 * Unregisters a database resource from being closed when the descriptor is
 * closed.
 */
void DBDescriptor::detach(Closable* closable) {
	std::lock_guard<std::mutex> lock(this->txnsMutex);
	this->closables.erase(closable);
}

/**
 * Adds the callback to a queue to be executed mutually exclusive and if the
 * lock is available, executes it immediately followed by any newly queued
 * callbacks. Called by `db.withLock()`.
 */
void DBDescriptor::lockCall(
	napi_env env,
	std::string key,
	napi_value callback,
	napi_deferred deferred,
	std::shared_ptr<DBHandle> owner
) {
	bool isNewLock = false;
	this->lockEnqueueCallback(
		env,       // env
		key,       // key
		callback,  // callback
		owner,     // owner
		false,     // skipEnqueueIfNewLock
		deferred,  // deferred
		&isNewLock // [out] isNewLock
	);

	if (!isNewLock) {
		DEBUG_LOG("%p DBDescriptor::lockCall callback queued for key:", this)
		DEBUG_LOG_KEY_LN(key)
		return;
	}

	// lock found
	std::unique_lock<std::mutex> locksMutex(this->locksMutex);
	auto lockHandle = this->locks.find(key);

	if (lockHandle == this->locks.end()) {
		DEBUG_LOG("%p DBDescriptor::lockCall no lock found for key:", this)
		DEBUG_LOG_KEY_LN(key)
		return;
	}

	auto& handle = lockHandle->second;

	// try to acquire the "lock" atomically
	bool expected = false;
	if (!handle->isRunning.compare_exchange_strong(expected, true)) {
		// another callback is already running
		DEBUG_LOG("%p DBDescriptor::lockCall another callback is already running for key:", this)
		DEBUG_LOG_KEY_LN(key)
		return;
	}

	// we now "own" the execution for this key
	if (handle->threadsafeCallbacks.empty()) {
		handle->isRunning = false;
		DEBUG_LOG("%p DBDescriptor::lockCall no callbacks left, removing lock for key:", this)
		DEBUG_LOG_KEY_LN(key)
		// remove the empty lock handle from the map
		this->locks.erase(key);
		return;
	}

	LockCallback lockCallback = handle->threadsafeCallbacks.front();
	handle->threadsafeCallbacks.pop();
	napi_threadsafe_function threadsafeCallback = lockCallback.callback;

	// release the mutex before calling the callback to avoid holding locks
	// during callback execution
	locksMutex.unlock();

	if (!threadsafeCallback) {
		DEBUG_LOG("%p DBDescriptor::lockCall threadsafe lock callback is null for key:", this)
		DEBUG_LOG_KEY_LN(key)
		return;
	}

	DEBUG_LOG("%p DBDescriptor::lockCall calling callback for key:", this)
	DEBUG_LOG_KEY_LN(key)

	// create callback data that includes the key for completion and deferred promise
	auto* callbackData = new LockCallbackCompletionData(key, weak_from_this(), lockCallback.deferred);

	// use threadsafe function instead of direct call
	napi_status status = ::napi_call_threadsafe_function(threadsafeCallback, callbackData, napi_tsfn_blocking);
	if (status != napi_ok && status != napi_closing) {
		DEBUG_LOG("%p DBDescriptor::lockCall failed to call threadsafe function\n", this);
		delete callbackData;
		this->onCallbackComplete(key);
	}

	// release the threadsafe function
	::napi_release_threadsafe_function(threadsafeCallback, napi_tsfn_release);
}

/**
 * Enqueues a callback to be called when a lock is acquired. Called by
 * `db.tryLock()` and `DBDescriptor::lockCall()`.
 */
void DBDescriptor::lockEnqueueCallback(
	napi_env env,
	std::string key,
	napi_value callback,
	std::shared_ptr<DBHandle> owner,
	bool skipEnqueueIfNewLock,
	napi_deferred deferred,
	bool* isNewLock
) {
	std::lock_guard<std::mutex> lock(this->locksMutex);
	std::shared_ptr<LockHandle> lockHandle;
	auto lockHandleIterator = this->locks.find(key);

	if (lockHandleIterator == this->locks.end()) {
		// no lock found
		DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback no lock found for key:", this)
		DEBUG_LOG_KEY_LN(key)
		lockHandle = std::make_shared<LockHandle>(owner, env);
		this->locks.emplace(key, lockHandle);
		if (isNewLock != nullptr) {
			*isNewLock = true;
		}
		if (skipEnqueueIfNewLock) {
			DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback skipping enqueue because lock already exists\n", this)
			return;
		}
	} else {
		DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback lock found for key %s\n", this, key.c_str())
		lockHandle = lockHandleIterator->second;
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
			env,                // env
			callback,           // func
			nullptr,            // async_resource
			resource_name,      // async_resource_name
			0,                  // max_queue_size
			1,                  // initial_thread_count
			nullptr,            // thread_finalize_data
			nullptr,            // thread_finalize_callback
			nullptr,            // context
			callJsCallback,     // call_js_cb
			&threadsafeCallback // [out] callback
		))

		DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback enqueuing callback %p\n", this, threadsafeCallback)
		NAPI_STATUS_THROWS_VOID(::napi_unref_threadsafe_function(env, threadsafeCallback))

		// Create LockCallback and add to queue
		lockHandle->threadsafeCallbacks.push(LockCallback(threadsafeCallback, deferred));
	}
}

/**
 * Checks if a lock exists for the given key. Called by `db.hasLock()`.
 */
bool DBDescriptor::lockExistsByKey(std::string key) {
	std::lock_guard<std::mutex> lock(this->locksMutex);
	auto lockHandle = this->locks.find(key);
	bool exists = lockHandle != this->locks.end();
	DEBUG_LOG("%p DBDescriptor::hasLock %s lock for key \"%s\"\n", this, exists ? "found" : "not found", key.c_str())
	return exists;
}

/**
 * Releases a lock by key. Called by `db.unlock()`.
 */
bool DBDescriptor::lockReleaseByKey(std::string key) {
	std::queue<LockCallback> threadsafeCallbacks;

	{
		std::lock_guard<std::mutex> lock(this->locksMutex);
		auto lockHandle = this->locks.find(key);

		if (lockHandle == this->locks.end()) {
			// no lock found
			DEBUG_LOG("%p DBDescriptor::lockReleaseByKey no lock found\n", this)
			return false;
		}

		// lock found, remove it
		threadsafeCallbacks = std::move(lockHandle->second->threadsafeCallbacks);
		DEBUG_LOG("%p DBDescriptor::lockReleaseByKey removing lock\n", this)
		this->locks.erase(key);
	}

	DEBUG_LOG("%p DBDescriptor::lockReleaseByKey calling %zu unlock callbacks\n", this, threadsafeCallbacks.size())

	// call the callbacks in order, but stop if any callback fails
	while (!threadsafeCallbacks.empty()) {
		auto lockCallback = threadsafeCallbacks.front();
		threadsafeCallbacks.pop();
		DEBUG_LOG("%p DBDescriptor::lockReleaseByKey calling callback %p\n", this, lockCallback.callback)
		napi_status status = ::napi_call_threadsafe_function(lockCallback.callback, nullptr, napi_tsfn_blocking);
		if (status == napi_closing) {
			continue;
		}
		::napi_release_threadsafe_function(lockCallback.callback, napi_tsfn_release);
	}

	return true;
}

/**
 * Releases all locks owned by the given handle. Called by `db.close()`.
 */
void DBDescriptor::lockReleaseByOwner(DBHandle* owner) {
	std::set<napi_threadsafe_function> threadsafeCallbacks;

	{
		std::lock_guard<std::mutex> lock(this->locksMutex);
			DEBUG_LOG("%p DBDescriptor::lockReleaseByOwner checking %d locks if they are owned handle %p\n", this, this->locks.size(), owner)
		for (auto it = this->locks.begin(); it != this->locks.end();) {
			auto lockOwner = it->second->owner.lock();
			if (!lockOwner || lockOwner.get() == owner) {
				DEBUG_LOG("%p DBDescriptor::lockReleaseByOwner found lock %p with %d callbacks\n", this, it->second.get(), it->second->threadsafeCallbacks.size())
				// move all callbacks from the queue
				while (!it->second->threadsafeCallbacks.empty()) {
					threadsafeCallbacks.insert(it->second->threadsafeCallbacks.front().callback);
					it->second->threadsafeCallbacks.pop();
				}
				it = this->locks.erase(it);
			} else {
				++it;
			}
		}
	}

	DEBUG_LOG("%p DBDescriptor::lockReleaseByOwner calling %ld unlock callbacks\n", this, threadsafeCallbacks.size())

	// call the callbacks in order, but stop if any callback fails
	for (auto& callback : threadsafeCallbacks) {
		DEBUG_LOG("%p DBDescriptor::lockReleaseByOwner calling callback %p\n", this, callback)
		napi_status status = ::napi_call_threadsafe_function(callback, nullptr, napi_tsfn_blocking);
		if (status == napi_closing) {
			continue;
		}
		::napi_release_threadsafe_function(callback, napi_tsfn_release);
	}
}

/**
 * Adds a transaction to the registry.
 */
void DBDescriptor::transactionAdd(std::shared_ptr<TransactionHandle> txnHandle) {
	uint32_t id = txnHandle->id;
	std::lock_guard<std::mutex> lock(this->txnsMutex);
	this->transactions.emplace(id, txnHandle);
	this->closables.insert(txnHandle.get());
}

/**
 * Retrieves a transaction from the registry.
 */
std::shared_ptr<TransactionHandle> DBDescriptor::transactionGet(uint32_t id) {
	std::lock_guard<std::mutex> lock(this->txnsMutex);
	return this->transactions[id];
}

/**
 * Removes a transaction from the registry.
 */
void DBDescriptor::transactionRemove(uint32_t id) {
	std::lock_guard<std::mutex> lock(this->txnsMutex);
	this->transactions.erase(id);
}

/**
 * Called when a lock callback completes (async or sync) to clean up the lock
 * handle and fire the next callback in the queue.
 */
void DBDescriptor::onCallbackComplete(const std::string& key) {
	// try to mark the current callback as complete and fire the next one
	// use a try-catch to handle the case where mutexes might be invalid
	try {
		std::lock_guard<std::mutex> lock(this->locksMutex);
		auto lockHandle = this->locks.find(key);
		if (lockHandle != this->locks.end()) {
			lockHandle->second->isRunning = false;
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete marking as complete (key=\"%s\")\n", this, key.c_str());
		} else {
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete lock already removed (key=\"%s\")\n", this, key.c_str());
			return; // lock was already cleaned up, nothing to do
		}
	} catch (const std::exception& e) {
		[[maybe_unused]] auto msg = e.what();
		DEBUG_LOG("%p DBDescriptor::onCallbackComplete failed to acquire lock (key=\"%s\"): %s\n", this, key.c_str(), msg)
		return; // mutex is invalid, descriptor is likely being destroyed
	}

	// fire the next callback in the queue
	DEBUG_LOG("%p DBDescriptor::onCallbackComplete firing next callback (key=\"%s\")\n", this, key.c_str());
	try {
		std::unique_lock<std::mutex> lock(this->locksMutex);
		auto lockHandle = this->locks.find(key);

		if (lockHandle == this->locks.end()) {
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete no lock found (key=\"%s\")\n", this, key.c_str());
			return;
		}

		auto& handle = lockHandle->second;

		// try to acquire the "lock" atomically
		bool expected = false;
		if (!handle->isRunning.compare_exchange_strong(expected, true)) {
			// another callback is already running
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete another callback is already running (key=\"%s\")\n", this, key.c_str())
			return;
		}

		// we now "own" the execution for this key
		if (handle->threadsafeCallbacks.empty()) {
			handle->isRunning = false;
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete no callbacks left (key=\"%s\"), removing lock\n", this, key.c_str())
			// remove the empty lock handle from the map
			this->locks.erase(key);
			return;
		}

		LockCallback lockCallback = handle->threadsafeCallbacks.front();
		handle->threadsafeCallbacks.pop();
		auto callback = lockCallback.callback;

		// release the mutex before calling the callback to avoid holding locks during callback execution
		lock.unlock();

		DEBUG_LOG("%p DBDescriptor::onCallbackComplete calling callback %p (key=\"%s\")\n", this, callback, key.c_str())

		// create callback data that includes the key for completion and deferred promise
		auto* callbackData = new LockCallbackCompletionData(key, weak_from_this(), lockCallback.deferred);

		// use threadsafe function instead of direct call
		napi_status status = ::napi_call_threadsafe_function(callback, callbackData, napi_tsfn_blocking);
		if (status != napi_ok && status != napi_closing) {
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete failed to call threadsafe function (key=\"%s\")\n", this, key.c_str());
			delete callbackData;
			this->onCallbackComplete(key);
		}

		// release the threadsafe function
		::napi_release_threadsafe_function(callback, napi_tsfn_release);
	} catch (const std::exception& e) {
		[[maybe_unused]] auto msg = e.what();
		DEBUG_LOG("%p DBDescriptor::onCallbackComplete failed to fire next callback (key=\"%s\"): %s\n", this, key.c_str(), msg)
	}
}

/**
 * `callJsCallback()` helper macros.
 */
#ifdef DEBUG
	#define CALL_JS_CB_DEBUG_LOG(msg, ...) \
		{ \
			std::string errorStr = rocksdb_js::getNapiExtendedError(env, status); \
			rocksdb_js::debugLog("callJsCallback() " msg ": %s (key=\"%s\")", ##__VA_ARGS__, errorStr.c_str(), callbackData->key.c_str()); \
		}
#else
	#define CALL_JS_CB_DEBUG_LOG(msg, ...)
#endif

#define CALL_JS_CB_NAPI_STATUS_CHECK(call, code, msg, ...) \
	{ \
		napi_status status = (call); \
		if (status != napi_ok) { \
			CALL_JS_CB_DEBUG_LOG(msg, ##__VA_ARGS__); \
			code; \
			return; \
		} \
	}

/**
 * Custom wrapper used by `napi_call_threadsafe_function()` to call user-
 * defined lock callback function. If the lock callback returns a Promise, it
 * is awaited before calling the `onCallbackComplete()` handler.
 *
 * For example, the callback passed into `db.tryLock()` or `db.withLock()` is
 * what is passed in as `jsCallback`. The code then invokes `jsCallback` and
 * checks if it returned a promise. If it did, it calls `then()` on the promise
 * with resolve and reject callbacks that call `onCallbackComplete()`.
 *
 * This mechanism is key to ensuring that only a single async lock callback
 * is running at a time.
 *
 * Note: Node.js runs this function which ever thread (main or worker) that
 * created the threadsafe function.
 */
static void callJsCallback(napi_env env, napi_value jsCallback, void* context, void* data) {
	if (env == nullptr || jsCallback == nullptr) {
		return;
	}

	// get the callback data from the function's data
	LockCallbackCompletionData* callbackData = static_cast<LockCallbackCompletionData*>(data);
	if (callbackData == nullptr) {
		DEBUG_LOG("callJsCallback callbackData is nullptr - calling js callback\n")
		// this is a tryLock callback - call it without completion callback
		napi_value global;
		napi_status status = ::napi_get_global(env, &global);
		if (status == napi_ok) {
			napi_value result;
			::napi_call_function(env, global, jsCallback, 0, nullptr, &result);
		}
		return;
	}

	// create shared_ptr from raw pointer for RAII management
	std::shared_ptr<LockCallbackCompletionData> callbackDataPtr(callbackData);

	// create a completion callback function
	napi_value completionCallback;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_create_function(
			env,
			"rocksdb-js.lock.callback.complete",
			NAPI_AUTO_LENGTH,
			[](napi_env env, napi_callback_info info) -> napi_value {
				// get the callback data from the function's data
				void* data;
				::napi_get_cb_info(env, info, nullptr, nullptr, nullptr, &data);
				LockCallbackCompletionData* callbackData = static_cast<LockCallbackCompletionData*>(data);

				if (callbackData) {
					// check if this callback is still valid
					if (auto desc = callbackData->descriptor.lock()) {
						// call the completion handler
						DEBUG_LOG("callJsCallback calling onCallbackComplete() (key=\"%s\")\n", callbackData->key.c_str());
						desc->onCallbackComplete(callbackData->key);
					} else {
						DEBUG_LOG("callJsCallback completion callback has no descriptor (key=\"%s\")\n", callbackData->key.c_str());
					}
					delete callbackData;
				}

				NAPI_RETURN_UNDEFINED()
			},
			callbackData,
			&completionCallback
		),
		delete callbackData,
		"failed to create completion callback"
	)

	// call the original callback without any arguments
	napi_value global;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_get_global(env, &global),
		delete callbackData,
		"napi_get_global() failed"
	)

	napi_value result;
	DEBUG_LOG("callJsCallback calling js callback (key=\"%s\")\n", callbackData->key.c_str())
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_call_function(env, global, jsCallback, 0, nullptr, &result),
		{
			if (auto desc = callbackData->descriptor.lock()) {
				desc->onCallbackComplete(callbackData->key);
			}
			delete callbackData;
		},
		"napi_call_function() failed"
	)

	// check if the result is a Promise
	napi_value promiseCtor;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_get_named_property(env, global, "Promise", &promiseCtor),
		// not a promise environment, complete immediately
		if (auto desc = callbackData->descriptor.lock()) {
			desc->onCallbackComplete(callbackData->key);
		}
		delete callbackData,
		"failed to get Promise constructor"
	)

	bool isPromise;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_instanceof(env, result, promiseCtor, &isPromise),
		// assume not a promise, complete immediately
		if (auto desc = callbackData->descriptor.lock()) {
			desc->onCallbackComplete(callbackData->key);
		}
		delete callbackData,
		"napi_instanceof() failed"
	)

	if (!isPromise) {
		DEBUG_LOG("callJsCallback result is not a Promise, completing immediately (key=\"%s\")\n", callbackData->key.c_str())

		// If this is a withLock call with a deferred promise, resolve it
		if (callbackData->deferred != nullptr) {
			DEBUG_LOG("callJsCallback resolving deferred promise for synchronous withLock (key=\"%s\")\n", callbackData->key.c_str());
			napi_value undefined;
			napi_get_undefined(env, &undefined);
			napi_resolve_deferred(env, callbackData->deferred, undefined);
		}

		if (auto desc = callbackData->descriptor.lock()) {
			desc->onCallbackComplete(callbackData->key);
		}
		return;
	}

	DEBUG_LOG("callJsCallback result is a Promise, attaching .then() callback (key=\"%s\")\n", callbackData->key.c_str())

	// get the 'then' method from the promise
	napi_value thenMethod;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_get_named_property(env, result, "then", &thenMethod),
		if (auto desc = callbackData->descriptor.lock()) {
			desc->onCallbackComplete(callbackData->key);
		}
		delete callbackData,
		"failed to get .then() method"
	)

	// create resolve and reject callbacks that both complete the lock
	// we need to store the shared_ptr in a way N-API callbacks can access it
	auto* resolveDataPtr = new std::shared_ptr<LockCallbackCompletionData>(callbackDataPtr);

	napi_value resolveCallback;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_create_function(
			env,
			"rocksdb-js.lock.callback.resolve",
			NAPI_AUTO_LENGTH,
			[](napi_env env, napi_callback_info info) -> napi_value {
				napi_value result;
				::napi_get_undefined(env, &result);

				DEBUG_LOG("callJsCallback promise resolve callback\n")

				void* data;
				::napi_get_cb_info(env, info, nullptr, nullptr, nullptr, &data);
				auto* callbackDataPtr = static_cast<std::shared_ptr<LockCallbackCompletionData>*>(data);

				if (callbackDataPtr && *callbackDataPtr) {
					auto& callbackData = **callbackDataPtr;
					auto desc = callbackData.descriptor.lock();
					if (!callbackData.completed.exchange(true) && desc) {
						DEBUG_LOG("callJsCallback promise resolved, calling onCallbackComplete() (key=\"%s\")\n", callbackData.key.c_str());

						// if this is a withLock call with a deferred promise, resolve it
						if (callbackData.deferred != nullptr) {
							DEBUG_LOG("callJsCallback resolving deferred promise for withLock (key=\"%s\")\n", callbackData.key.c_str());
							napi_value undefined;
							napi_get_undefined(env, &undefined);
							napi_resolve_deferred(env, callbackData.deferred, undefined);
						}

						desc->onCallbackComplete(callbackData.key);
					} else {
						DEBUG_LOG("callJsCallback promise resolve callback already completed (key=\"%s\")\n", callbackData.key.c_str());
					}
				}

				// clean up the shared_ptr wrapper
				delete callbackDataPtr;
				return result;
			},
			resolveDataPtr,
			&resolveCallback
		),
		/* cleanup */ {
			if (auto desc = callbackData->descriptor.lock()) {
				desc->onCallbackComplete(callbackData->key);
			}
			delete resolveDataPtr;
		},
		"failed to create resolve callback"
	)

	// create reject callback - shared_ptr handles safe sharing between resolve/reject
	auto* rejectDataPtr = new std::shared_ptr<LockCallbackCompletionData>(callbackDataPtr);

	napi_value rejectCallback;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_create_function(
			env,
			"rocksdb-js.lock.callback.reject",
			NAPI_AUTO_LENGTH,
			[](napi_env env, napi_callback_info info) -> napi_value {
				napi_value result;
				::napi_get_undefined(env, &result);

				void* data;
				::napi_get_cb_info(env, info, nullptr, nullptr, nullptr, &data);
				auto* callbackDataPtr = static_cast<std::shared_ptr<LockCallbackCompletionData>*>(data);

				if (callbackDataPtr && *callbackDataPtr) {
					auto& callbackData = **callbackDataPtr;
					if (auto desc = callbackData.descriptor.lock()) {
						DEBUG_LOG("callJsCallback promise rejected, calling onCallbackComplete() (key=\"%s\")\n", callbackData.key.c_str());

						// if this is a withLock call with a deferred promise, reject it
						if (callbackData.deferred != nullptr) {
							DEBUG_LOG("callJsCallback rejecting deferred promise for withLock (key=\"%s\")\n", callbackData.key.c_str());
							// get the error from the first argument of the reject callback
							size_t argc = 1;
							napi_value argv[1];
							napi_get_cb_info(env, info, &argc, argv, nullptr, nullptr);
							napi_value error = argc > 0 ? argv[0] : nullptr;
							if (error == nullptr) {
								napi_get_undefined(env, &error);
							}
							napi_reject_deferred(env, callbackData.deferred, error);
						}

						desc->onCallbackComplete(callbackData.key);
					}
				}

				// clean up the shared_ptr wrapper
				delete callbackDataPtr;
				return result;
			},
			rejectDataPtr,
			&rejectCallback
		),
		/* cleanup */ {
			if (auto desc = callbackData->descriptor.lock()) {
				desc->onCallbackComplete(callbackData->key);
			}
			delete rejectDataPtr;
			delete resolveDataPtr;
		},
		"failed to create reject callback"
	)

	// call `promise.then(resolveCallback, rejectCallback)` for key "key"
	napi_value thenArgs[] = { resolveCallback, rejectCallback };
	napi_value thenResult;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_call_function(env, result, thenMethod, 2, thenArgs, &thenResult),
		{
			if (auto desc = callbackData->descriptor.lock()) {
				desc->onCallbackComplete(callbackData->key);
			}
			delete resolveDataPtr;
			delete rejectDataPtr;
		},
		"failed to call .then()"
	)
}

/**
 * Finalize callback for when the user shared ArrayBuffer is garbage collected.
 * It removes the corresponding entry from the `userSharedBuffers` map to and
 * calls the finalize function, which removes the event listener, if applicable.
 */
static void userSharedBufferFinalize(napi_env env, void* data, void* hint) {
	auto* finalizeData = static_cast<UserSharedBufferFinalizeData*>(hint);

	if (auto descriptor = finalizeData->descriptor.lock()) {
		DEBUG_LOG("%p userSharedBufferFinalize for key:", descriptor.get())
		DEBUG_LOG_KEY(finalizeData->key)
		DEBUG_LOG_MSG(" (use_count: %ld)\n", finalizeData->sharedData ? finalizeData->sharedData.use_count() : 0);

		if (finalizeData->callbackRef) {
			napi_value callback;
			if (::napi_get_reference_value(env, finalizeData->callbackRef, &callback) == napi_ok) {
				DEBUG_LOG("%p userSharedBufferFinalize removing listener", descriptor.get())
				descriptor->removeListener(env, finalizeData->key, callback);
			}
		}

		std::string key = finalizeData->key;
		std::weak_ptr<DBDescriptor> weakDesc = descriptor;

		std::lock_guard<std::mutex> lock(descriptor->userSharedBuffersMutex);
		auto it = descriptor->userSharedBuffers.find(key);
		if (it != descriptor->userSharedBuffers.end() && it->second == finalizeData->sharedData) {
			// check if this shared_ptr is about to become the last reference
			// (map entry + this finalizer's copy = 2, after finalizer exits only map = 1)
			if (finalizeData->sharedData.use_count() <= 2) {
				descriptor->userSharedBuffers.erase(key);
				DEBUG_LOG("%p userSharedBufferFinalize removed user shared buffer for key:", descriptor.get())
				DEBUG_LOG_KEY_LN(key)
			}
		}
	} else {
		DEBUG_LOG("userSharedBufferFinalize descriptor was already destroyed for key:")
		DEBUG_LOG_KEY_LN(finalizeData->key)
	}

	delete finalizeData;
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
 * ```ts
 * const db = new NativeDatabase();
 * const userSharedBuffer = db.getUserSharedBuffer('foo', new ArrayBuffer(10));
 * ```
 */
napi_value DBDescriptor::getUserSharedBuffer(
	napi_env env,
	std::string key,
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
		))

		DEBUG_LOG("%p DBDescriptor::getUserSharedBuffer Initializing user shared buffer with default buffer size: %ld\n", this, size)
		it = this->userSharedBuffers.emplace(key, std::make_shared<UserSharedBufferData>(data, size)).first;
	}

	DEBUG_LOG("%p DBDescriptor::getUserSharedBuffer Creating external ArrayBuffer with size %ld for key:", this, it->second->size)
	DEBUG_LOG_KEY_LN(key)

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
	))
	return result;
}

#define NAPI_STATUS_THROWS_FREE_DATA(call) \
	{ \
		napi_status status = (call); \
		if (status != napi_ok) { \
			std::string errorStr = rocksdb_js::getNapiExtendedError(env, status); \
			::napi_throw_error(env, nullptr, errorStr.c_str()); \
			DEBUG_LOG("callListenerCallback error: %s\n", errorStr.c_str()) \
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
napi_ref DBDescriptor::addListener(
	napi_env env,
	std::string key,
	napi_value callback,
	std::weak_ptr<DBHandle> owner
) {
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
		it = this->listenerCallbacks.emplace(key, std::vector<std::shared_ptr<ListenerCallback>>()).first;
	}

	napi_ref callbackRef;
	NAPI_STATUS_THROWS(::napi_create_reference(env, callback, 1, &callbackRef))
	it->second.emplace_back(std::make_shared<ListenerCallback>(env, threadsafeCallback, callbackRef, owner));

	DEBUG_LOG("%p DBDescriptor::addListener added listener for key:", this)
	DEBUG_LOG_KEY(key);
	DEBUG_LOG_MSG(" (listeners=%zu)\n", it->second.size())

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
napi_value DBDescriptor::emit(napi_env env, std::string key, napi_value args) {
	napi_value result;
	ListenerData* data = nullptr;
	std::vector<std::weak_ptr<ListenerCallback>> listenersToCall;
	uint32_t argc = 0;

	{
		std::lock_guard<std::mutex> lock(this->listenerCallbacksMutex);
		auto it = this->listenerCallbacks.find(key);

		if (it == this->listenerCallbacks.end()) {
			DEBUG_LOG("%p DBDescriptor::emit key has no listeners:", this)
			DEBUG_LOG_KEY_LN(key)
			NAPI_STATUS_THROWS(::napi_get_boolean(env, false, &result));
			return result;
		}

		if (args) {
			bool isArray = false;
			NAPI_STATUS_THROWS(::napi_is_array(env, args, &isArray))
			if (isArray) {
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
		}

		// copy weak pointers to avoid holding the mutex during callback execution
		listenersToCall.reserve(it->second.size());
		for (auto& listener : it->second) {
			listenersToCall.push_back(std::weak_ptr<ListenerCallback>(listener));
		}

		DEBUG_LOG("%p DBDescriptor::emit calling %zu listener%s with %d arg%s for key:",
			this, listenersToCall.size(), listenersToCall.size() == 1 ? "" : "s", argc, argc == 1 ? "" : "s")
		DEBUG_LOG_KEY_LN(key)
	}

	for (auto& weakListener : listenersToCall) {
		if (auto listener = weakListener.lock()) {
			// create a separate copy of data for each listener to avoid double-delete
			ListenerData* listenerData = data ? new ListenerData(*data) : nullptr;
			if (listener->threadsafeCallback) {
				::napi_call_threadsafe_function(listener->threadsafeCallback, listenerData, napi_tsfn_blocking);
			}
		}
	}

	// clean up the original data since we made copies
	if (data) {
		delete data;
	}

	NAPI_STATUS_THROWS(::napi_get_boolean(env, true, &result));
	return result;
}

napi_value DBDescriptor::listeners(napi_env env, std::string key) {
	size_t count = 0;
	std::lock_guard<std::mutex> lock(this->listenerCallbacksMutex);
	auto it = this->listenerCallbacks.find(key);

	if (it != this->listenerCallbacks.end()) {
		count = it->second.size();
	}

	DEBUG_LOG("%p DBDescriptor::listeners key has %zu listener%s:", this, count, count == 1 ? "" : "s")
	DEBUG_LOG_KEY_LN(key)

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
napi_value DBDescriptor::removeListener(napi_env env, std::string key, napi_value callback) {
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
			if (env != (*listener)->env) {
				++listener;
				continue;
			}

			napi_value fn;
			NAPI_STATUS_THROWS(::napi_get_reference_value((*listener)->env, (*listener)->callbackRef, &fn))
			bool isEqual = false;
			NAPI_STATUS_THROWS(::napi_strict_equals(env, fn, callback, &isEqual))
			if (isEqual) {
				listener = it->second.erase(listener);
				DEBUG_LOG("%p DBDescriptor::removeListener removed listener for key:", this)
				DEBUG_LOG_KEY(key);
				DEBUG_LOG_MSG(" (listeners=%zu)\n", it->second.size())
				found = true;
				break;
			}

			++listener;
		}

		if (it->second.empty()) {
			DEBUG_LOG("%p DBDescriptor::removeListener All listeners removed, removing key:", this)
			DEBUG_LOG_KEY_LN(key);
			this->listenerCallbacks.erase(it);
		}
	} else {
		DEBUG_LOG("%p DBDescriptor::removeListener No listeners found for key:", this)
		DEBUG_LOG_KEY_LN(key)
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_get_boolean(env, found, &result));
	return result;
}

/**
 * Removes all listeners owned by the specified DBHandle.
 *
 * @param owner The DBHandle that owns the listeners to remove.
 */
void DBDescriptor::removeListenersByOwner(DBHandle* owner) {
	std::lock_guard<std::mutex> lock(this->listenerCallbacksMutex);

	DEBUG_LOG("%p DBDescriptor::removeListenersByOwner removing listeners for owner %p\n", this, owner)

	for (auto keyIt = this->listenerCallbacks.begin(); keyIt != this->listenerCallbacks.end(); ) {
		auto& listeners = keyIt->second;

		// remove listeners owned by this handle
		listeners.erase(
			std::remove_if(listeners.begin(), listeners.end(),
				[owner](const std::shared_ptr<ListenerCallback>& callback) {
					auto sharedOwner = callback->owner.lock();
					bool shouldRemove = (sharedOwner.get() == owner) || callback->owner.expired();
					if (shouldRemove) {
						DEBUG_LOG("%p DBDescriptor::removeListenersByOwner removing listener", owner)
						// note: can't safely log key here as we're in iterator
					}
					return shouldRemove;
				}),
			listeners.end()
		);

		// remove the key entirely if no listeners remain
		if (listeners.empty()) {
			DEBUG_LOG("%p DBDescriptor::removeListenersByOwner removing empty key\n", this)
			keyIt = this->listenerCallbacks.erase(keyIt);
		} else {
			++keyIt;
		}
	}
}

} // namespace rocksdb_js
