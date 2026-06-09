#include "napi/event_emitter.h"
#include <algorithm>
#include "core/debug.h"
#include "napi/helpers.h"
#include "napi/macros.h"

namespace rocksdb_js {

/**
 * Threadsafe-function finalizer. Node-API guarantees this runs on the JS
 * thread once the tsfn's ref count drops to zero, making it the safe place
 * to delete the napi_ref backing the listener callback.
 *
 * We pass the napi_ref via `thread_finalize_data` at create time; ownership
 * of the ref transfers to the tsfn from that point on.
 */
static void finalizeListenerCallback(napi_env env, void* finalize_data, void* /*finalize_hint*/) {
	napi_ref callbackRef = static_cast<napi_ref>(finalize_data);
	if (callbackRef) {
		napi_status status = ::napi_delete_reference(env, callbackRef);
		if (status != napi_ok) {
			DEBUG_LOG("finalizeListenerCallback failed to delete callback reference (status=%d)\n", status);
		}
	}
}

/**
 * Releases the threadsafe function held by a listener. Safe to call from any
 * thread: `napi_release_threadsafe_function` is thread-safe, and the
 * napi_ref is owned by the tsfn — it will be deleted on the JS thread by
 * `finalizeListenerCallback` when the tsfn's ref count reaches zero.
 *
 * The local napi_ref pointer is nulled so subsequent passes (e.g. the
 * removeListenersByOwner erase predicate) can identify this listener as
 * already torn down.
 */
static void releaseListenerResources(ListenerCallback& listener) {
	// Atomically take ownership of the tsfn so a concurrent caller (e.g.,
	// notify on a worker thread, or a parallel removeListenersByOwner pass)
	// can't release the same handle twice.
	napi_threadsafe_function tsfn = listener.threadsafeCallback.exchange(nullptr);
	if (tsfn) {
		napi_status status = ::napi_release_threadsafe_function(tsfn, napi_tsfn_release);
		if (status != napi_ok) {
			DEBUG_LOG("releaseListenerResources failed to release threadsafe callback (status=%d)\n", status);
		}
	}
	listener.callbackRef = nullptr;
}

#define NAPI_STATUS_THROWS_FREE_DATA(call) \
	do { \
		napi_status status = (call); \
		if (status != napi_ok) { \
			std::string errorStr = rocksdb_js::getNapiExtendedError(env, status); \
			::napi_throw_error(env, nullptr, errorStr.c_str()); \
			DEBUG_LOG("callListenerCallback error: %s\n", errorStr.c_str()); \
			if (listenerData) { \
				delete listenerData; \
			} \
			if (argv) { \
				delete[] argv; \
			} \
			return; \
		} \
	} while (0)

/**
 * Threadsafe-function trampoline that deserializes the listener args (if any)
 * and invokes the JS callback on the JS thread.
 */
static void callListenerCallback(napi_env env, napi_value jsCallback, void* unusedContext, void* data) {
	(void)unusedContext;
	if (env == nullptr || jsCallback == nullptr) {
		return;
	}

	ListenerData* listenerData = static_cast<ListenerData*>(data);
	uint32_t argc = 0;
	napi_value* argv = nullptr;
	napi_value global;

	NAPI_STATUS_THROWS_FREE_DATA(::napi_get_global(env, &global));

	if (listenerData != nullptr) {
		DEBUG_LOG("callListenerCallback deserializing listenerData (listenerData=%p)\n", listenerData);

		// only deserialize the emitted data if it exists and is not empty
		if (!listenerData->args.empty()) {
			napi_value json;
			napi_value parse;
			napi_value jsonString;
			napi_value arrayArgs;
			NAPI_STATUS_THROWS_FREE_DATA(::napi_get_named_property(env, global, "JSON", &json));
			NAPI_STATUS_THROWS_FREE_DATA(::napi_get_named_property(env, json, "parse", &parse));
			NAPI_STATUS_THROWS_FREE_DATA(::napi_create_string_utf8(env, listenerData->args.c_str(), listenerData->args.length(), &jsonString));
			NAPI_STATUS_THROWS_FREE_DATA(::napi_call_function(env, json, parse, 1, &jsonString, &arrayArgs));
			NAPI_STATUS_THROWS_FREE_DATA(::napi_get_array_length(env, arrayArgs, &argc));

			argv = new napi_value[argc];
			for (uint32_t i = 0; i < argc; i++) {
				NAPI_STATUS_THROWS_FREE_DATA(::napi_get_element(env, arrayArgs, i, &argv[i]));
			}
		} else {
			DEBUG_LOG("callListenerCallback listenerData has empty args\n");
		}

		delete listenerData;
		listenerData = nullptr;
	} else {
		DEBUG_LOG("callListenerCallback listenerData is nullptr\n");
	}

	napi_value result;
	DEBUG_LOG("callListenerCallback calling listener callback\n");
	napi_status status = ::napi_call_function(env, global, jsCallback, argc, argv, &result);
	if (status != napi_ok) {
		DEBUG_LOG("callListenerCallback failed to call listener callback (status=%d)\n", status);
		std::string errorStr = rocksdb_js::getNapiExtendedError(env, status);
		DEBUG_LOG("callListenerCallback error: %s\n", errorStr.c_str());
		::napi_throw_error(env, nullptr, errorStr.c_str());
	} else {
		DEBUG_LOG("callListenerCallback called listener callback successfully!\n");
	}

	if (argv) {
		delete[] argv;
	}
}

ListenerData* serializeListenerArgs(napi_env env, napi_value value) {
	bool isArray = false;
	if (::napi_is_array(env, value, &isArray) != napi_ok || !isArray) {
		return nullptr;
	}

	uint32_t argc = 0;
	if (::napi_get_array_length(env, value, &argc) != napi_ok || argc == 0) {
		return nullptr;
	}

	napi_value global;
	napi_value json;
	napi_value stringify;
	napi_value jsonString;
	size_t len = 0;

	if (::napi_get_global(env, &global) != napi_ok) return nullptr;
	if (::napi_get_named_property(env, global, "JSON", &json) != napi_ok) return nullptr;
	if (::napi_get_named_property(env, json, "stringify", &stringify) != napi_ok) return nullptr;
	if (::napi_call_function(env, json, stringify, 1, &value, &jsonString) != napi_ok) return nullptr;
	if (::napi_get_value_string_utf8(env, jsonString, nullptr, 0, &len) != napi_ok) return nullptr;

	auto* data = new ListenerData(len);
	if (::napi_get_value_string_utf8(env, jsonString, &data->args[0], len + 1, nullptr) != napi_ok) {
		delete data;
		return nullptr;
	}

	return data;
}

napi_ref EventEmitter::addListener(
	napi_env env,
	const std::string& key,
	napi_value callback,
	std::weak_ptr<void> owner
) {
	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, callback, &type));
	if (type != napi_function) {
		::napi_throw_error(env, nullptr, "Callback must be a function");
		return nullptr;
	}

	napi_value resourceName;
	NAPI_STATUS_THROWS(::napi_create_string_latin1(
		env,
		"rocksdb-js.listener",
		NAPI_AUTO_LENGTH,
		&resourceName
	));

	napi_ref callbackRef;
	NAPI_STATUS_THROWS(::napi_create_reference(env, callback, 1, &callbackRef));

	// `hasOwner` distinguishes ownerless global listeners (don't reap on
	// removeListenersByOwner) from per-object listeners whose owner has
	// already expired (do reap).
	bool hasOwner = (owner.lock() != nullptr);
	auto listenerCallback = std::make_shared<ListenerCallback>(env, callbackRef, std::move(owner), hasOwner);

	// Pass the napi_ref as `thread_finalize_data`. Node-API guarantees the
	// finalize callback runs on the JS thread when the tsfn's ref count
	// hits zero, so napi_delete_reference is called from a legal context
	// even when releaseListenerResources is invoked from a worker thread
	// (e.g., a DBDescriptor destroyed off the JS thread).
	//
	// Use a local napi_threadsafe_function for the out-param because
	// listenerCallback->threadsafeCallback is std::atomic<...> and its
	// address can't be passed to a C API expecting a plain pointer slot.
	napi_threadsafe_function tsfn = nullptr;
	napi_status status = ::napi_create_threadsafe_function(
		env,                       // env
		callback,                  // func
		nullptr,                   // async_resource
		resourceName,              // async_resource_name
		0,                         // max_queue_size
		1,                         // initial_thread_count
		callbackRef,               // thread_finalize_data
		finalizeListenerCallback,  // thread_finalize_callback
		nullptr,                   // context
		callListenerCallback,      // call_js_cb
		&tsfn                      // [out] callback
	);

	if (status != napi_ok) {
		DEBUG_LOG("%p EventEmitter::addListener failed to create threadsafe function (status=%d)\n", this, status);
		std::string errorStr = rocksdb_js::getNapiExtendedError(env, status, "Failed to create threadsafe function");
		napi_status delStatus = ::napi_delete_reference(env, callbackRef);
		if (delStatus != napi_ok) {
			DEBUG_LOG("%p EventEmitter::addListener failed to delete reference (status=%d)\n", this, delStatus);
		}
		::napi_throw_error(env, nullptr, errorStr.c_str());
		return nullptr;
	}

	listenerCallback->threadsafeCallback.store(tsfn);

	napi_status unrefStatus = ::napi_unref_threadsafe_function(env, tsfn);
	if (unrefStatus != napi_ok) {
		// Tear the tsfn down so the napi_ref (now owned by the tsfn finalizer)
		// is released on the JS thread instead of leaking for the lifetime of
		// the env. Then propagate the original error.
		DEBUG_LOG("%p EventEmitter::addListener unref failed (status=%d), releasing tsfn\n", this, unrefStatus);
		std::string errorStr = rocksdb_js::getNapiExtendedError(env, unrefStatus, "Failed to unref threadsafe function");
		releaseListenerResources(*listenerCallback);
		::napi_throw_error(env, nullptr, errorStr.c_str());
		return nullptr;
	}

	std::lock_guard<std::mutex> lock(this->mutex);
	auto it = this->callbacks.find(key);
	if (it == this->callbacks.end()) {
		it = this->callbacks.emplace(key, std::vector<std::shared_ptr<ListenerCallback>>()).first;
	}
	it->second.push_back(listenerCallback);

	DEBUG_LOG("%p EventEmitter::addListener added listener for key:", this);
	DEBUG_LOG_KEY(key);
	DEBUG_LOG_MSG(" (listeners=%zu)\n", it->second.size());

	return callbackRef;
}

bool EventEmitter::notify(const std::string& key, ListenerData* data) {
	// Snapshot + acquire under the mutex: every tsfn release path also runs
	// under this mutex, so while we hold it the tsfn count cannot drop. We
	// bump each tsfn's count via napi_acquire_threadsafe_function, which
	// keeps it alive across the post-mutex call below even if another
	// thread (e.g., removeListener, removeListenersByEnv on a dying env)
	// releases concurrently. Without the acquire there would be a window
	// where notify holds a tsfn pointer that Node has just freed.
	std::vector<napi_threadsafe_function> acquiredTsfns;
	{
		std::lock_guard<std::mutex> lock(this->mutex);
		auto it = this->callbacks.find(key);
		if (it == this->callbacks.end()) {
			if (data) {
				delete data;
			}
			DEBUG_LOG("%p EventEmitter::notify key has no listeners:", this);
			DEBUG_LOG_KEY_LN(key);
			return false;
		}

		acquiredTsfns.reserve(it->second.size());
		for (auto& listener : it->second) {
			napi_threadsafe_function tsfn = listener->threadsafeCallback.load();
			if (!tsfn) {
				continue;
			}
			napi_status status = ::napi_acquire_threadsafe_function(tsfn);
			if (status == napi_ok) {
				acquiredTsfns.push_back(tsfn);
			} else {
				DEBUG_LOG("%p EventEmitter::notify acquire failed (status=%d), skipping listener\n", this, status);
			}
		}
	}

	DEBUG_LOG("%p EventEmitter::notify calling %zu listener%s for key:",
		this, acquiredTsfns.size(), acquiredTsfns.size() == 1 ? "" : "s");
	DEBUG_LOG_KEY_LN(key);

	for (napi_threadsafe_function tsfn : acquiredTsfns) {
		// create a separate copy of data for each listener to avoid double-delete
		ListenerData* listenerData = data ? new ListenerData(*data) : nullptr;

		napi_status status = ::napi_call_threadsafe_function(tsfn, listenerData, napi_tsfn_blocking);
		if (status != napi_ok) {
			DEBUG_LOG("%p EventEmitter::notify failed to call threadsafeCallback (status=%d)\n", this, status);
			if (listenerData) {
				delete listenerData;
			}
		} else {
			DEBUG_LOG("%p EventEmitter::notify called threadsafeCallback for key successfully!", this);
			DEBUG_LOG_KEY_LN(key);
		}

		// Pair with the acquire above. When this is the last reference, the
		// tsfn's finalize callback runs (on the owning env's JS thread) and
		// deletes the napi_ref.
		napi_status releaseStatus = ::napi_release_threadsafe_function(tsfn, napi_tsfn_release);
		if (releaseStatus != napi_ok) {
			DEBUG_LOG("%p EventEmitter::notify failed to release acquired tsfn (status=%d)\n", this, releaseStatus);
		}
	}

	if (data) {
		delete data;
	}

	DEBUG_LOG("%p EventEmitter::notify finished calling %zu listener%s for key:", this, acquiredTsfns.size(), acquiredTsfns.size() == 1 ? "" : "s");
	DEBUG_LOG_KEY_LN(key);

	return true;
}

napi_value EventEmitter::listeners(napi_env env, const std::string& key) {
	size_t count = 0;
	{
		std::lock_guard<std::mutex> lock(this->mutex);
		auto it = this->callbacks.find(key);
		if (it != this->callbacks.end()) {
			count = it->second.size();
		}
	}

	DEBUG_LOG("%p EventEmitter::listeners key has %zu listener%s:", this, count, count == 1 ? "" : "s");
	DEBUG_LOG_KEY_LN(key);

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_uint32(env, static_cast<uint32_t>(count), &result));
	return result;
}

napi_value EventEmitter::removeListener(napi_env env, const std::string& key, napi_value callback) {
	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, callback, &type));
	if (type != napi_function) {
		::napi_throw_error(env, nullptr, "Callback must be a function");
		return nullptr;
	}

	bool found = false;
	std::lock_guard<std::mutex> lock(this->mutex);
	auto it = this->callbacks.find(key);

	if (it != this->callbacks.end()) {
		for (auto listener = it->second.begin(); listener != it->second.end();) {
			if (env != (*listener)->env) {
				++listener;
				continue;
			}

			napi_value fn;
			NAPI_STATUS_THROWS(::napi_get_reference_value((*listener)->env, (*listener)->callbackRef, &fn));
			bool isEqual = false;
			NAPI_STATUS_THROWS(::napi_strict_equals(env, fn, callback, &isEqual));
			if (isEqual) {
				releaseListenerResources(**listener);

				listener = it->second.erase(listener);
				DEBUG_LOG("%p EventEmitter::removeListener removed listener for key:", this);
				DEBUG_LOG_KEY(key);
				DEBUG_LOG_MSG(" (listeners=%zu)\n", it->second.size());
				found = true;
				break;
			}

			++listener;
		}

		if (it->second.empty()) {
			DEBUG_LOG("%p EventEmitter::removeListener all listeners removed and removing key:", this);
			DEBUG_LOG_KEY_LN(key);
			this->callbacks.erase(it);
		}
	} else {
		DEBUG_LOG("%p EventEmitter::removeListener no listeners found for key:", this);
		DEBUG_LOG_KEY_LN(key);
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_get_boolean(env, found, &result));
	return result;
}

void EventEmitter::removeListenersByOwner(void* owner) {
	std::lock_guard<std::mutex> lock(this->mutex);

	DEBUG_LOG("%p EventEmitter::removeListenersByOwner removing listeners for owner %p\n", this, owner);

	for (auto keyIt = this->callbacks.begin(); keyIt != this->callbacks.end();) {
		auto& listeners = keyIt->second;

		// Two-phase: release the matching listeners' threadsafe-fn first (which
		// schedules napi_ref deletion via the tsfn finalizer on the JS thread),
		// then erase the entries. Dropping the shared_ptr alone would not release
		// the tsfn because ListenerCallback has no destructor; the explicit
		// release is what guarantees we don't leak.
		//
		// Listeners that were registered without an owner (hasOwner == false,
		// e.g., global emitter clients) are skipped — `weak_ptr<void>::expired`
		// returns true for an empty weak_ptr, so without this check the lambda
		// below would reap every ownerless listener on any owner-scoped call.
		for (auto& callback : listeners) {
			if (!callback->hasOwner) {
				continue;
			}
			auto sharedOwner = callback->owner.lock();
			bool shouldRemove = (sharedOwner.get() == owner) || callback->owner.expired();
			if (shouldRemove) {
				DEBUG_LOG("%p EventEmitter::removeListenersByOwner releasing listener for owner %p\n", this, owner);
				releaseListenerResources(*callback);
			}
		}

		listeners.erase(
			std::remove_if(listeners.begin(), listeners.end(),
				[](const std::shared_ptr<ListenerCallback>& callback) {
					// teardown above zeroed `threadsafeCallback` for matching
					// listeners; `callbackRef` is owned by the tsfn finalizer
					// at this point, so the tsfn pointer is the reliable marker.
					return callback->hasOwner && callback->threadsafeCallback.load() == nullptr;
				}),
			listeners.end()
		);

		if (listeners.empty()) {
			DEBUG_LOG("%p EventEmitter::removeListenersByOwner removing empty key\n", this);
			keyIt = this->callbacks.erase(keyIt);
		} else {
			++keyIt;
		}
	}
}

void EventEmitter::removeListenersByEnv(napi_env env) {
	std::lock_guard<std::mutex> lock(this->mutex);

	DEBUG_LOG("%p EventEmitter::removeListenersByEnv removing listeners for env %p\n", this, env);

	for (auto keyIt = this->callbacks.begin(); keyIt != this->callbacks.end();) {
		auto& listeners = keyIt->second;

		// Release matching listeners' tsfns first (queues napi_ref deletion
		// via the tsfn finalizer on the env's JS thread), then erase. The
		// erase predicate keys on env + null tsfn so we only remove the
		// entries we just released.
		for (auto& callback : listeners) {
			if (callback->env == env) {
				DEBUG_LOG("%p EventEmitter::removeListenersByEnv releasing listener for env %p\n", this, env);
				releaseListenerResources(*callback);
			}
		}

		listeners.erase(
			std::remove_if(listeners.begin(), listeners.end(),
				[env](const std::shared_ptr<ListenerCallback>& callback) {
					return callback->env == env && callback->threadsafeCallback.load() == nullptr;
				}),
			listeners.end()
		);

		if (listeners.empty()) {
			DEBUG_LOG("%p EventEmitter::removeListenersByEnv removing empty key\n", this);
			keyIt = this->callbacks.erase(keyIt);
		} else {
			++keyIt;
		}
	}
}

void EventEmitter::releaseAll() {
	std::lock_guard<std::mutex> lock(this->mutex);

	DEBUG_LOG("%p EventEmitter::releaseAll releasing %zu key(s)\n", this, this->callbacks.size());

	for (auto& [key, listeners] : this->callbacks) {
		for (auto& listener : listeners) {
			releaseListenerResources(*listener);
		}
	}

	this->callbacks.clear();
}

size_t EventEmitter::size() const {
	std::lock_guard<std::mutex> lock(this->mutex);
	return this->callbacks.size();
}

} // namespace rocksdb_js
