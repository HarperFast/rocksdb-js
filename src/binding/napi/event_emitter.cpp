#include "napi/event_emitter.h"
#include <algorithm>
#include "core/debug.h"
#include "napi/helpers.h"
#include "napi/macros.h"

namespace rocksdb_js {

/**
 * Releases the threadsafe function and callback ref held by a listener.
 *
 * Must be called on the JS thread, because napi_delete_reference is only
 * legal from the env's JS thread. `napi_release_threadsafe_function` is
 * thread-safe and decrements the tsfn ref count; the actual finalize runs
 * on the JS thread once the count reaches zero.
 */
static void releaseListenerResources(ListenerCallback& listener) {
	if (listener.threadsafeCallback) {
		napi_status status = ::napi_release_threadsafe_function(listener.threadsafeCallback, napi_tsfn_release);
		listener.threadsafeCallback = nullptr;
		if (status != napi_ok) {
			DEBUG_LOG("releaseListenerResources failed to release threadsafe callback (status=%d)\n", status);
		}
	}

	if (listener.callbackRef) {
		napi_status status = ::napi_delete_reference(listener.env, listener.callbackRef);
		listener.callbackRef = nullptr;
		if (status != napi_ok) {
			DEBUG_LOG("releaseListenerResources failed to delete callback reference (status=%d)\n", status);
		}
	}
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

	napi_value resource_name;
	NAPI_STATUS_THROWS(::napi_create_string_latin1(
		env,
		"rocksdb-js.listener",
		NAPI_AUTO_LENGTH,
		&resource_name
	));

	napi_ref callbackRef;
	NAPI_STATUS_THROWS(::napi_create_reference(env, callback, 1, &callbackRef));

	auto listenerCallback = std::make_shared<ListenerCallback>(env, callbackRef, std::move(owner));

	napi_status status = ::napi_create_threadsafe_function(
		env,                    // env
		callback,               // func
		nullptr,                // async_resource
		resource_name,          // async_resource_name
		0,                      // max_queue_size
		1,                      // initial_thread_count
		nullptr,                // thread_finalize_data
		nullptr,                // thread_finalize_callback
		nullptr,                // context
		callListenerCallback,   // call_js_cb
		&listenerCallback->threadsafeCallback // [out] callback
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

	NAPI_STATUS_THROWS(::napi_unref_threadsafe_function(env, listenerCallback->threadsafeCallback));

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
	// copy the listeners to avoid holding the mutex during callback execution
	std::vector<std::shared_ptr<ListenerCallback>> listenersToCall;
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

		listenersToCall.reserve(it->second.size());
		for (auto& listener : it->second) {
			listenersToCall.push_back(listener);
		}
	}

	DEBUG_LOG("%p EventEmitter::notify calling %zu listener%s for key:",
		this, listenersToCall.size(), listenersToCall.size() == 1 ? "" : "s");
	DEBUG_LOG_KEY_LN(key);

	for (auto& listener : listenersToCall) {
		// create a separate copy of data for each listener to avoid double-delete
		ListenerData* listenerData = data ? new ListenerData(*data) : nullptr;
		if (listener->threadsafeCallback) {
			DEBUG_LOG("%p EventEmitter::notify calling threadsafeCallback for key:", this);
			DEBUG_LOG_KEY_LN(key);

			napi_status status = ::napi_call_threadsafe_function(listener->threadsafeCallback, listenerData, napi_tsfn_blocking);
			if (status != napi_ok) {
				DEBUG_LOG("%p EventEmitter::notify failed to call threadsafeCallback (status=%d)\n", this, status);
				if (listenerData) {
					delete listenerData;
				}
			} else {
				DEBUG_LOG("%p EventEmitter::notify called threadsafeCallback for key successfully!", this);
				DEBUG_LOG_KEY_LN(key);
			}
		} else {
			DEBUG_LOG("%p EventEmitter::notify threadsafeCallback is null for key:", this);
			DEBUG_LOG_KEY_LN(key);
			if (listenerData) {
				delete listenerData;
			}
		}
	}

	if (data) {
		delete data;
	}

	DEBUG_LOG("%p EventEmitter::notify finished calling %zu listener%s for key:", this, listenersToCall.size(), listenersToCall.size() == 1 ? "" : "s");
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

		// Two-phase: release threadsafe-fn / napi_ref for matching listeners
		// first, then erase. The teardown must run on the JS thread (caller
		// guarantees this — typically DBHandle::close from a NAPI callback);
		// dropping the shared_ptr alone would leak both resources because
		// ListenerCallback has no destructor that releases them.
		for (auto& callback : listeners) {
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
					// teardown above zeroed both fields for matching listeners
					return callback->threadsafeCallback == nullptr && callback->callbackRef == nullptr;
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
