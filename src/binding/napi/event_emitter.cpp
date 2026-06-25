#include "napi/event_emitter.h"
#include <algorithm>
#include <chrono>
#include <thread>
#include "core/debug.h"
#include "core/test_seam.h"
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
	// Reserve first so the push_back below cannot throw (shared_ptr copy is
	// noexcept): if growth fails it does so here, before the counter moves, so
	// the increment and the publish stay all-or-nothing even under OOM.
	it->second.reserve(it->second.size() + 1);
	// Bump the gate before publishing the listener into the map. A lock-free
	// reader in notify() that observes a nonzero count falls through to the
	// locked path and blocks behind this critical section, so it sees the new
	// listener; a reader that still observes zero cannot yet see the push_back
	// either, so dropping is correct. Incrementing after push_back would open a
	// window where the listener is visible but the count reads zero, letting the
	// fast path skip a deliverable listener.
	this->listenerCount.fetch_add(1, std::memory_order_relaxed);
	it->second.push_back(listenerCallback);

	DEBUG_LOG("%p EventEmitter::addListener added listener for key:", this);
	DEBUG_LOG_KEY(key);
	DEBUG_LOG_MSG(" (listeners=%zu)\n", it->second.size());

	return callbackRef;
}

bool EventEmitter::notify(const std::string& key, ListenerData* data) {
	// Lock-free fast path: when no listeners are registered anywhere, skip the
	// mutex and the key hash entirely. This keeps normally-silent emit sites
	// (e.g. native transaction-log warnings) cheap on the common no-listener
	// path. A concurrent add that has not yet incremented the count is the same
	// add-vs-emit race that exists without this check.
	if (this->listenerCount.load(std::memory_order_relaxed) == 0) {
		if (data) {
			delete data;
		}
		DEBUG_LOG("%p EventEmitter::notify no listeners (fast path) for key:", this);
		DEBUG_LOG_KEY_LN(key);
		return false;
	}

	// Call every listener's tsfn while holding the mutex. Keeping the calls
	// inside the locked region (rather than snapshotting under the lock and
	// calling after unlock) is what makes notify safe against env teardown:
	// every tsfn release path — removeListener, removeListenersByOwner,
	// releaseAll, and crucially removeListenersByEnv, which a dying worker env
	// runs from its cleanup hook — also takes this mutex. While we hold it,
	// none of them can run, so a listener's creation reference keeps its tsfn
	// alive, and a dying worker env cannot finish tearing down (its cleanup
	// hook blocks here) and let Node free its tsfns until we return. A snapshot
	// + napi_acquire_threadsafe_function before unlocking does NOT close this:
	// the acquire bumps a tsfn-level count that env teardown does not honor, so
	// a worker env could still free the tsfn out from under the post-unlock call
	// (HarperFast/harper#1370).
	//
	// Holding the mutex across the calls cannot deadlock or block unduly: the
	// call is made in napi_tsfn_nonblocking mode, so napi_call_threadsafe_function
	// only enqueues (the JS trampoline runs later on the owning env's thread,
	// never re-entering this mutex) and never blocks. With the unbounded queue
	// (max_queue_size 0) nonblocking is equivalent to blocking on Node, but
	// blocking mode could stall under this mutex on a runtime whose N-API shim
	// treats the queue as bounded or drains it differently during env teardown;
	// nonblocking removes that hazard (a full queue returns napi_queue_full,
	// handled below, instead of blocking the lock holder).
	//
	// This relies on Node running an env's cleanup hooks before freeing that
	// env's tsfns — the same ordering removeListenersByEnv already depends on.
	bool found = false;
	{
		std::lock_guard<std::mutex> lock(this->mutex);
		auto it = this->callbacks.find(key);
		if (it != this->callbacks.end()) {
			found = true;
			// Deterministic test seam (inert unless ROCKSDB_JS_NOTIFY_DELAY_MS is
			// set): widens the window where this->mutex is held across the calls
			// below, so a test can force the worker-env-teardown vs notify race
			// that otherwise only surfaces at production scale (HarperFast/harper#1370).
			// Holding the mutex here is exactly what makes the call safe — a
			// concurrent removeListenersByEnv blocks on the mutex until we return,
			// so the tsfn cannot be freed mid-call. Read once; a single cached-int
			// branch per notify when unset.
			const int delayMs = testDelayMs("ROCKSDB_JS_NOTIFY_DELAY_MS");
			if (delayMs > 0) {
				std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
			}
			DEBUG_LOG("%p EventEmitter::notify calling %zu listener%s for key:",
				this, it->second.size(), it->second.size() == 1 ? "" : "s");
			DEBUG_LOG_KEY_LN(key);

			for (auto& listener : it->second) {
				napi_threadsafe_function tsfn = listener->threadsafeCallback.load();
				if (!tsfn) {
					continue;
				}

				// a separate copy of data per listener avoids a double-delete: the
				// tsfn trampoline (callListenerCallback) deletes the copy it receives.
				ListenerData* listenerData = data ? new ListenerData(*data) : nullptr;

				napi_status status = ::napi_call_threadsafe_function(tsfn, listenerData, napi_tsfn_nonblocking);
				if (status != napi_ok) {
					// e.g. napi_closing once the owning env starts tearing down (or
					// napi_queue_full, which the unbounded queue never returns); that
					// listener will be scrubbed by removeListenersByEnv. Not a crash:
					// we hold the mutex, so the tsfn is still allocated, just closing.
					DEBUG_LOG("%p EventEmitter::notify failed to call threadsafeCallback (status=%d)\n", this, status);
					if (listenerData) {
						delete listenerData;
					}
				} else {
					DEBUG_LOG("%p EventEmitter::notify called threadsafeCallback for key successfully!", this);
					DEBUG_LOG_KEY_LN(key);
				}
			}
		} else {
			DEBUG_LOG("%p EventEmitter::notify key has no listeners:", this);
			DEBUG_LOG_KEY_LN(key);
		}
	}

	if (data) {
		delete data;
	}

	DEBUG_LOG("%p EventEmitter::notify finished for key:", this);
	DEBUG_LOG_KEY_LN(key);

	return found;
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
				this->listenerCount.fetch_sub(1, std::memory_order_relaxed);
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

		size_t before = listeners.size();
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
		if (size_t removed = before - listeners.size()) {
			this->listenerCount.fetch_sub(removed, std::memory_order_relaxed);
		}

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

		size_t before = listeners.size();
		listeners.erase(
			std::remove_if(listeners.begin(), listeners.end(),
				[env](const std::shared_ptr<ListenerCallback>& callback) {
					return callback->env == env && callback->threadsafeCallback.load() == nullptr;
				}),
			listeners.end()
		);
		if (size_t removed = before - listeners.size()) {
			this->listenerCount.fetch_sub(removed, std::memory_order_relaxed);
		}

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
	this->listenerCount.store(0, std::memory_order_relaxed);
}

size_t EventEmitter::size() const {
	std::lock_guard<std::mutex> lock(this->mutex);
	return this->callbacks.size();
}

} // namespace rocksdb_js
