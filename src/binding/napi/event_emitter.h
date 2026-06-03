#ifndef __NAPI_EVENT_EMITTER_H__
#define __NAPI_EVENT_EMITTER_H__

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "napi/binding.h"

namespace rocksdb_js {

/**
 * A struct to hold the serialized arguments to emit to listener callbacks.
 */
struct ListenerData final {
	std::string args;

	ListenerData(size_t size) : args(size, '\0') {}
	ListenerData(const ListenerData& other) : args(other.args) {}
};

/**
 * A wrapper for a listener callback that holds the threadsafe callback, env,
 * callback reference, and an optional owner. The callback reference is used to
 * locate and remove the listener; the env is used for cleanup; the owner is
 * used by EventEmitter::removeListenersByOwner to bulk-remove listeners that
 * belong to a specific object (e.g., a DBHandle being closed).
 *
 * Owner is type-erased as std::weak_ptr<void> so the same EventEmitter can be
 * used both with per-object owners and with no owner at all (global emitter).
 */
struct ListenerCallback final {
	napi_env env;
	napi_threadsafe_function threadsafeCallback;
	napi_ref callbackRef;
	std::weak_ptr<void> owner;

	ListenerCallback(napi_env env, napi_ref callbackRef, std::weak_ptr<void> owner)
		: env(env), threadsafeCallback(nullptr), callbackRef(callbackRef), owner(std::move(owner)) {}
};

/**
 * Serializes a JS value (expected to be an array) into a ListenerData via
 * JSON.stringify. Returns nullptr when `value` is not an array, is an empty
 * array, or a NAPI call fails (in which case a JS exception is left pending
 * and the caller's next NAPI call will propagate it).
 *
 * Used by the JS-facing `notify` entry points to package call arguments into
 * a thread-safe payload.
 */
ListenerData* serializeListenerArgs(napi_env env, napi_value value);

/**
 * A reusable, thread-safe event emitter that dispatches to JavaScript listener
 * callbacks via napi_threadsafe_function. Used by both per-database
 * (DBDescriptor) and process-global (GlobalEvents) event surfaces.
 */
class EventEmitter final {
public:
	/**
	 * Registers a JS callback for the given event key.
	 *
	 * @param env The current N-API environment.
	 * @param key The event key.
	 * @param callback The JS function to invoke when the event is emitted.
	 * @param owner Optional weak reference to the owning object. Defaults to
	 *              an empty weak_ptr for ownerless listeners (global emitter).
	 * @returns The napi_ref backing the callback, suitable for later removal.
	 */
	napi_ref addListener(
		napi_env env,
		const std::string& key,
		napi_value callback,
		std::weak_ptr<void> owner = {}
	);

	/**
	 * Calls all registered listeners for the given key. Safe to call from any
	 * thread; dispatch happens through napi_threadsafe_function.
	 *
	 * Takes ownership of `data` (deleted before return). Each listener receives
	 * an independent copy.
	 *
	 * @returns true if there was at least one listener.
	 */
	bool notify(const std::string& key, ListenerData* data);

	/**
	 * Returns the number of listeners for `key` as a napi uint32.
	 */
	napi_value listeners(napi_env env, const std::string& key);

	/**
	 * Removes a specific listener (matched by JS function strict-equality) for
	 * the given key. Returns a napi boolean indicating whether a listener was
	 * found and removed.
	 */
	napi_value removeListener(napi_env env, const std::string& key, napi_value callback);

	/**
	 * Removes all listeners owned by the given raw pointer. The pointer must
	 * match the address that was held by the listener's owning shared_ptr at
	 * the time the listener was added. Also purges any listeners whose owner
	 * weak_ptr has expired.
	 */
	void removeListenersByOwner(void* owner);

	/**
	 * Releases all listeners' threadsafe functions and callback refs and
	 * clears the map. Must be called on the JS thread (callback refs cannot
	 * be deleted from worker threads).
	 */
	void releaseAll();

	/**
	 * Returns the number of distinct event keys with listeners. Used by
	 * registry status reporting.
	 */
	size_t size() const;

private:
	std::unordered_map<std::string, std::vector<std::shared_ptr<ListenerCallback>>> callbacks;
	mutable std::mutex mutex;
};

} // namespace rocksdb_js

#endif
