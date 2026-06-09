#ifndef __NAPI_EVENT_EMITTER_H__
#define __NAPI_EVENT_EMITTER_H__

#include <atomic>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include "core/json.h"
#include "napi/binding.h"

namespace rocksdb_js {

/**
 * A struct to hold the serialized arguments to emit to listener callbacks.
 */
struct ListenerData final {
	std::string args;

	ListenerData() = default;
	ListenerData(size_t size) : args(size, '\0') {}
	ListenerData(const ListenerData& other) : args(other.args) {}

	/**
	 * Builds a ListenerData payload from one or more pre-stringified args,
	 * JSON-escaped and wrapped in a JSON array (the shape the JS listener
	 * trampoline expects). Use this from native call sites that need to
	 * surface text — typically a single message — to a JS listener.
	 *
	 * Each arg is encoded as a JSON string via `appendJsonString` so paths
	 * containing `"`, `\`, or control characters round-trip safely.
	 *
	 * Defined inline so files that need only this factory (e.g. recovery
	 * code in the native GoogleTest binary, which doesn't link the rest of
	 * the N-API event-emitter machinery) get the implementation without
	 * pulling in event_emitter.cpp.
	 *
	 * Returns a heap-allocated ListenerData; ownership transfers to the
	 * recipient (`EventEmitter::notify` / `emitGlobalEvent`).
	 */
	static ListenerData* fromStrings(std::initializer_list<std::string_view> args) {
		std::string payload;
		size_t reserve = 2; // '[' + ']'
		for (std::string_view arg : args) {
			reserve += arg.size() + 3; // quotes + comma; escapes may grow this
		}
		payload.reserve(reserve);

		payload += '[';
		bool first = true;
		for (std::string_view arg : args) {
			if (!first) payload += ',';
			first = false;
			appendJsonString(payload, arg);
		}
		payload += ']';

		auto* data = new ListenerData();
		data->args = std::move(payload);
		return data;
	}
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
 * The `hasOwner` flag distinguishes "listener was registered without an owner"
 * (global emitter case) from "owner was non-null at register time but has
 * since expired" — only the latter should be reaped by removeListenersByOwner.
 */
struct ListenerCallback final {
	/**
	 * The environment of the current callback.
	 */
	napi_env env;

	/**
	 * The threadsafe function of the current callback. This is what is actually
	 * called when the event is emitted.
	 *
	 * Atomic because `notify` reads it without holding EventEmitter::mutex
	 * (it copies the listener vector under the lock, then iterates and reads
	 * the tsfn pointer after releasing the lock), while `releaseListenerResources`
	 * can run concurrently on another thread and write nullptr. Plain access
	 * would be a data race under the C++ memory model.
	 */
	std::atomic<napi_threadsafe_function> threadsafeCallback;

	/**
	 * The callback reference of the current callback. This is used to remove
	 * the listener callback.
	 */
	napi_ref callbackRef;

	/**
	 * The DBHandle that owns this listener (weak reference to avoid cycles).
	 */
	std::weak_ptr<void> owner;

	/**
	 * Whether the listener has an owner. Process-wide global listeners have no owner.
	 */
	bool hasOwner;

	ListenerCallback(napi_env env, napi_ref callbackRef, std::weak_ptr<void> owner, bool hasOwner)
		: env(env),
		  threadsafeCallback(nullptr),
		  callbackRef(callbackRef),
		  owner(std::move(owner)),
		  hasOwner(hasOwner) {}
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
	EventEmitter() = default;
	// Runs releaseAll so that paths which destroy an EventEmitter without
	// an explicit close (partial construction, exceptions thrown between
	// open() and close()) still clean up tsfns. With the deferred-finalizer
	// design, releaseAll is safe to call from any thread.
	~EventEmitter() { releaseAll(); }

	// EventEmitter holds OS resources (tsfns, mutex); not copyable or movable.
	EventEmitter(const EventEmitter&) = delete;
	EventEmitter& operator=(const EventEmitter&) = delete;
	EventEmitter(EventEmitter&&) = delete;
	EventEmitter& operator=(EventEmitter&&) = delete;

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
	 * Removes all listeners registered against the given napi_env, releasing
	 * their threadsafe functions. Called from the env-cleanup hook on every
	 * emitter that outlives an individual env — the process-wide GlobalEvents
	 * singleton and every per-DBDescriptor emitter in the DBRegistry (both
	 * are shared across envs loaded into the same native binary). When one
	 * env is torn down — e.g. a `worker_threads` worker exiting while the
	 * main thread continues running — this ensures no dangling tsfn pointers
	 * remain that a later notify from a surviving thread would dereference.
	 *
	 * Safe to call from any thread: napi_release_threadsafe_function is
	 * thread-safe, and the napi_ref deletion is deferred to the tsfn's
	 * finalize callback which Node-API runs on the JS thread.
	 */
	void removeListenersByEnv(napi_env env);

	/**
	 * Releases all listeners' threadsafe functions and clears the map. Safe
	 * to call from any thread: napi_release_threadsafe_function is
	 * thread-safe and the napi_ref is owned by the tsfn finalizer which
	 * runs on the JS thread.
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
