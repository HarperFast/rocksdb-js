#ifndef __NAPI_GLOBAL_EVENTS_H__
#define __NAPI_GLOBAL_EVENTS_H__

#include "napi/event_emitter.h"

namespace rocksdb_js {

/**
 * Process-wide event emitter. Exposed to JS as static methods on the
 * `RocksDatabase` class (`on`, `off`, `listenerCount`) and used internally by
 * native code that needs to surface warnings or notifications to JS without
 * a database context — e.g., warnings from the transaction log layer that
 * lives below `DBDescriptor`.
 *
 * Use namespaced event keys for internal events (`'transactionLog:warning'`,
 * etc.) so they don't collide with user-defined events.
 */
class GlobalEvents final {
public:
	/**
	 * Returns the process-wide singleton.
	 *
	 * Uses C++11 magic-static initialization, mirroring `DBSettings::getInstance`,
	 * so concurrent first-callers from worker threads don't race during
	 * construction.
	 */
	static EventEmitter& getInstance() {
		static EventEmitter instance;
		return instance;
	}

	GlobalEvents() = delete;

	/**
	 * Registers the JS-facing surface (`addListener`, `removeListener`,
	 * `listenerCount`) onto the binding exports object. Called once from
	 * `NAPI_MODULE_INIT`.
	 */
	static void Init(napi_env env, napi_value exports);

	/**
	 * Releases all global listeners. Called from the module env-cleanup hook
	 * so threadsafe functions don't outlive their N-API environment.
	 */
	static void Shutdown();
};

/**
 * Inline helper for emitting a global event from native code. Returns `true`
 * if there was at least one listener.
 *
 * Takes ownership of `data` (it is freed by `notify`, even when no listeners
 * are registered).
 */
inline bool emitGlobalEvent(const std::string& key, ListenerData* data = nullptr) {
	return GlobalEvents::getInstance().notify(key, data);
}

} // namespace rocksdb_js

#endif
