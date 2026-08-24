#ifndef __NAPI_BACKGROUND_ERROR_H__
#define __NAPI_BACKGROUND_ERROR_H__

#include <string>
#include "napi/binding.h"

namespace rocksdb_js {

/**
 * Per-env addon data (stored via `napi_set_instance_data`). Holds a reference
 * to the JS `BackgroundError` constructor so any code with just an `napi_env`
 * — a `Database` method or the generic event-emitter trampoline — can build an
 * instance without threading the module `exports` through. One slot per env;
 * this is the module's addon data (add fields here if future classes need the
 * same env-scoped access).
 *
 * Threading/env contract: instance data is per-`napi_env`, NOT shared across
 * envs — each env (main thread + each worker_thread) runs `Init` and stores its
 * OWN `napi_ref` here, so no napi handle ever crosses an env boundary. All
 * access (set/get, and the ref it holds) must be on that env's JS thread. Set
 * exactly once per env in `Init`; a second `napi_set_instance_data` would
 * replace the slot and leak this struct.
 */
struct AddonData {
	napi_ref backgroundErrorCtor = nullptr;
};

/**
 * Retrieves the per-env `AddonData`, or `nullptr` if it has not been installed
 * on this env (e.g. a context that never ran `BackgroundError::Init`).
 */
AddonData* getAddonData(napi_env env);

/**
 * The JS `BackgroundError` class — a real `Error` subclass surfaced to consumers
 * as the payload of the per-database `'error'` event and the return value of
 * `db.getLastError()` (HarperFast/rocksdb-js#730). Defined natively so both the
 * push (event) and pull (`getLastError`) paths can reconstruct one identical
 * instance from the JSON a background error was serialized to (see
 * `backgroundErrorToJson`), keeping the `DBDescriptor` env-free and thread-safe.
 */
class BackgroundError {
public:
	/**
	 * Defines the class on `exports.BackgroundError`, wires its prototype chain to
	 * `Error`, and stashes the constructor in the env's `AddonData`. Call once per
	 * env from the module init.
	 */
	static void Init(napi_env env, napi_value exports);

	/** N-API constructor: `new BackgroundError(details)` copies `details`' fields. */
	static napi_value Constructor(napi_env env, napi_callback_info info);

	/**
	 * Builds a `BackgroundError` from its JSON string form: `JSON.parse`s `json`
	 * and calls the stashed constructor. MUST be called on `env`'s JS thread — it
	 * reads this env's instance data and constructs napi values (callers: the
	 * `getLastError` method and the event-emitter tsfn trampoline, both JS-thread;
	 * the RocksDB background thread only ever writes the JSON string, never calls
	 * this). Returns `nullptr` and leaves a pending exception on failure (missing
	 * addon data / malformed JSON).
	 */
	static napi_value New(napi_env env, const std::string& json);
};

} // namespace rocksdb_js

#endif
