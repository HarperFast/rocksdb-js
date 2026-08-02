#ifndef __NAPI_ENV_TEARDOWN_H__
#define __NAPI_ENV_TEARDOWN_H__

namespace rocksdb_js {

/**
 * Tracks whether the calling thread is currently inside its env's
 * `napi_add_env_cleanup_hook` callback.
 *
 * Node runs `Realm::RunCleanup()` — which destroys the env's `BaseObject`s and
 * frees N-API-owned per-env state — *immediately before* draining the cleanup
 * queue that invokes our hook (`Environment::RunCleanup`, env.cc: `
 * principal_realm_->RunCleanup(); cleanup_queue_.Drain();`). So by the time our
 * hook runs, the env is on the right thread but is already partially freed, and
 * any N-API call that touches env state (even one as innocuous as
 * `napi_delete_reference`, which writes the env's `last_error` via
 * `napi_clear_last_error`) is a use-after-free.
 *
 * A "am I on the owning JS thread?" check is therefore NOT sufficient to decide
 * whether an N-API call is safe during teardown — teardown runs on exactly that
 * thread. Close paths reached from the cleanup hook must consult this instead.
 * Confirmed with ThreadSanitizer against a from-source TSan Node: three
 * heap-use-after-free writes in `napi_clear_last_error` reached from
 * `TransactionHandle::close()` -> `DBDescriptor::finishClose()` ->
 * `DBRegistry::Shutdown()` -> our cleanup hook (HarperFast/rocksdb-js#741).
 *
 * Thread-local rather than per-env: the hook always runs on its own env's
 * thread, and the only calls that matter are the ones made beneath it.
 */
inline bool& envTeardownFlag() {
	thread_local bool tearingDown = false;
	return tearingDown;
}

inline bool isEnvTearingDown() {
	return envTeardownFlag();
}

/** RAII marker for the duration of an env cleanup hook. */
struct EnvTeardownScope {
	EnvTeardownScope() {
		envTeardownFlag() = true;
	}
	~EnvTeardownScope() {
		envTeardownFlag() = false;
	}
	EnvTeardownScope(const EnvTeardownScope&) = delete;
	EnvTeardownScope& operator=(const EnvTeardownScope&) = delete;
};

} // namespace rocksdb_js

#endif
