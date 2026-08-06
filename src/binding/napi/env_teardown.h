#ifndef __NAPI_ENV_TEARDOWN_H__
#define __NAPI_ENV_TEARDOWN_H__

namespace rocksdb_js {

/**
 * True while the calling thread is inside its env's cleanup hook.
 *
 * Node runs `Realm::RunCleanup()` — which frees N-API per-env state — before
 * draining the cleanup queue that invokes the hook, so during teardown the env
 * is on the right thread but already partially freed: any N-API call that
 * touches env state is a use-after-free (even `napi_delete_reference`, which
 * writes `last_error`). A thread-identity check is therefore not sufficient to
 * decide whether an N-API call is safe; close paths reached from the cleanup
 * hook must consult this as well.
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
