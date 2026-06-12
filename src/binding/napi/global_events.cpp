#include "napi/global_events.h"
#include "core/debug.h"
#include "napi/helpers.h"
#include "napi/macros.h"

namespace rocksdb_js {

/**
 * JS surface: `binding.addListener(eventName, callback)`.
 *
 * Exposed on `RocksDatabase` as the static `RocksDatabase.on(...)`.
 */
static napi_value AddListener(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2);
	NAPI_GET_STRING(argv[0], key, "Event is required");
	if (argc < 2) {
		::napi_throw_error(env, nullptr, "Callback is required");
		return nullptr;
	}
	GlobalEvents::getInstance().addListener(env, key, argv[1]);
	NAPI_RETURN_UNDEFINED();
}

/**
 * JS surface: `binding.removeListener(eventName, callback)`.
 *
 * Exposed on `RocksDatabase` as the static `RocksDatabase.off(...)` /
 * `RocksDatabase.removeListener(...)`.
 */
static napi_value RemoveListener(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2);
	NAPI_GET_STRING(argv[0], key, "Event is required");
	if (argc < 2) {
		::napi_throw_error(env, nullptr, "Callback is required");
		return nullptr;
	}
	return GlobalEvents::getInstance().removeListener(env, key, argv[1]);
}

/**
 * JS surface: `binding.listenerCount(eventName)`.
 *
 * Exposed on `RocksDatabase` as the static `RocksDatabase.listenerCount(...)`.
 */
static napi_value ListenerCount(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	NAPI_GET_STRING(argv[0], key, "Event is required");
	return GlobalEvents::getInstance().listeners(env, key);
}

/**
 * JS surface: `binding.notify(eventName, args)`.
 *
 * Exposed on `RocksDatabase` as the static `RocksDatabase.notify(event, ...args)`.
 * The TS wrapper splats rest args into the array that this function expects.
 *
 * Mirrors the per-database `Database::Notify` shape so the C++ serialization
 * path is shared via `serializeListenerArgs`.
 */
static napi_value Notify(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2);
	NAPI_GET_STRING(argv[0], key, "Event is required");

	ListenerData* data = argc > 1 ? serializeListenerArgs(env, argv[1]) : nullptr;
	bool exceptionPending = false;
	NAPI_STATUS_THROWS(::napi_is_exception_pending(env, &exceptionPending));
	if (exceptionPending) {
		return nullptr;
	}

	bool notified = GlobalEvents::getInstance().notify(key, data);
	napi_value result;
	NAPI_STATUS_THROWS(::napi_get_boolean(env, notified, &result));
	return result;
}

void GlobalEvents::Init(napi_env env, napi_value exports) {
	napi_value fn;

	NAPI_STATUS_THROWS_VOID(::napi_create_function(env, "addListener", NAPI_AUTO_LENGTH, AddListener, nullptr, &fn));
	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, "addListener", fn));

	NAPI_STATUS_THROWS_VOID(::napi_create_function(env, "removeListener", NAPI_AUTO_LENGTH, RemoveListener, nullptr, &fn));
	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, "removeListener", fn));

	NAPI_STATUS_THROWS_VOID(::napi_create_function(env, "listenerCount", NAPI_AUTO_LENGTH, ListenerCount, nullptr, &fn));
	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, "listenerCount", fn));

	NAPI_STATUS_THROWS_VOID(::napi_create_function(env, "notify", NAPI_AUTO_LENGTH, Notify, nullptr, &fn));
	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, "notify", fn));
}

void GlobalEvents::Shutdown() {
	DEBUG_LOG("GlobalEvents::Shutdown releasing all global listeners\n");
	GlobalEvents::getInstance().releaseAll();
}

} // namespace rocksdb_js
