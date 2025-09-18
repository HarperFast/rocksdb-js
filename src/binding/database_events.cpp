#include "database.h"
#include "db_descriptor.h"

namespace rocksdb_js {

/**
 * Adds a listener.
 *
 * @example
 * ```ts
 * const db = new NativeDatabase();
 * db.addEventListener('foo', () => {
 *   console.log('foo');
 * });
 *
 * db.notify('foo');
 * ```
 */
napi_value Database::AddListener(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	NAPI_GET_STRING(argv[0], key, "Event is required")
	UNWRAP_DB_HANDLE_AND_OPEN()
	(*dbHandle)->addListener(env, key, argv[1]);
	NAPI_RETURN_UNDEFINED()
}

/**
 * Calls all listeners for a given key.
 *
 * @example
 * ```ts
 * const db = new NativeDatabase();
 * db.addEventListener('foo', () => {
 *   console.log('foo');
 * });
 *
 * db.notify('foo'); // returns `true` if there were listeners
 * db.notify('bar'); // returns `false` if there were no listeners
 * ```
 */
napi_value Database::Notify(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	NAPI_GET_STRING(argv[0], key, "Event is required")
	UNWRAP_DB_HANDLE_AND_OPEN()
	napi_value result;
	DEBUG_LOG("Database::Notify calling notify env=%p args=%p\n", env, argv[1]);
	ListenerData* data = nullptr;

	// need to serialize the args to a string
	bool isArray = false;
	NAPI_STATUS_THROWS(::napi_is_array(env, argv[1], &isArray))
	if (isArray) {
		uint32_t argc = 0;
		NAPI_STATUS_THROWS(::napi_get_array_length(env, argv[1], &argc))
		if (argc > 0) {
			napi_value global;
			napi_value json;
			napi_value stringify;
			napi_value jsonString;
			size_t len;
			NAPI_STATUS_THROWS(::napi_get_global(env, &global));
			NAPI_STATUS_THROWS(::napi_get_named_property(env, global, "JSON", &json));
			NAPI_STATUS_THROWS(::napi_get_named_property(env, json, "stringify", &stringify));
			NAPI_STATUS_THROWS(::napi_call_function(env, json, stringify, 1, &argv[1], &jsonString));
			NAPI_STATUS_THROWS(::napi_get_value_string_utf8(env, jsonString, nullptr, 0, &len));
			data = new ListenerData(len);
			NAPI_STATUS_THROWS(::napi_get_value_string_utf8(env, jsonString, &data->args[0], len + 1, nullptr));
		}
	}

	bool notified = (*dbHandle)->descriptor->notify(key, data);
	NAPI_STATUS_THROWS(::napi_get_boolean(env, notified, &result));
	return result;
}

/**
 * Gets the number of listeners for the given key.
 *
 * @example
 * ```ts
 * const db = new NativeDatabase();
 * db.listeners('foo'); // returns 0
 *
 * db.addEventListener('foo', () => {});
 * db.listeners('foo'); // returns 1
 *
 * db.addEventListener('foo', () => {});
 * db.listeners('foo'); // returns 2
 * ```
 */
napi_value Database::Listeners(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	NAPI_GET_STRING(argv[0], key, "Event is required")
	UNWRAP_DB_HANDLE_AND_OPEN()
	return (*dbHandle)->descriptor->listeners(env, key);
}

/**
 * Removes a listener.
 *
 * @example
 * ```ts
 * const db = new NativeDatabase();
 * db.addEventListener('foo', () => {});
 * db.removeEventListener('foo', () => {});
 * db.listeners('foo'); // returns 0
 * ```
 */
napi_value Database::RemoveListener(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	NAPI_GET_STRING(argv[0], key, "Event is required")
	UNWRAP_DB_HANDLE_AND_OPEN()
	return (*dbHandle)->descriptor->removeListener(env, key, argv[1]);
}

} // namespace rocksdb_js
