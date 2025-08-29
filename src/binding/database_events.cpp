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
	return (*dbHandle)->descriptor->notify(env, key, argv[1]);
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
