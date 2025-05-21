#ifndef __DB_ITERATOR_H__
#define __DB_ITERATOR_H__

#include <memory>
#include <node_api.h>
#include "db_handle.h"
#include "rocksdb/iterator.h"

namespace rocksdb_js {

/**
 * The `NativeCursor` JavaScript class implementation.
 * 
 * @example
 * ```js
 * const db = new binding.NativeDatabase();
 * db.open('/tmp/testdb');
 * const iter = db.getRange();
 * for await (const [key, value] of iter) {
 *   console.log(key, value);
 * }
 * ```
 */
struct DBIterator final {
	static napi_value Constructor(napi_env env, napi_callback_info info);
	static napi_value Next(napi_env env, napi_callback_info info);
	static napi_value Return(napi_env env, napi_callback_info info);
	static napi_value Throw(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);
};

} // namespace rocksdb_js

#endif
