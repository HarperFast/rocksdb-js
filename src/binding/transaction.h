#ifndef __TRANSACTION_H__
#define __TRANSACTION_H__

#include <node_api.h>

namespace rocksdb_js {

/**
 * The `NativeTransaction` JavaScript class implementation.
 * 
 * @example
 * ```js
 * const db = new binding.NativeDatabase();
 * db.open('/tmp/testdb');
 * const txn = new binding.NativeTransaction(db);
 * txn.put('foo', 'bar');
 * txn.commit();
 * ```
 */
struct Transaction final {
	static napi_value Constructor(napi_env env, napi_callback_info info);
	static napi_value Abort(napi_env env, napi_callback_info info);
	static napi_value Commit(napi_env env, napi_callback_info info);
	static napi_value CommitSync(napi_env env, napi_callback_info info);
	static napi_value Get(napi_env env, napi_callback_info info);
	static napi_value Id(napi_env env, napi_callback_info info);
	static napi_value Put(napi_env env, napi_callback_info info);
	static napi_value Remove(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);

	static napi_ref constructor;
};

} // namespace rocksdb_js

#endif