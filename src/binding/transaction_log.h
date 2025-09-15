#ifndef __TRANSACTION_LOG_H__
#define __TRANSACTION_LOG_H__

#include <node_api.h>

namespace rocksdb_js {

/**
 * The `NativeTransactionLog` JavaScript class implementation.
 *
 * @example
 * ```js
 * const db = new binding.NativeDatabase();
 * db.open('/tmp/testdb');
 * const txn = new binding.NativeTransactionLog(db);
 * txn.put('foo', 'bar');
 * txn.commit();
 * ```
 */
struct TransactionLog final {
	static napi_value Constructor(napi_env env, napi_callback_info info);
	static napi_value Commit(napi_env env, napi_callback_info info);
	static napi_value GetRange(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);
};

} // namespace rocksdb_js

#endif