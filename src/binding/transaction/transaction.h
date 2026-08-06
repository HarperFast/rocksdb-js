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
	static napi_value AbandonWrites(napi_env env, napi_callback_info info);
	static napi_value Abort(napi_env env, napi_callback_info info);
	static napi_value Commit(napi_env env, napi_callback_info info);
	static napi_value CommitSync(napi_env env, napi_callback_info info);
	static napi_value Get(napi_env env, napi_callback_info info);
	static napi_value GetCount(napi_env env, napi_callback_info info);
	static napi_value GetSync(napi_env env, napi_callback_info info);
	static napi_value GetTimestamp(napi_env env, napi_callback_info info);
	static napi_value Id(napi_env env, napi_callback_info info);
	static napi_value PutSync(napi_env env, napi_callback_info info);
	static napi_value RemoveSync(napi_env env, napi_callback_info info);
	static napi_value SetTimestamp(napi_env env, napi_callback_info info);
	static napi_value UseLog(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);

	/**
	 * Invalidates any coordinated-retry parked wake-callback TSFNs still
	 * outstanding for `env` (see completeCommitWork's IsBusy+coordinatedRetry
	 * park loop in transaction.cpp). Call from the module's per-env cleanup
	 * hook, mirroring DBRegistry::ReleaseCommitCompletionsByEnv -- a parked
	 * callback is stored on the process-global LockTracker it's waiting on,
	 * so it can fire long after (and on a different thread than) the env
	 * that created it, once that env has already torn down and Node has
	 * reclaimed the tsfn (HarperFast/rocksdb-js#741).
	 */
	static void ReleaseParkedFlagsByEnv(napi_env env);
};

} // namespace rocksdb_js

#endif