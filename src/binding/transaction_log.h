#ifndef __TRANSACTION_LOG_H__
#define __TRANSACTION_LOG_H__

#include <node_api.h>

namespace rocksdb_js {

/**
 * The `NativeTransactionLog` JavaScript class implementation.
 */
struct TransactionLog final {
	static napi_value Constructor(napi_env env, napi_callback_info info);
	static napi_value GetMemoryMapOfFile(napi_env env, napi_callback_info info);
	static napi_value GetSequencedLogs(napi_env env, napi_callback_info info);
	static napi_value AddEntry(napi_env env, napi_callback_info info);
	static napi_value AddEntryCopy(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);
};

} // namespace rocksdb_js

#endif