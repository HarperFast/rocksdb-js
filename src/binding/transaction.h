#ifndef DB_TRANSACTION_H
#define DB_TRANSACTION_H

#include "rocksdb/utilities/transaction_db.h"
#include <node_api.h>

namespace rocksdb_js {

class TransactionHandle {
public:
	rocksdb::Transaction* txn;
};

class Transaction {
public:
	static napi_value Constructor(napi_env env, napi_callback_info info);
	static napi_value Abort(napi_env env, napi_callback_info info);
	static napi_value Commit(napi_env env, napi_callback_info info);
	static napi_value Get(napi_env env, napi_callback_info info);
	static napi_value Put(napi_env env, napi_callback_info info);
	static napi_value Remove(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);

	static napi_ref constructor;
};

} // namespace rocksdb_js

#endif