#ifndef __TRANSACTION_H__
#define __TRANSACTION_H__

#include <node_api.h>

namespace rocksdb_js {

/**
 * A simple wrapper around the DBHandle and TransactionHandle to pass into the
 * Transaction JS constructor so it can be cleaned up when the Transaction JS
 * object is garbage collected.
 */
struct DBTxnHandle final : Closable {
	DBTxnHandle(std::shared_ptr<DBHandle> dbHandle)
		: dbDescriptor(dbHandle->descriptor)
	{
		this->txnHandle = std::make_shared<TransactionHandle>(dbHandle);
		this->dbDescriptor->transactionAdd(this->txnHandle);
	}

	~DBTxnHandle() {
		this->close();
	}

	void close() {
		if (this->txnHandle) {
			this->dbDescriptor->transactionRemove(this->txnHandle);
			this->txnHandle->close();
			this->txnHandle.reset();
		}
	}

	std::shared_ptr<DBDescriptor> dbDescriptor;
	std::shared_ptr<TransactionHandle> txnHandle;
};

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
	static napi_value GetSync(napi_env env, napi_callback_info info);
	static napi_value Id(napi_env env, napi_callback_info info);
	static napi_value PutSync(napi_env env, napi_callback_info info);
	static napi_value RemoveSync(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);
};

} // namespace rocksdb_js

#endif