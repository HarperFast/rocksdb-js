#ifndef __TRANSACTION_H__
#define __TRANSACTION_H__

#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include <node_api.h>
#include <memory>

namespace rocksdb_js {

/**
 * A handle to a RocksDB transaction. This is used to keep the transaction
 * alive until the transaction is committed or aborted.
 *
 * It also has a reference to the database handle so that the transaction knows
 * which column family to use.
 */
struct TransactionHandle final {
	TransactionHandle(std::shared_ptr<DBHandle> dbHandle) :
		dbHandle(dbHandle),
		txn(nullptr)
	{
		if (dbHandle->mode == DBMode::Pessimistic) {
			auto* tdb = static_cast<rocksdb::TransactionDB*>(dbHandle->db.get());
			rocksdb::TransactionOptions txnOptions;
			this->txn = tdb->BeginTransaction(rocksdb::WriteOptions(), txnOptions);
		} else if (dbHandle->mode == DBMode::Optimistic) {
			auto* odb = static_cast<rocksdb::OptimisticTransactionDB*>(dbHandle->db.get());
			rocksdb::OptimisticTransactionOptions txnOptions;
			this->txn = odb->BeginTransaction(rocksdb::WriteOptions(), txnOptions);
		} else {
			throw std::runtime_error("Invalid database");
		}
		this->txn->SetSnapshot();
	}

	~TransactionHandle() {
		this->release();
	}

	void release() {
		if (this->txn) {
			this->txn->ClearSnapshot();
			delete this->txn;
			this->txn = nullptr;
		}
	}

	std::shared_ptr<DBHandle> dbHandle;
	rocksdb::Transaction* txn;
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
	static napi_value Get(napi_env env, napi_callback_info info);
	static napi_value Put(napi_env env, napi_callback_info info);
	static napi_value Remove(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);

	static napi_ref constructor;
};

} // namespace rocksdb_js

#endif