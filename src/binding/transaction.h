#ifndef __TRANSACTION_H__
#define __TRANSACTION_H__

#include "db_handle.h"
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
 *
 * This handle contains `get()`, `put()`, and `remove()` methods which are
 * shared between the `Database` and `Transaction` classes.
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
		this->id = this->txn->GetId() & 0xffffffff;
	}

	~TransactionHandle() {
		this->release();
	}

	/**
	 * Get a value using the specified database handle.
	 */
	rocksdb::Status get(
		std::string& key,
		std::string& result,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr
	) {
		auto readOptions = rocksdb::ReadOptions();
		readOptions.snapshot = this->txn->GetSnapshot();

		std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
		auto column = dbHandle->column.get();

		// TODO: should this be GetForUpdate?
		return this->txn->Get(readOptions, column, rocksdb::Slice(key), &result);
	}

	/**
	 * Put a value using the specified database handle.
	 */
	rocksdb::Status put(
		std::string& key,
		std::string& value,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr
	) {
		std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
		auto column = dbHandle->column.get();
		return this->txn->Put(column, rocksdb::Slice(key), rocksdb::Slice(value));
	}

	/**
	 * Remove a value using the specified database handle.
	 */
	rocksdb::Status remove(
		std::string& key,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr
	) {
		std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
		auto column = dbHandle->column.get();
		return this->txn->Delete(column, rocksdb::Slice(key));
	}

	/**
	 * Release the transaction. This is called after successful commit, after
	 * the transaction has been aborted, or when the transaction is destroyed.
	 */
	void release() {
		if (this->txn) {
			this->txn->ClearSnapshot();
			delete this->txn;
			this->txn = nullptr;
		}
	}

	std::shared_ptr<DBHandle> dbHandle;
	rocksdb::Transaction* txn;
	std::mutex commitMutex;
	uint32_t id;
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
	static napi_value Id(napi_env env, napi_callback_info info);
	static napi_value Put(napi_env env, napi_callback_info info);
	static napi_value Remove(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);

	static napi_ref constructor;
};

} // namespace rocksdb_js

#endif