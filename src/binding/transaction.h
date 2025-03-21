#ifndef __TRANSACTION_H__
#define __TRANSACTION_H__

#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include <node_api.h>
#include <memory>

namespace rocksdb_js {

class TransactionHandle final {
public:
	TransactionHandle(std::shared_ptr<DBHandle> dbHandle) :
		dbHandle(dbHandle),
		txn(nullptr)
	{
		rocksdb::TransactionOptions txnOptions;
		this->txn = this->dbHandle->db->BeginTransaction(rocksdb::WriteOptions(), txnOptions);
		this->txn->SetSnapshot();
	}

	~TransactionHandle() {
		this->release();
	}

	void release() {
		if (this->txn) {
			fprintf(stderr, "releasing transaction snapshot\n");
			auto snapshot = this->txn->GetSnapshot();
			if (snapshot) {
				this->dbHandle->db->ReleaseSnapshot(snapshot);
			}
			this->txn->ClearSnapshot();
			fprintf(stderr, "released transaction snapshot\n");
			delete this->txn;
			this->txn = nullptr;
		}
	}

	std::shared_ptr<DBHandle> dbHandle;
	rocksdb::Transaction* txn;
};

class Transaction final {
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