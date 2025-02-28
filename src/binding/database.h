#ifndef __DATABASE_H__
#define __DATABASE_H__

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include <node_api.h>

namespace rocksdb_js {

// class Store final {
// public:
// 	Store();
// 	~Store();

// private:
// 	rocksdb::TransactionDB* db;
// };

class Database final {
public:
	static napi_ref constructor_ref;
	static void Init(napi_env env, napi_value exports);
	static napi_value Close(napi_env env, napi_callback_info info);
	static napi_value Get(napi_env env, napi_callback_info info);
	static napi_value IsOpen(napi_env env, napi_callback_info info);
	static napi_value Open(napi_env env, napi_callback_info info);
	static napi_value Put(napi_env env, napi_callback_info info);

	Database(const std::string& path);
	~Database();

private:
	rocksdb::TransactionDB* db;
	std::string path;
	// static std::map<std::string, Store*> stores;
};

} // namespace rocksdb_js

#endif