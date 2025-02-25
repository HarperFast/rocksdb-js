#ifndef __DATABASE_H__
#define __DATABASE_H__

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include <node_api.h>

namespace rocksdb_js {

class Database final {
public:
	static void Init(napi_env env, napi_value exports);
	static napi_value New(napi_env env, napi_callback_info info);
	static napi_value Get(napi_env env, napi_callback_info info);
	static napi_value IsOpen(napi_env env, napi_callback_info info);
	static napi_value Open(napi_env env, napi_callback_info info);
	static napi_value Put(napi_env env, napi_callback_info info);
	static napi_value Close(napi_env env, napi_callback_info info);

	Database(const std::string& path);
	~Database();

private:
	rocksdb::TransactionDB* db;
	std::string path;
};

} // namespace rocksdb_js

#endif