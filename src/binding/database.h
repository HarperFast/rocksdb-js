#ifndef __DATABASE_H__
#define __DATABASE_H__

#include "rocksdb/db.h"
#include <node_api.h>

namespace rocksdb_js {

class Database final {
public:
	static void Init(napi_env env, napi_value exports);
	static napi_value New(napi_env env, napi_callback_info info);
	static napi_value Get(napi_env env, napi_callback_info info);
	static napi_value Open(napi_env env, napi_callback_info info);
	static napi_value Put(napi_env env, napi_callback_info info);
	static napi_value Close(napi_env env, napi_callback_info info);

	Database();
	~Database();

private:
	rocksdb::DB* db;
};

} // namespace rocksdb_js

#endif