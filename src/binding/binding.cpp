#include "binding.h"
#include "rocksdb/db.h"
#include "database.h"

using namespace Napi;

// Replace NAPI_MODULE_INIT() with NODE_API_MODULE
static Napi::Object Init(Napi::Env env, Napi::Object exports) {
	// version
	exports.Set("version", String::New(env, rocksdb::GetRocksVersionAsString()));

	// database
	rocksdb_js::Database::Init(env, exports);

	return exports;
}

NODE_API_MODULE(NODE_GYP_MODULE_NAME, Init)
