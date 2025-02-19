#include "binding.h"
#include "rocksdb/db.h"
#include "database.h"

NAPI_INIT() {
	napi_value version;
	napi_create_string_utf8(env, rocksdb::GetRocksVersionAsString().c_str(), NAPI_AUTO_LENGTH, &version);
	napi_set_named_property(env, exports, "version", version);

	// database
	rocksdb_js::Database::Init(env, exports);
}
