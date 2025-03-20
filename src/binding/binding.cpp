#include "binding.h"
#include "db_registry.h"
#include "db_wrap.h"
#include "macros.h"
#include "rocksdb/db.h"
#include "transaction.h"
#include "util.h"

namespace rocksdb_js {

NAPI_MODULE_INIT() {
	napi_value version;
	napi_create_string_utf8(env, rocksdb::GetRocksVersionAsString().c_str(), NAPI_AUTO_LENGTH, &version);
	napi_set_named_property(env, exports, "version", version);

	// registry
	NAPI_STATUS_THROWS(::napi_add_env_cleanup_hook(env, [](void* data) {
		rocksdb_js::DBRegistry* registry = (rocksdb_js::DBRegistry*)data;
		registry->cleanup();
	}, rocksdb_js::DBRegistry::getInstance()));

	// database
	rocksdb_js::db_wrap::init(env, exports);

	// transaction
	rocksdb_js::Transaction::Init(env, exports);

	return exports;
}

} // namespace rocksdb_js
