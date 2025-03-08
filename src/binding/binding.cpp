#include "binding.h"
#include "db_wrap.h"
#include "macros.h"
#include "registry.h"
#include "rocksdb/db.h"
#include "util.h"

namespace rocksdb_js {

NAPI_MODULE_INIT() {
	napi_value version;
	napi_create_string_utf8(env, rocksdb::GetRocksVersionAsString().c_str(), NAPI_AUTO_LENGTH, &version);
	napi_set_named_property(env, exports, "version", version);

	// registry
	NAPI_STATUS_THROWS(::napi_add_env_cleanup_hook(env, [](void* data) {
		rocksdb_js::Registry* registry = (rocksdb_js::Registry*)data;
		registry->cleanup();
	}, rocksdb_js::Registry::getInstance()));

	// database
	rocksdb_js::db_wrap::init(env, exports);

	return exports;
}

} // namespace rocksdb_js
