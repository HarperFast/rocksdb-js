#include "binding.h"
#include "database.h"
#include "macros.h"
#include "registry.h"
#include "rocksdb/db.h"
#include "util.h"

namespace rocksdb_js {

napi_value openDB(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	return Registry::getDatabaseStore(env, argv, argc);
}

NAPI_MODULE_INIT() {
	napi_value version;
	napi_create_string_utf8(env, rocksdb::GetRocksVersionAsString().c_str(), NAPI_AUTO_LENGTH, &version);
	napi_set_named_property(env, exports, "version", version);

	NAPI_EXPORT_FUNCTION(openDB)

	// registry
	NAPI_STATUS_THROWS(::napi_add_env_cleanup_hook(env, [](void* data) {
		rocksdb_js::Registry* registry = (rocksdb_js::Registry*)data;
		registry->cleanup();
	}, rocksdb_js::Registry::getInstance()));

	// database
	Database::Init(env, exports);

	return exports;
}

} // namespace rocksdb_js
