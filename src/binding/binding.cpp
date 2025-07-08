#include "binding.h"
#include "database.h"
#include "db_iterator.h"
#include "db_registry.h"
#include "db_settings.h"
#include "macros.h"
#include "rocksdb/db.h"
#include "transaction.h"
#include "util.h"

namespace rocksdb_js {

NAPI_MODULE_INIT() {
#ifdef DEBUG
	// disable buffering for stderr to ensure messages are written immediately
	setvbuf(stderr, nullptr, _IONBF, 0);
#endif

	napi_value version;
	napi_create_string_utf8(env, rocksdb::GetRocksVersionAsString().c_str(), NAPI_AUTO_LENGTH, &version);
	napi_set_named_property(env, exports, "version", version);

	// registry cleanup
	NAPI_STATUS_THROWS(::napi_add_env_cleanup_hook(env, [](void* data) {
		rocksdb_js::DBRegistry::Purge();
	}, nullptr));

	// database
	rocksdb_js::Database::Init(env, exports);

	// transaction
	rocksdb_js::Transaction::Init(env, exports);

	// db iterator
	rocksdb_js::DBIterator::Init(env, exports);

	// db settings
	rocksdb_js::DBSettings::Init(env, exports);

	return exports;
}

} // namespace rocksdb_js
