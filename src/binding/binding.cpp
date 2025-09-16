#include "binding.h"
#include "database.h"
#include "db_iterator.h"
#include "db_registry.h"
#include "db_settings.h"
#include "macros.h"
#include "rocksdb/db.h"
#include "transaction.h"
#include "transaction_log.h"
#include "util.h"
#include <atomic>

namespace rocksdb_js {

/**
 * The number of active `rocksdb-js` modules.
 *
 * There can be multiple instances of this module in the same Node.js process
 * (main thread + worker threads) and we only want to cleanup after the last
 * instance exits.
 */
static std::atomic<int> moduleRefCount{0};

NAPI_MODULE_INIT() {
#ifdef DEBUG
	// disable buffering for stderr to ensure messages are written immediately
	::setvbuf(stderr, nullptr, _IONBF, 0);
#endif

	napi_value version;
	napi_create_string_utf8(env, rocksdb::GetRocksVersionAsString().c_str(), NAPI_AUTO_LENGTH, &version);
	napi_set_named_property(env, exports, "version", version);

	[[maybe_unused]] int refCount = ++moduleRefCount;
	DEBUG_LOG("Binding::Init Module ref count: %d\n", refCount);

	// registry cleanup
	NAPI_STATUS_THROWS(::napi_add_env_cleanup_hook(env, [](void* data) {
		DEBUG_LOG("Binding::Init env cleanup start\n");

		int newCount = --moduleRefCount;
		DEBUG_LOG("Binding::Init Module ref count after cleanup: %d\n", newCount);

		if (newCount == 0) {
			DEBUG_LOG("Binding::Init Last module cleaned up, purging all databases\n");
			rocksdb_js::DBRegistry::PurgeAll();
		} else if (newCount < 0) {
			DEBUG_LOG("Binding::Init WARNING: Module ref count went negative!\n");
		}

		DEBUG_LOG("Binding::Init env cleanup done\n");
	}, nullptr));

	// database
	rocksdb_js::Database::Init(env, exports);

	// transaction
	rocksdb_js::Transaction::Init(env, exports);

	// transaction log
	rocksdb_js::TransactionLog::Init(env, exports);

	// db iterator
	rocksdb_js::DBIterator::Init(env, exports);

	// db settings
	rocksdb_js::DBSettings::Init(env, exports);

	return exports;
}

} // namespace rocksdb_js
