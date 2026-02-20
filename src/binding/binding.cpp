#include "binding.h"
#include "database.h"
#include "db_iterator.h"
#include "db_registry.h"
#include "db_settings.h"
#include "macros.h"
#include "rocksdb/db.h"
#include "rocksdb/statistics.h"
#include "transaction.h"
#include "transaction_log.h"
#include "transaction_log_file.h"
#include "util.h"
#include <atomic>

namespace rocksdb_js {

#define EXPORT_CONSTANT(dest, constant) \
	napi_value constant##Value; \
	NAPI_STATUS_THROWS(::napi_create_uint32(env, constant, &constant##Value)); \
	NAPI_STATUS_THROWS(::napi_set_named_property(env, dest, #constant, constant##Value));

#define EXPORT_STATS_LEVEL(dest, key, value) \
	napi_value key##Value; \
	NAPI_STATUS_THROWS(::napi_create_uint32(env, value, &key##Value)); \
	NAPI_STATUS_THROWS(::napi_set_named_property(env, dest, #key, key##Value));

/**
 * Shutdown function to ensure that we write in-memory data from all databases.
 */
napi_value Shutdown(napi_env env, napi_callback_info info) {
	DBRegistry::Shutdown();
	napi_value result;
	NAPI_STATUS_THROWS(::napi_get_undefined(env, &result));
	return result;
}

/**
 * The number of active `rocksdb-js` modules.
 *
 * There can be multiple instances of this module in the same Node.js process
 * (main thread + worker threads) and we only want to cleanup after the last
 * instance exits.
 */
static std::atomic<int32_t> moduleRefCount{0};

NAPI_MODULE_INIT() {
#ifdef DEBUG
	// disable buffering for stderr to ensure messages are written immediately
	::setvbuf(stderr, nullptr, _IONBF, 0);
#endif

	napi_value version;
	napi_create_string_utf8(env, rocksdb::GetRocksVersionAsString().c_str(), NAPI_AUTO_LENGTH, &version);
	napi_set_named_property(env, exports, "version", version);

	[[maybe_unused]] int32_t refCount = ++moduleRefCount;
	DEBUG_LOG("Binding::Init Module ref count: %d\n", refCount);

	// initialize the registry
	rocksdb_js::DBRegistry::Init(env, exports);

	// registry cleanup
	NAPI_STATUS_THROWS(::napi_add_env_cleanup_hook(env, [](void* data) {
		int32_t newRefCount = --moduleRefCount;
		if (newRefCount == 0) {
			DEBUG_LOG("Binding::Init Cleaning up last instance, purging all databases\n");
			rocksdb_js::DBRegistry::PurgeAll();
			DEBUG_LOG("Binding::Init env cleanup done\n");
		} else if (newRefCount < 0) {
			DEBUG_LOG("Binding::Init WARNING: Module ref count went negative!\n");
		} else {
			DEBUG_LOG("Binding::Init Skipping cleanup, %d remaining instances\n", newRefCount);
		}
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

	// shutdown function
	napi_value shutdownFn;
	NAPI_STATUS_THROWS(::napi_create_function(env, "shutdown", NAPI_AUTO_LENGTH, Shutdown, nullptr, &shutdownFn));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, exports, "shutdown", shutdownFn));

	// constants
	napi_value constants;
	napi_create_object(env, &constants);
	EXPORT_CONSTANT(constants, TRANSACTION_LOG_TOKEN)
	EXPORT_CONSTANT(constants, TRANSACTION_LOG_FILE_HEADER_SIZE)
	EXPORT_CONSTANT(constants, TRANSACTION_LOG_ENTRY_HEADER_SIZE)
	EXPORT_CONSTANT(constants, TRANSACTION_LOG_ENTRY_LAST_FLAG)
	EXPORT_CONSTANT(constants, ONLY_IF_IN_MEMORY_CACHE_FLAG)
	EXPORT_CONSTANT(constants, NOT_IN_MEMORY_CACHE_FLAG)
	EXPORT_CONSTANT(constants, ALWAYS_CREATE_NEW_BUFFER_FLAG)
	NAPI_STATUS_THROWS(::napi_set_named_property(env, exports, "constants", constants));

	napi_value statsLevel;
	napi_create_object(env, &statsLevel);
	EXPORT_STATS_LEVEL(statsLevel, DisableAll, rocksdb::StatsLevel::kDisableAll)
	EXPORT_STATS_LEVEL(statsLevel, ExceptTickers, rocksdb::StatsLevel::kExceptTickers)
	EXPORT_STATS_LEVEL(statsLevel, ExceptHistogramOrTimers, rocksdb::StatsLevel::kExceptHistogramOrTimers)
	EXPORT_STATS_LEVEL(statsLevel, ExceptTimers, rocksdb::StatsLevel::kExceptTimers)
	EXPORT_STATS_LEVEL(statsLevel, ExceptDetailedTimers, rocksdb::StatsLevel::kExceptDetailedTimers)
	EXPORT_STATS_LEVEL(statsLevel, ExceptTimeForMutex, rocksdb::StatsLevel::kExceptTimeForMutex)
	EXPORT_STATS_LEVEL(statsLevel, All, rocksdb::StatsLevel::kAll)
	NAPI_STATUS_THROWS(::napi_set_named_property(env, exports, "StatsLevel", statsLevel));

	return exports;
}

} // namespace rocksdb_js
