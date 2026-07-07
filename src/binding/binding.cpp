#include "napi/binding.h"
#include "database/backup.h"
#include "database/database.h"
#include "iterator/db_iterator.h"
#include "iterator/db_iterator_handle.h"
#include "database/db_registry.h"
#include "database/db_settings.h"
#include "napi/global_events.h"
#include "napi/macros.h"
#include "rocksdb/db.h"
#include "rocksdb/statistics.h"
#include "transaction/transaction.h"
#include "transaction_log/transaction_log.h"
#include "transaction_log/transaction_log_file.h"
#include "transaction_log/transaction_log_store_registry.h"
#include "core/platform.h"
#include "core/file_lock.h"
#include "napi/helpers.h"
#include "napi/async.h"
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
	GlobalEvents::Shutdown();
	DBRegistry::Shutdown();
	napi_value result;
	NAPI_STATUS_THROWS(::napi_get_undefined(env, &result));
	return result;
}

/**
 * Returns the current thread id.
 */
napi_value CurrentThreadId(napi_env env, napi_callback_info info) {
	napi_value result;
	auto threadId = getThreadId();
	NAPI_STATUS_THROWS(::napi_create_int64(env, threadId, &result));
	return result;
}

/**
 * Takes a file lock by opening and exclusively locking the given file (see
 * `tryAcquireFileLock`). Returns an opaque non-zero token to pass to
 * `fileLockRelease`, or `0` if another holder currently has the lock. Throws on
 * a hard error.
 */
napi_value TryFileLock(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	NAPI_GET_STRING(argv[0], file, "Expected file path");

	try {
		uint32_t token = tryAcquireFileLock(file);
		napi_value result;
		NAPI_STATUS_THROWS(::napi_create_uint32(env, token, &result));
		return result;
	} catch (const std::exception& e) {
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}
}

/**
 * Releases a file lock previously returned by `tryFileLock`
 * by closing its handle. A no-op for token `0` or an unknown token.
 */
napi_value FileLockRelease(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	uint32_t token;
	NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[0], &token));
	releaseFileLock(token);
	NAPI_RETURN_UNDEFINED();
}

/**
 * Advises the kernel that the file-backed pages of every mapped transaction log
 * are cold (MADV_COLD), so they are reclaimed first under memory pressure. Meant
 * to be called periodically from a single host-driven timer (see
 * TransactionLogStoreRegistry::CoolTransactionLogs). Returns { maps, bytes }.
 */
napi_value CoolTransactionLogs(napi_env env, napi_callback_info info) {
	TransactionLogCoolResult cooled = TransactionLogStoreRegistry::CoolTransactionLogs();

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_object(env, &result));

	napi_value maps;
	NAPI_STATUS_THROWS(::napi_create_uint32(env, cooled.maps, &maps));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "maps", maps));

	napi_value bytes;
	NAPI_STATUS_THROWS(::napi_create_int64(env, static_cast<int64_t>(cooled.bytes), &bytes));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "bytes", bytes));

	return result;
}

/**
 * Returns the number of live transaction-log memory maps across the process.
 * Used by tests to verify that releasing a (frozen) log's external buffer
 * actually unmaps the mapping rather than leaving it retained.
 */
napi_value TransactionLogMapCount(napi_env env, napi_callback_info info) {
	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_int64(env, MemoryMap::liveCount.load(std::memory_order_relaxed), &result));
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

	// initialize the registries
	rocksdb_js::DBRegistry::Init(env, exports);
	rocksdb_js::TransactionLogStoreRegistry::Init();

	// registry cleanup
	// The C++ statics in this .node binary are shared across every Node env
	// that loads it (main thread + worker_threads). Each env's cleanup hook
	// fires when *that* env is being torn down — not when the whole process
	// exits. We need to drop the dying env's listeners from every emitter
	// that outlives the env — the global singleton and every per-DBDescriptor
	// emitter in the DBRegistry (DBDescriptors are shared across envs that
	// open the same path, so a worker's listeners can sit on a descriptor the
	// main thread still notifies on). A surviving env's notify() would
	// otherwise dereference dangling tsfn pointers. Full Shutdown only runs
	// when this was the last env.
	NAPI_STATUS_THROWS(::napi_add_env_cleanup_hook(env, [](void* data) {
		napi_env dyingEnv = static_cast<napi_env>(data);
		rocksdb_js::GlobalEvents::getInstance().removeListenersByEnv(dyingEnv);
		rocksdb_js::DBRegistry::RemoveListenersByEnv(dyingEnv);

		int32_t newRefCount = --moduleRefCount;
		if (newRefCount == 0) {
			DEBUG_LOG("Binding::Init Cleaning up last instance, shutting down all databases\n");
			rocksdb_js::GlobalEvents::Shutdown();
			rocksdb_js::TransactionLogStoreRegistry::Shutdown();
			rocksdb_js::DBRegistry::Shutdown();
			DEBUG_LOG("Binding::Init env cleanup done\n");
		} else if (newRefCount < 0) {
			DEBUG_LOG("Binding::Init WARNING: Module ref count went negative!\n");
		} else {
			DEBUG_LOG("Binding::Init Skipping cleanup, %d remaining instances\n", newRefCount);
		}
	}, env));

	// database
	rocksdb_js::Database::Init(env, exports);

	// backup management functions (restore/list/delete/purge/verify)
	rocksdb_js::initBackupExports(env, exports);

	// transaction
	rocksdb_js::Transaction::Init(env, exports);

	// transaction log
	rocksdb_js::TransactionLog::Init(env, exports);

	// db iterator
	rocksdb_js::DBIterator::Init(env, exports);

	// db settings
	rocksdb_js::DBSettings::Init(env, exports);

	// global event emitter (addListener / removeListener / listenerCount)
	rocksdb_js::GlobalEvents::Init(env, exports);

	// shutdown function
	napi_value shutdownFn;
	NAPI_STATUS_THROWS(::napi_create_function(env, "shutdown", NAPI_AUTO_LENGTH, Shutdown, nullptr, &shutdownFn));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, exports, "shutdown", shutdownFn));

	// currentThreadId function
	napi_value currentThreadIdFn;
	NAPI_STATUS_THROWS(::napi_create_function(env, "currentThreadId", NAPI_AUTO_LENGTH, CurrentThreadId, nullptr, &currentThreadIdFn));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, exports, "currentThreadId", currentThreadIdFn));

	// file lock functions (see src/backup.ts)
	napi_value tryFileLockFn;
	NAPI_STATUS_THROWS(::napi_create_function(env, "tryFileLock", NAPI_AUTO_LENGTH, TryFileLock, nullptr, &tryFileLockFn));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, exports, "tryFileLock", tryFileLockFn));

	napi_value fileLockReleaseFn;
	NAPI_STATUS_THROWS(::napi_create_function(env, "fileLockRelease", NAPI_AUTO_LENGTH, FileLockRelease, nullptr, &fileLockReleaseFn));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, exports, "fileLockRelease", fileLockReleaseFn));

	// coolTransactionLogs function
	napi_value coolTransactionLogsFn;
	NAPI_STATUS_THROWS(::napi_create_function(env, "coolTransactionLogs", NAPI_AUTO_LENGTH, CoolTransactionLogs, nullptr, &coolTransactionLogsFn));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, exports, "coolTransactionLogs", coolTransactionLogsFn));

	// transactionLogMapCount function (test/diagnostics)
	napi_value transactionLogMapCountFn;
	NAPI_STATUS_THROWS(::napi_create_function(env, "transactionLogMapCount", NAPI_AUTO_LENGTH, TransactionLogMapCount, nullptr, &transactionLogMapCountFn));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, exports, "transactionLogMapCount", transactionLogMapCountFn));

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
	EXPORT_CONSTANT(constants, POPULATE_VERSION_FLAG)
	EXPORT_CONSTANT(constants, FRESH_VERSION_FLAG)
	EXPORT_CONSTANT(constants, RETRY_NOW_VALUE)
	EXPORT_CONSTANT(constants, ITERATOR_REVERSE_FLAG)
	EXPORT_CONSTANT(constants, ITERATOR_INCLUSIVE_END_FLAG)
	EXPORT_CONSTANT(constants, ITERATOR_EXCLUSIVE_START_FLAG)
	EXPORT_CONSTANT(constants, ITERATOR_INCLUDE_VALUES_FLAG)
	EXPORT_CONSTANT(constants, ITERATOR_NEEDS_STABLE_VALUE_BUFFER_FLAG)
	EXPORT_CONSTANT(constants, ITERATOR_CONTEXT_IS_TRANSACTION_FLAG)
	EXPORT_CONSTANT(constants, ITERATOR_RESULT_DONE)
	EXPORT_CONSTANT(constants, ITERATOR_RESULT_FAST)
	NAPI_STATUS_THROWS(::napi_set_named_property(env, exports, "constants", constants));

	// stats
	napi_value statsObj;
	napi_create_object(env, &statsObj);

	// stats level
	napi_value statsLevel;
	napi_create_object(env, &statsLevel);
	EXPORT_STATS_LEVEL(statsLevel, DisableAll, rocksdb::StatsLevel::kDisableAll)
	EXPORT_STATS_LEVEL(statsLevel, ExceptTickers, rocksdb::StatsLevel::kExceptTickers)
	EXPORT_STATS_LEVEL(statsLevel, ExceptHistogramOrTimers, rocksdb::StatsLevel::kExceptHistogramOrTimers)
	EXPORT_STATS_LEVEL(statsLevel, ExceptTimers, rocksdb::StatsLevel::kExceptTimers)
	EXPORT_STATS_LEVEL(statsLevel, ExceptDetailedTimers, rocksdb::StatsLevel::kExceptDetailedTimers)
	EXPORT_STATS_LEVEL(statsLevel, ExceptTimeForMutex, rocksdb::StatsLevel::kExceptTimeForMutex)
	EXPORT_STATS_LEVEL(statsLevel, All, rocksdb::StatsLevel::kAll)
	NAPI_STATUS_THROWS(::napi_set_named_property(env, statsObj, "StatsLevel", statsLevel));

	// Stat names are documented in docs/stats.md rather than enumerated here.
	NAPI_STATUS_THROWS(::napi_set_named_property(env, exports, "stats", statsObj));

	return exports;
}

} // namespace rocksdb_js
