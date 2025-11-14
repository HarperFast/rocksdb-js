#include "binding.h"
#include "database.h"
#include "db_iterator.h"
#include "db_registry.h"
#include "db_settings.h"
#include "macros.h"
#include "rocksdb/db.h"
#include "transaction.h"
#include "transaction_log.h"
#include "transaction_log_file.h"
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
static std::atomic<uint32_t> moduleRefCount{0};

NAPI_MODULE_INIT() {
#ifdef DEBUG
	// disable buffering for stderr to ensure messages are written immediately
	::setvbuf(stderr, nullptr, _IONBF, 0);
#endif

	napi_value version;
	napi_create_string_utf8(env, rocksdb::GetRocksVersionAsString().c_str(), NAPI_AUTO_LENGTH, &version);
	napi_set_named_property(env, exports, "version", version);

	[[maybe_unused]] uint32_t refCount = ++moduleRefCount;
	DEBUG_LOG("Binding::Init Module ref count: %u\n", refCount);

	// initialize the registry
	DBRegistry::Init();

	// registry cleanup
	NAPI_STATUS_THROWS(::napi_add_env_cleanup_hook(env, [](void* data) {
		uint32_t newRefCount = --moduleRefCount;
		if (newRefCount == 0) {
			DEBUG_LOG("Binding::Init Cleaning up last instance, purging all databases\n")
			rocksdb_js::DBRegistry::PurgeAll();
			DEBUG_LOG("Binding::Init env cleanup done\n")
		} else if (newRefCount < 0) {
			DEBUG_LOG("Binding::Init WARNING: Module ref count went negative!\n")
		} else {
			DEBUG_LOG("Binding::Init Skipping cleanup, %u remaining instances\n", newRefCount)
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

	// constants
	napi_value constants;
	napi_create_object(env, &constants);

	napi_value woofToken, blockSize, fileHeaderSize, blockHeaderSize, txnHeaderSize, continuationFlag;
	napi_create_uint32(env, WOOF_TOKEN, &woofToken);
	napi_create_uint32(env, BLOCK_SIZE, &blockSize);
	napi_create_uint32(env, FILE_HEADER_SIZE, &fileHeaderSize);
	napi_create_uint32(env, BLOCK_HEADER_SIZE, &blockHeaderSize);
	napi_create_uint32(env, TXN_HEADER_SIZE, &txnHeaderSize);
	napi_create_uint32(env, CONTINUATION_FLAG, &continuationFlag);

	napi_set_named_property(env, constants, "WOOF_TOKEN", woofToken);
	napi_set_named_property(env, constants, "BLOCK_SIZE", blockSize);
	napi_set_named_property(env, constants, "FILE_HEADER_SIZE", fileHeaderSize);
	napi_set_named_property(env, constants, "BLOCK_HEADER_SIZE", blockHeaderSize);
	napi_set_named_property(env, constants, "TXN_HEADER_SIZE", txnHeaderSize);
	napi_set_named_property(env, constants, "CONTINUATION_FLAG", continuationFlag);

	napi_set_named_property(env, exports, "constants", constants);

	return exports;
}

} // namespace rocksdb_js
