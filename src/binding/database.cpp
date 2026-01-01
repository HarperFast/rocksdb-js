#include <node_api.h>
#include <sstream>
#include "database.h"
#include "db_handle.h"
#include "db_iterator.h"
#include "db_iterator_handle.h"
#include "db_registry.h"
#include "macros.h"
#include "transaction.h"
#include "util.h"

namespace rocksdb_js {

/**
 * Creates a new `NativeDatabase` JavaScript object containing an database
 * handle to an unopened RocksDB database.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * ```
 */
napi_value Database::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR_WITH_DATA("Database")

	// create shared_ptr on heap so it persists after function returns
	napi_ref exportsRef = reinterpret_cast<napi_ref>(data);
	auto* dbHandle = new std::shared_ptr<DBHandle>(std::make_shared<DBHandle>(env, exportsRef));

	DEBUG_LOG("Database::Constructor Creating NativeDatabase DBHandle=%p\n", dbHandle->get())

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(dbHandle),
			[](napi_env env, void* data, void* hint) {
				DEBUG_LOG("Database::Constructor NativeDatabase GC'd dbHandle=%p\n", data)
				auto* dbHandle = static_cast<std::shared_ptr<DBHandle>*>(data);
				if (*dbHandle) {
					DBRegistry::CloseDB(*dbHandle);
				}
				delete dbHandle;
			},
			nullptr, // finalize_hint
			nullptr  // result
		));

		return jsThis;
	} catch (const std::exception& e) {
		delete dbHandle;
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}
}

/**
 * Removes all entries from the RocksDB database by delete the column family
 * files.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * await db.clear();
 * ```
 */
napi_value Database::Clear(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(3)
	UNWRAP_DB_HANDLE_AND_OPEN()

	napi_value resolve = argv[0];
	napi_value reject = argv[1];

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(
		env,
		"database.clear",
		NAPI_AUTO_LENGTH,
		&name
	))

	auto state = new AsyncClearState(env, *dbHandle);
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef))
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef))

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,       // node_env
		nullptr,   // async_resource
		name,      // async_resource_name
		[](napi_env doNotUse, void* data) { // execute
			auto state = reinterpret_cast<AsyncClearState*>(data);
			// check if database is still open before proceeding
			if (!state->handle || !state->handle->opened() || state->handle->isCancelled()) {
				state->status = rocksdb::Status::Aborted("Database closed during clear operation");
			} else {
				state->status = state->handle->clear();
			}
			// signal that execute handler is complete
			state->signalExecuteCompleted();
		},
		[](napi_env env, napi_status status, void* data) { // complete
			auto state = reinterpret_cast<AsyncClearState*>(data);

			state->deleteAsyncWork();

			if (status != napi_cancelled) {
				napi_value global;
				NAPI_STATUS_THROWS_VOID(::napi_get_global(env, &global))

				if (state->status.ok()) {
					napi_value undefined;
					NAPI_STATUS_THROWS_VOID(::napi_get_undefined(env, &undefined))
					state->callResolve(undefined);
				} else {
					ROCKSDB_STATUS_CREATE_NAPI_ERROR_VOID(state->status, "Clear failed")
					state->callReject(error);
				}
			}

			delete state;
		},
		state,     // data
		&state->asyncWork // -> result
	));

	// Register the async work with the database handle
	(*dbHandle)->registerAsyncWork();

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork))

	NAPI_RETURN_UNDEFINED()
}

/**
 * Removes all entries from the RocksDB database by compacting and deleting the files for the column family
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * db.clearSync();
 * ```
 */
napi_value Database::ClearSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_DB_HANDLE_AND_OPEN()
	rocksdb::Status status = (*dbHandle)->clear();
	if (!status.ok()) {
		ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, "Clear failed to write batch")
		::napi_throw(env, error);
		return nullptr;
	}
	NAPI_RETURN_UNDEFINED()
}

/**
 * Closes the RocksDB database. If this is the last database instance for this
 * given path and column family, it will automatically be removed from the
 * registry.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * db.close();
 * ```
 */
napi_value Database::Close(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB_HANDLE()

	if (*dbHandle && (*dbHandle)->descriptor) {
		DEBUG_LOG("%p Database::Close closing database: %s\n", dbHandle->get(), (*dbHandle)->descriptor->path.c_str())
		DBRegistry::CloseDB(*dbHandle);
		DEBUG_LOG("%p Database::Close closed database\n", dbHandle->get())
	} else {
		DEBUG_LOG("%p Database::Close Database not opened\n", dbHandle->get())
	}

	NAPI_RETURN_UNDEFINED()
}

/**
 * Flushes the RocksDB database memtable to disk synchronously.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * db.flushSync();
 * ```
 */
napi_value Database::FlushSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB_HANDLE_AND_OPEN()

	ROCKSDB_STATUS_THROWS_ERROR_LIKE((*dbHandle)->descriptor->flush(), "Flush failed")

	NAPI_RETURN_UNDEFINED()
}

/**
 * Flushes the RocksDB database memtable to disk asynchronously.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * await db.flush();
 * ```
 */
napi_value Database::Flush(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	UNWRAP_DB_HANDLE_AND_OPEN()

	napi_value resolve = argv[0];
	napi_value reject = argv[1];

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(
		env,
		"database.flush",
		NAPI_AUTO_LENGTH,
		&name
	))

	auto state = new AsyncFlushState(env, *dbHandle);
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef))
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef))

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,       // node_env
		nullptr,   // async_resource
		name,      // async_resource_name
		[](napi_env doNotUse, void* data) { // execute
			auto state = reinterpret_cast<AsyncFlushState*>(data);
			// check if database is still open before proceeding
			if (!state->handle || !state->handle->opened() || state->handle->isCancelled()) {
				state->status = rocksdb::Status::Aborted("Database closed during flush operation");
			} else {
				state->status = state->handle->descriptor->flush();
			}
			// signal that execute handler is complete
			state->signalExecuteCompleted();
		},
		[](napi_env env, napi_status status, void* data) { // complete
			auto state = reinterpret_cast<AsyncFlushState*>(data);

			state->deleteAsyncWork();

			if (status != napi_cancelled) {
				if (state->status.ok()) {
					napi_value undefined;
					NAPI_STATUS_THROWS_VOID(::napi_get_undefined(env, &undefined))
					state->callResolve(undefined);
				} else {
					ROCKSDB_STATUS_CREATE_NAPI_ERROR_VOID(state->status, "Flush failed")
					state->callReject(error);
				}
			}

			delete state;
		},
		state,
		&state->asyncWork
	))

	(*dbHandle)->registerAsyncWork();

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork))

	NAPI_RETURN_UNDEFINED()
}

/**
 * Gets a value from the RocksDB database.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const value = await db.get('foo');
 * ```
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const txnId = 123;
 * const value = await db.get('foo', txnId);
 * ```
 */
napi_value Database::Get(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(4)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	napi_value resolve = argv[1];
	napi_value reject = argv[2];
	UNWRAP_DB_HANDLE_AND_OPEN()

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);

	napi_valuetype txnIdType;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[3], &txnIdType));

	if (txnIdType == napi_number) {
		uint32_t txnId;
		NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[3], &txnId));

		auto txnHandle = (*dbHandle)->descriptor->transactionGet(txnId);
		if (!txnHandle) {
			std::string errorMsg = "Get failed: Transaction not found (txnId: " + std::to_string(txnId) + ")";
			::napi_throw_error(env, nullptr, errorMsg.c_str());
			NAPI_RETURN_UNDEFINED()
		}
		return txnHandle->get(env, keySlice, resolve, reject, *dbHandle);
	}

	rocksdb::ReadOptions readOptions;
	readOptions.read_tier = rocksdb::kBlockCacheTier;

	// try to get the value from the block cache
	std::string value;
	rocksdb::Status status = (*dbHandle)->descriptor->db->Get(
		readOptions,
		(*dbHandle)->column.get(),
		keySlice,
		&value
	);

	if (!status.IsIncomplete()) {
		// found it in the block cache!
		return resolveGetSyncResult(env, "Get failed", status, value, resolve, reject);
	}

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_latin1(
		env,
		"rocksdb-js.get",
		NAPI_AUTO_LENGTH,
		&name
	))

	readOptions.read_tier = rocksdb::kReadAllTier;
	auto state = new AsyncGetState<std::shared_ptr<DBHandle>>(env, *dbHandle, readOptions, keySlice);
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef))
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef))

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,       // node_env
		nullptr,   // async_resource
		name,      // async_resource_name
		[](napi_env doNotUse, void* data) { // execute
			auto state = reinterpret_cast<AsyncGetState<std::shared_ptr<DBHandle>>*>(data);
			// check if database is still open before proceeding
			if (!state->handle || !state->handle->opened() || state->handle->isCancelled()) {
				state->status = rocksdb::Status::Aborted("Database closed during get operation");
			} else {
				state->status = state->handle->descriptor->db->Get(
					state->readOptions,
					state->handle->column.get(),
					state->keySlice,
					&state->value
				);
			}
			// signal that execute handler is complete
			state->signalExecuteCompleted();
		},
		[](napi_env env, napi_status status, void* data) { // complete
			auto state = reinterpret_cast<AsyncGetState<std::shared_ptr<DBHandle>>*>(data);

			state->deleteAsyncWork();

			if (status != napi_cancelled) {
				resolveGetResult(env, "Get failed", state);
			}

			delete state;
		},
		state,     // data
		&state->asyncWork // -> result
	));

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork))

	napi_value returnStatus;
	NAPI_STATUS_THROWS(::napi_create_uint32(env, 1, &returnStatus))
	return returnStatus;
}

/**
 * Gets the number of keys within a range or in the entire RocksDB database.
 *
 * @example
 * ```typescript
 * const db = NativeDatabase.open('path/to/db');
 * const total = db.getCount();
 * const range = db.getCount({ start: 'a', end: 'z' });
 * ```
 */
napi_value Database::GetCount(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	UNWRAP_DB_HANDLE_AND_OPEN()

	DBIteratorOptions itOptions;
	itOptions.initFromNapiObject(env, argv[0]);
	itOptions.values = false;

	uint64_t count = 0;

	napi_valuetype txnIdType;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[1], &txnIdType));

	if (txnIdType == napi_number) {
		uint32_t txnId;
		NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[1], &txnId));

		auto txnHandle = (*dbHandle)->descriptor->transactionGet(txnId);
		if (!txnHandle) {
			std::string errorMsg = "Get count failed: Transaction not found (txnId: " + std::to_string(txnId) + ")";
			::napi_throw_error(env, nullptr, errorMsg.c_str());
			NAPI_RETURN_UNDEFINED()
		}
		txnHandle->getCount(itOptions, count, *dbHandle);
	} else {
		// if we don't have a start or end key, we can just get the estimated number of keys
		if (itOptions.startKeyStr == nullptr && itOptions.endKeyStr == nullptr) {
			(*dbHandle)->descriptor->db->GetIntProperty(
				(*dbHandle)->column.get(),
				"rocksdb.estimate-num-keys",
				&count
			);
		} else {
			// for range queries, we need to iterate to count keys
			std::unique_ptr<DBIteratorHandle> itHandle = std::make_unique<DBIteratorHandle>(*dbHandle, itOptions);
			while (itHandle->iterator->Valid()) {
				++count;
				itHandle->iterator->Next();
			}
		}
	}

	DEBUG_LOG("%p Database::GetCount count=%llu\n", dbHandle->get(), count)

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_int64(env, count, &result))
	return result;
}

napi_value Database::GetMonotonicTimestamp(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB_HANDLE_AND_OPEN()

	double timestamp = rocksdb_js::getMonotonicTimestamp();
	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_double(env, timestamp, &result))
	return result;
}

/**
 * Gets the oldest unreleased snapshot unix timestamp.
 *
 * @example
 * ```typescript
 * const db = NativeDatabase.open('path/to/db');
 * const oldestSnapshotTimestamp = db.getOldestSnapshotTimestamp();
 * ```
 */
napi_value Database::GetOldestSnapshotTimestamp(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB_HANDLE_AND_OPEN()

	uint64_t timestamp = 0;
	bool success = (*dbHandle)->descriptor->db->GetIntProperty(
		(*dbHandle)->column.get(),
		"rocksdb.oldest-snapshot-time",
		&timestamp
	);

	if (!success) {
		::napi_throw_error(env, nullptr, "Failed to get oldest snapshot timestamp");
		NAPI_RETURN_UNDEFINED()
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_int64(env, timestamp, &result))
	return result;
}

/**
 * Gets a value from the RocksDB database.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const value = await db.get('foo');
 * ```
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const txnId = 123;
 * const value = await db.get('foo', txnId);
 * ```
 */
napi_value Database::GetSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	UNWRAP_DB_HANDLE_AND_OPEN()

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);
	std::string value;
	rocksdb::Status status;

	napi_valuetype txnIdType;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[1], &txnIdType));

	if (txnIdType == napi_number) {
		uint32_t txnId;
		NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[1], &txnId));

		auto txnHandle = (*dbHandle)->descriptor->transactionGet(txnId);
		if (!txnHandle) {
			std::string errorMsg = "Get sync failed: Transaction not found (txnId: " + std::to_string(txnId) + ")";
			::napi_throw_error(env, nullptr, errorMsg.c_str());
			NAPI_RETURN_UNDEFINED()
		}
		status = txnHandle->getSync(keySlice, value, *dbHandle);
	} else {
		status = (*dbHandle)->descriptor->db->Get(
			rocksdb::ReadOptions(),
			(*dbHandle)->column.get(),
			keySlice,
			&value
		);
	}

	if (status.IsNotFound()) {
		NAPI_RETURN_UNDEFINED()
	}

	if (!status.ok()) {
		ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, "Get failed")
		::napi_throw(env, error);
		return nullptr;
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_buffer_copy(
		env,
		value.size(),
		value.data(),
		nullptr,
		&result
	))

	return result;
}

/**
 * Gets or creates a buffer that an be shared across worker threads.
 */
napi_value Database::GetUserSharedBuffer(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(3)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	std::string keyStr(key + keyStart, keyEnd - keyStart);
	UNWRAP_DB_HANDLE_AND_OPEN()

	// if we have a callback, add it as a listener
	napi_ref callbackRef = nullptr;
	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[2], &type))
	if (type != napi_undefined) {
		if (type == napi_function) {
			DEBUG_LOG("Database::GetUserSharedBuffer key start=%u end=%u:\n", keyStart, keyEnd)
			DEBUG_LOG_KEY_LN(keyStr)
			callbackRef = (*dbHandle)->descriptor->addListener(env, keyStr, argv[2], *dbHandle);
		} else {
			::napi_throw_error(env, nullptr, "Callback must be a function");
			return nullptr;
		}
	}

	return (*dbHandle)->descriptor->getUserSharedBuffer(env, keyStr, argv[1], callbackRef);
}

/**
 * Checks if the database has a lock on the given key.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const hasLock = db.hasLock('foo');
 * ```
 */
napi_value Database::HasLock(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	UNWRAP_DB_HANDLE_AND_OPEN()

	std::string keyStr(key + keyStart, keyEnd - keyStart);
	bool hasLock = (*dbHandle)->descriptor->lockExistsByKey(keyStr);

	napi_value result;
	NAPI_STATUS_THROWS(::napi_get_boolean(
		env,
		hasLock,
		&result
	))
	return result;
}

/**
 * Checks if the RocksDB database is open.
 */
napi_value Database::IsOpen(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB_HANDLE()

	napi_value result;
	NAPI_STATUS_THROWS(::napi_get_boolean(env, (*dbHandle)->opened(), &result))
	return result;
}

/**
 * Lists all transaction logs in the database.
 */
napi_value Database::ListLogs(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB_HANDLE_AND_OPEN()
	return (*dbHandle)->descriptor->listTransactionLogStores(env);
}

/**
 * Opens the RocksDB database. This must be called before any data methods are called.
 */
napi_value Database::Open(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	UNWRAP_DB_HANDLE()

	if ((*dbHandle)->opened()) {
		// already open
		NAPI_RETURN_UNDEFINED()
	}

	NAPI_GET_STRING(argv[0], path, "Database path is required")
	const napi_value options = argv[1];

	bool disableWAL = false;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "disableWAL", disableWAL));

	std::string name;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "name", name));

	bool noBlockCache = false;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "noBlockCache", noBlockCache));

	std::string modeName;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "mode", modeName));

	uint32_t parallelismThreads = std::max<uint32_t>(1, std::thread::hardware_concurrency() / 2);
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "parallelismThreads", parallelismThreads));

	uint32_t transactionLogRetentionMs = 3 * 24 * 60 * 60 * 1000; // 3 days
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "transactionLogRetentionMs", transactionLogRetentionMs));

	float transactionLogMaxAgeThreshold = 0.75f;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "transactionLogMaxAgeThreshold", transactionLogMaxAgeThreshold));
	if (transactionLogMaxAgeThreshold < 0.0f || transactionLogMaxAgeThreshold > 1.0f) {
		::napi_throw_error(env, nullptr, "transactionLogMaxAgeThreshold must be between 0.0 and 1.0");
		return nullptr;
	}

	std::string transactionLogsPath = (std::filesystem::path(path) / "transaction_logs").string();
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "transactionLogsPath", transactionLogsPath));

	uint32_t transactionLogMaxSize = 16 * 1024 * 1024; // 16MB
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "transactionLogMaxSize", transactionLogMaxSize));
	if (transactionLogMaxSize > 0 && transactionLogMaxSize < TRANSACTION_LOG_ENTRY_HEADER_SIZE) {
		std::string errorMsg = "transactionLogMaxSize must be greater than " + std::to_string(TRANSACTION_LOG_ENTRY_HEADER_SIZE) + " bytes";
		::napi_throw_error(env, nullptr, errorMsg.c_str());
		return nullptr;
	}

	DBMode mode = DBMode::Optimistic;
	if (modeName == "pessimistic") {
		mode = DBMode::Pessimistic;
	}

	DBOptions dbHandleOptions {
		disableWAL,
		mode,
		name,
		noBlockCache,
		parallelismThreads,
		transactionLogMaxAgeThreshold,
		transactionLogMaxSize,
		transactionLogRetentionMs,
		transactionLogsPath
	};

	try {
		(*dbHandle)->open(path, dbHandleOptions);
	} catch (const std::exception& e) {
		DEBUG_LOG("%p Database::Open Error: %s\n", dbHandle->get(), e.what())
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}

	NAPI_RETURN_UNDEFINED()
}

/**
 * Purges transaction logs.
 */
napi_value Database::PurgeLogs(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_DB_HANDLE_AND_OPEN()

	return (*dbHandle)->descriptor->purgeTransactionLogs(env, argv[0]);
}

/**
 * Puts a key-value pair into the RocksDB database.
 */
napi_value Database::PutSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(3)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	NAPI_GET_BUFFER(argv[1], value, nullptr)
	UNWRAP_DB_HANDLE_AND_OPEN()

	rocksdb::Status status;

	napi_valuetype txnIdType;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[2], &txnIdType));

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);
	rocksdb::Slice valueSlice(value + valueStart, valueEnd - valueStart);

	DEBUG_LOG("%p Database::PutSync key:", dbHandle->get())
	DEBUG_LOG_KEY_LN(keySlice)

	DEBUG_LOG("%p Database::PutSync value:", dbHandle->get())
	DEBUG_LOG_KEY_LN(valueSlice)

	if (txnIdType == napi_number) {
		uint32_t txnId;
		NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[2], &txnId));

		auto txnHandle = (*dbHandle)->descriptor->transactionGet(txnId);
		if (!txnHandle) {
			std::string errorMsg = "Put sync failed: Transaction not found (txnId: " + std::to_string(txnId) + ")";
			::napi_throw_error(env, nullptr, errorMsg.c_str());
			NAPI_RETURN_UNDEFINED()
		}
		status = txnHandle->putSync(
			keySlice,
			valueSlice,
			*dbHandle
		);
	} else {
		rocksdb::WriteOptions writeOptions;
		writeOptions.disableWAL = (*dbHandle)->disableWAL;
		status = (*dbHandle)->descriptor->db->Put(
			writeOptions,
			(*dbHandle)->column.get(),
			keySlice,
			valueSlice
		);
	}

	if (!status.ok()) {
		ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, "Put failed")
		::napi_throw(env, error);
		return nullptr;
	}

	NAPI_RETURN_UNDEFINED()
}


/**
 * Removes a key from the RocksDB database.
 */
napi_value Database::RemoveSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	UNWRAP_DB_HANDLE_AND_OPEN()

	rocksdb::Status status;

	napi_valuetype txnIdType;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[1], &txnIdType));

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);

	if (txnIdType == napi_number) {
		uint32_t txnId;
		NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[1], &txnId));

		auto txnHandle = (*dbHandle)->descriptor->transactionGet(txnId);
		if (!txnHandle) {
			std::string errorMsg = "Remove sync failed: Transaction not found (txnId: " + std::to_string(txnId) + ")";
			::napi_throw_error(env, nullptr, errorMsg.c_str());
			NAPI_RETURN_UNDEFINED()
		}
		status = txnHandle->removeSync(keySlice, *dbHandle);
	} else {
		rocksdb::WriteOptions writeOptions;
		writeOptions.disableWAL = (*dbHandle)->disableWAL;
		status = (*dbHandle)->descriptor->db->Delete(
			writeOptions,
			(*dbHandle)->column.get(),
			keySlice
		);
	}

	if (!status.ok()) {
		ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, "Remove failed")
		::napi_throw(env, error);
		return nullptr;
	}

	NAPI_RETURN_UNDEFINED()
}

/**
 * Tries to acquire a lock on the given key. If a callback is specified, queues
 * the callback to be called when the lock is released.
 *
 * @param key - The key to lock.
 * @param callback - The callback to call when the lock is released.
 *
 * @returns `true` if the lock was acquired, `false` otherwise.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const lockSuccess = db.tryLock('foo', () => {
 *   console.log('lock was released');
 * });
 * ```
 */
napi_value Database::TryLock(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	UNWRAP_DB_HANDLE_AND_OPEN()

	napi_value result;
	std::string keyStr(key + keyStart, keyEnd - keyStart);
	bool isNewLock = false;

	(*dbHandle)->descriptor->lockEnqueueCallback(
		env,       // env
		keyStr,    // key
		argv[1],   // callback
		*dbHandle, // owner
		true,      // skipEnqueueIfExists
		nullptr,   // deferred
		&isNewLock // [out] isNewLock
	);

	NAPI_STATUS_THROWS(::napi_get_boolean(env, isNewLock, &result))
	return result;
}

/**
 * Releases a lock on the given key. If a callback was specified when the lock
 * was acquired, calls the callback.
 *
 * @param key - The key to unlock.
 *
 * @returns `true` if the lock was released, `false` otherwise.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const lockSuccess = db.tryLock('foo', () => {
 *   console.log('lock was released');
 * });
 * db.unlock('foo'); // calls the callback, returns `true`
 * db.unlock('foo'); // returns `false` because the lock was already released
 * ```
 */
napi_value Database::Unlock(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	UNWRAP_DB_HANDLE_AND_OPEN()

	napi_value result;
	std::string keyStr(key + keyStart, keyEnd - keyStart);
	bool unlocked = (*dbHandle)->descriptor->lockReleaseByKey(keyStr);
	NAPI_STATUS_THROWS(::napi_get_boolean(env, unlocked, &result))
	return result;
}

/**
 * Get or create a transaction log.
 */
napi_value Database::UseLog(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	NAPI_GET_STRING(argv[0], name, "Name is required")
	UNWRAP_DB_HANDLE_AND_OPEN()

	return (*dbHandle)->useLog(env, jsThis, name);
}

/**
 * Mutually exclusive execution of a function across threads for a given key.
 */
napi_value Database::WithLock(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")

	// Create a promise first, then check if database is open
	napi_deferred deferred;
	napi_value promise;
	NAPI_STATUS_THROWS(::napi_create_promise(env, &deferred, &promise))

	// Check if database is open
	std::shared_ptr<DBHandle>* dbHandle = nullptr;
	NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&dbHandle)))
	if (dbHandle == nullptr || !(*dbHandle)->opened()) {
		napi_value error;
		napi_create_string_utf8(env, "Database not open", NAPI_AUTO_LENGTH, &error);
		napi_reject_deferred(env, deferred, error);
		return promise;
	}

	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[1], &type))
	if (type != napi_function) {
		napi_value error;
		napi_create_string_utf8(env, "Callback must be a function", NAPI_AUTO_LENGTH, &error);
		napi_reject_deferred(env, deferred, error);
		return promise;
	}

	std::string keyStr(key + keyStart, keyEnd - keyStart);
	(*dbHandle)->descriptor->lockCall(env, keyStr, argv[1], deferred, *dbHandle);

	return promise;
}

/**
 * Initializes the `NativeDatabase` JavaScript class.
 */
void Database::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "addListener", nullptr, AddListener, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "clear", nullptr, Clear, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "clearSync", nullptr, ClearSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "close", nullptr, Close, nullptr, nullptr, nullptr, napi_default, nullptr },
    	{ "flush", nullptr, Flush, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "flushSync", nullptr, FlushSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "get", nullptr, Get, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getCount", nullptr, GetCount, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getMonotonicTimestamp", nullptr, GetMonotonicTimestamp, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getOldestSnapshotTimestamp", nullptr, GetOldestSnapshotTimestamp, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getSync", nullptr, GetSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getUserSharedBuffer", nullptr, GetUserSharedBuffer, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "hasLock", nullptr, HasLock, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "listeners", nullptr, Listeners, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "listLogs", nullptr, ListLogs, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "notify", nullptr, Notify, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "open", nullptr, Open, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "opened", nullptr, nullptr, IsOpen, nullptr, nullptr, napi_default, nullptr },
		{ "purgeLogs", nullptr, PurgeLogs, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "putSync", nullptr, PutSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "removeListener", nullptr, RemoveListener, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "removeSync", nullptr, RemoveSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "tryLock", nullptr, TryLock, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "unlock", nullptr, Unlock, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "useLog", nullptr, UseLog, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "withLock", nullptr, WithLock, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	auto className = "Database";
	constexpr size_t len = sizeof("Database") - 1;

	napi_ref exportsRef;
	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, exports, 1, &exportsRef))

	napi_value ctor;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		className,                           // className
		len,                                 // length of class name
		Database::Constructor,               // constructor
		reinterpret_cast<void*>(exportsRef), // constructor arg
		sizeof(properties) / sizeof(napi_property_descriptor), // number of properties
		properties,                          // properties array
		&ctor                                // [out] constructor
	))

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, ctor))
}

/**
 * Resolves the result of a `Get` operation.
 */
napi_value resolveGetSyncResult(
	napi_env env,
	const char* errorMsg,
	rocksdb::Status& status,
	std::string& value,
	napi_value resolve,
	napi_value reject
) {
	napi_value global;
	NAPI_STATUS_THROWS(::napi_get_global(env, &global))

	napi_value result;

	if (status.IsNotFound()) {
		napi_get_undefined(env, &result);
		NAPI_STATUS_THROWS(::napi_call_function(env, global, resolve, 1, &result, nullptr))
	} else if (!status.ok()) {
		ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, errorMsg)
		NAPI_STATUS_THROWS(::napi_call_function(env, global, reject, 1, &error, nullptr))
	} else {
		// TODO: when in "fast" mode, use the shared buffer
		NAPI_STATUS_THROWS(::napi_create_buffer_copy(
			env,
			value.size(),
			value.data(),
			nullptr,
			&result
		))
		NAPI_STATUS_THROWS(::napi_call_function(env, global, resolve, 1, &result, nullptr))
	}

	napi_value returnStatus;
	NAPI_STATUS_THROWS(::napi_create_uint32(env, 0, &returnStatus))
	return returnStatus;
}

} // namespace rocksdb_js
