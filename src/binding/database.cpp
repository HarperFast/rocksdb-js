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
	NAPI_CONSTRUCTOR_WITH_DATA("Database");

	// create shared_ptr on heap so it persists after function returns
	napi_ref exportsRef = reinterpret_cast<napi_ref>(data);
	auto* dbHandle = new std::shared_ptr<DBHandle>(std::make_shared<DBHandle>(env, exportsRef));

	DEBUG_LOG("Database::Constructor Creating NativeDatabase DBHandle=%p\n", dbHandle->get());

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(dbHandle),
			[](napi_env env, void* data, void* hint) {
				DEBUG_LOG("Database::Constructor NativeDatabase GC'd dbHandle=%p\n", data);
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
 * Removes all entries in a RocksDB database column family using an uncapped
 * range.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * await db.clear();
 * ```
 */
napi_value Database::Clear(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2);
	UNWRAP_DB_HANDLE_AND_OPEN();

	napi_value resolve = argv[0];
	napi_value reject = argv[1];

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(
		env,
		"database.clear",
		NAPI_AUTO_LENGTH,
		&name
	));

	auto state = new AsyncClearState(env, *dbHandle);
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef));
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef));

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
				NAPI_STATUS_THROWS_VOID(::napi_get_global(env, &global));

				if (state->status.ok()) {
					napi_value undefined;
					NAPI_STATUS_THROWS_VOID(::napi_get_undefined(env, &undefined));
					state->callResolve(undefined);
				} else {
					ROCKSDB_STATUS_CREATE_NAPI_ERROR_VOID(state->status, "Failed to clear database");
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

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork));

	NAPI_RETURN_UNDEFINED();
}

/**
 * Removes all entries in a RocksDB database column family using an uncapped
 * range (synchronously).
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * db.clearSync();
 * ```
 */
napi_value Database::ClearSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	UNWRAP_DB_HANDLE_AND_OPEN();
	rocksdb::Status status = (*dbHandle)->clear();
	if (!status.ok()) {
		ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, "Failed to clear database");
		::napi_throw(env, error);
		return nullptr;
	}
	NAPI_RETURN_UNDEFINED();
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
	NAPI_METHOD();
	UNWRAP_DB_HANDLE();

	if (*dbHandle && (*dbHandle)->descriptor) {
		DEBUG_LOG("%p Database::Close closing database: %s\n", dbHandle->get(), (*dbHandle)->descriptor->path.c_str());
		DBRegistry::CloseDB(*dbHandle);
		DEBUG_LOG("%p Database::Close closed database\n", dbHandle->get());
	} else {
		DEBUG_LOG("%p Database::Close Database not opened\n", dbHandle->get());
	}

	NAPI_RETURN_UNDEFINED();
}

/**
 * Destroys the RocksDB database.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * db.destroy();
 * ```
 */
napi_value Database::Destroy(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	UNWRAP_DB_HANDLE();

	if (*dbHandle) {
		try {
			DBRegistry::DestroyDB((*dbHandle)->path);
		} catch (const std::exception& e) {
			DEBUG_LOG("%p Database::Destroy Error: %s\n", dbHandle->get(), e.what());
			::napi_throw_error(env, nullptr, e.what());
			return nullptr;
		}
	} else {
		::napi_throw_error(env, nullptr, "Invalid database handle");
		return nullptr;
	}

	NAPI_RETURN_UNDEFINED();
}

/**
 * Drops the RocksDB database column family asynchronously. If the column family
 * is the default, it will clear the database instead.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * await db.drop();
 * ```
 */
napi_value Database::Drop(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2);
	UNWRAP_DB_HANDLE_AND_OPEN();

	if ((*dbHandle)->column->GetName() == "default") {
		return Database::Clear(env, info);
	}

	napi_value resolve = argv[0];
	napi_value reject = argv[1];

	napi_value global;
	NAPI_STATUS_THROWS(::napi_get_global(env, &global));

	DEBUG_LOG("%p Database::Drop dropping database: %s\n", dbHandle->get(), (*dbHandle)->descriptor->path.c_str());
	rocksdb::Status status = (*dbHandle)->descriptor->db->DropColumnFamily((*dbHandle)->column.get());
	if (!status.ok()) {
		ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, "Failed to drop database");
		NAPI_STATUS_THROWS_ERROR(::napi_call_function(
			env, global, reject, 1, &error, nullptr
		), "Failed to call reject function");
		return nullptr;
	}

	NAPI_STATUS_THROWS_ERROR(::napi_call_function(
		env, global, resolve, 0, nullptr, nullptr
	), "Failed to call resolve function");
	DEBUG_LOG("%p Database::Drop dropped database\n", dbHandle->get());
	NAPI_RETURN_UNDEFINED();
}

/**
 * Drops the RocksDB database column family. If the column family is the
 * default, it will clear the database instead.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * db.dropSync();
 * ```
 */
napi_value Database::DropSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_DB_HANDLE_AND_OPEN();

	if ((*dbHandle)->column->GetName() == "default") {
		return Database::ClearSync(env, info);
	}

	DEBUG_LOG("%p Database::DropSync dropping database: %s\n", dbHandle->get(), (*dbHandle)->descriptor->path.c_str());
	ROCKSDB_STATUS_THROWS_ERROR_LIKE((*dbHandle)->descriptor->db->DropColumnFamily((*dbHandle)->column.get()), "Failed to drop database");
	DEBUG_LOG("%p Database::DropSync dropped database\n", dbHandle->get());
	NAPI_RETURN_UNDEFINED();
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
	NAPI_METHOD();
	UNWRAP_DB_HANDLE_AND_OPEN();

	ROCKSDB_STATUS_THROWS_ERROR_LIKE((*dbHandle)->descriptor->flush(), "Flush failed");

	NAPI_RETURN_UNDEFINED();
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
	NAPI_METHOD_ARGV(2);
	UNWRAP_DB_HANDLE_AND_OPEN();

	napi_value resolve = argv[0];
	napi_value reject = argv[1];

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(
		env,
		"database.flush",
		NAPI_AUTO_LENGTH,
		&name
	));

	auto state = new AsyncFlushState(env, *dbHandle);
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef));
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef));

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
					NAPI_STATUS_THROWS_VOID(::napi_get_undefined(env, &undefined));
					state->callResolve(undefined);
				} else {
					ROCKSDB_STATUS_CREATE_NAPI_ERROR_VOID(state->status, "Flush failed");
					state->callReject(error);
				}
			}

			delete state;
		},
		state,
		&state->asyncWork
	));

	(*dbHandle)->registerAsyncWork();

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork));

	NAPI_RETURN_UNDEFINED();
}

/**
 * Asynchronously gets a value from the RocksDB database. The first argument, that specifies the key, can be a buffer or a number
 * indicating the length of the key that was written to the shared buffer.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const value = await db.get('foo');
 * ```
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const b = Buffer.alloc(1024);
 * db.setDefaultKeyBuffer(b);
 * b.utf8Write('foo');
 * const value = await db.get(3);
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
	NAPI_METHOD_ARGV(4);

	UNWRAP_DB_HANDLE_AND_OPEN();
	rocksdb::Slice keySlice;
	if (!rocksdb_js::getSliceFromArg(env, argv[0], keySlice, (*dbHandle)->defaultKeyBufferPtr, "Key must be a buffer")) {
		return nullptr;
	}
	std::string key(keySlice.data(), keySlice.size());

	napi_value resolve = argv[1];
	napi_value reject = argv[2];

	napi_valuetype txnIdType;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[3], &txnIdType));

	if (txnIdType == napi_number) {
		uint32_t txnId;
		NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[3], &txnId));

		auto txnHandle = (*dbHandle)->descriptor->transactionGet(txnId);
		if (!txnHandle) {
			std::string errorMsg = "Get failed: Transaction not found (txnId: " + std::to_string(txnId) + ")";
			::napi_throw_error(env, nullptr, errorMsg.c_str());
			NAPI_RETURN_UNDEFINED();
		}
		return txnHandle->get(env, key, resolve, reject, *dbHandle);
	}

	rocksdb::ReadOptions readOptions;
	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_latin1(
		env,
		"rocksdb-js.get",
		NAPI_AUTO_LENGTH,
		&name
	));

	auto state = new AsyncGetState<std::shared_ptr<DBHandle>>(env, *dbHandle, readOptions, std::move(key));
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef));
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef));

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
					state->key,
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

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork));

	napi_value returnStatus;
	NAPI_STATUS_THROWS(::napi_create_uint32(env, 1, &returnStatus));
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
	NAPI_METHOD_ARGV(2);
	UNWRAP_DB_HANDLE_AND_OPEN();

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
			NAPI_RETURN_UNDEFINED();
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

	DEBUG_LOG("%p Database::GetCount count=%llu\n", dbHandle->get(), count);

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_int64(env, count, &result));
	return result;
}

napi_value Database::GetMonotonicTimestamp(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_DB_HANDLE_AND_OPEN();

	double timestamp = rocksdb_js::getMonotonicTimestamp();
	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_double(env, timestamp, &result));
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
	NAPI_METHOD();
	UNWRAP_DB_HANDLE_AND_OPEN();

	uint64_t timestamp = 0;
	bool success = (*dbHandle)->descriptor->db->GetIntProperty(
		(*dbHandle)->column.get(),
		"rocksdb.oldest-snapshot-time",
		&timestamp
	);

	if (!success) {
		::napi_throw_error(env, nullptr, "Failed to get oldest snapshot timestamp");
		NAPI_RETURN_UNDEFINED();
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_int64(env, timestamp, &result));
	return result;
}

/**
 * Gets a RocksDB database property as a string.
 *
 * @example
 * ```typescript
 * const db = NativeDatabase.open('path/to/db');
 * const levelStats = db.getDBProperty('rocksdb.levelstats');
 * ```
 */
napi_value Database::GetDBProperty(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	UNWRAP_DB_HANDLE_AND_OPEN();

	NAPI_GET_STRING(argv[0], propertyName, "Property name is required");

	std::string value;
	bool success = (*dbHandle)->descriptor->db->GetProperty(
		(*dbHandle)->column.get(),
		propertyName,
		&value
	);

	if (!success) {
		::napi_throw_error(env, nullptr, "Failed to get database property");
		NAPI_RETURN_UNDEFINED();
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(
		env,
		value.c_str(),
		value.length(),
		&result
	));
	return result;
}

/**
 * Gets a RocksDB database property as an integer.
 *
 * @example
 * ```typescript
 * const db = NativeDatabase.open('path/to/db');
 * const blobFiles = db.getDBIntProperty('rocksdb.num-blob-files');
 * ```
 */
napi_value Database::GetDBIntProperty(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	UNWRAP_DB_HANDLE_AND_OPEN();

	NAPI_GET_STRING(argv[0], propertyName, "Property name is required");

	uint64_t value = 0;
	bool success = (*dbHandle)->descriptor->db->GetIntProperty(
		(*dbHandle)->column.get(),
		propertyName,
		&value
	);

	if (!success) {
		::napi_throw_error(env, nullptr, "Failed to get database integer property");
		NAPI_RETURN_UNDEFINED();
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_int64(env, value, &result));
	return result;
}

/**
 * Synchronously gets a value from the RocksDB database. The first argument, that specifies the key, can be a buffer or a number
 * indicating the length of the key that was written to the shared buffer.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const value = db.getSync('foo');
 * ```
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const b = Buffer.alloc(1024);
 * db.setDefaultKeyBuffer(b);
 * b.utf8Write('foo');
 * const value = db.getSync(3);
 * ```
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const txnId = 123;
 * const value = db.getSync('foo', txnId);
 * ```
 */
napi_value Database::GetSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(3);
	UNWRAP_DB_HANDLE_AND_OPEN();
	// we store this in key slice (no copying) because we are synchronously using the key
	rocksdb::Slice keySlice;
	if (!rocksdb_js::getSliceFromArg(env, argv[0], keySlice, (*dbHandle)->defaultKeyBufferPtr, "Key must be a buffer")) {
		return nullptr;
	}
	int32_t flags;
	NAPI_STATUS_THROWS(::napi_get_value_int32(env, argv[1], &flags));
	rocksdb::PinnableSlice value; // we can use a PinnableSlice here, so we can copy directly from the database cache to our buffer
	rocksdb::Status status;

	napi_valuetype txnIdType;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[2], &txnIdType));
	rocksdb::ReadOptions readOptions;
	if (flags & ONLY_IF_IN_MEMORY_CACHE_FLAG) {
		// this is used by get() so that the getSync() call will fail if the entry is not in the cache
		readOptions.read_tier = rocksdb::kBlockCacheTier;
	}

	if (txnIdType == napi_number) {
		uint32_t txnId;
		NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[2], &txnId));

		auto txnHandle = (*dbHandle)->descriptor->transactionGet(txnId);
		if (!txnHandle) {
			std::string errorMsg = "Get sync failed: Transaction not found (txnId: " + std::to_string(txnId) + ")";
			::napi_throw_error(env, nullptr, errorMsg.c_str());
			NAPI_RETURN_UNDEFINED();
		}
		status = txnHandle->getSync(keySlice, value, readOptions, *dbHandle);
	} else {
		status = (*dbHandle)->descriptor->db->Get(
			readOptions,
			(*dbHandle)->column.get(),
			keySlice,
			&value
		);
	}

	if (status.IsNotFound()) {
		NAPI_RETURN_UNDEFINED();
	}

	napi_value result;
	if (status.IsIncomplete()) {
		// This means we only wanted values in memory, it was not found, so return a flag indicating that
		NAPI_STATUS_THROWS(::napi_create_int32(env, NOT_IN_MEMORY_CACHE_FLAG, &result));
		return result;
	}

	if (!status.ok()) {
		ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, "Get failed");
		::napi_throw(env, error);
		return nullptr;
	}

	if (!(flags & ALWAYS_CREATE_NEW_BUFFER_FLAG) && // this flag is used by getBinary() to force a new buffer to be created (that can safely live long-term)
			(*dbHandle)->defaultValueBufferPtr != nullptr &&
			value.size() <= (*dbHandle)->defaultValueBufferLength) {
		// if it fits in the default value buffer, copy the data and just return the length
		::memcpy((*dbHandle)->defaultValueBufferPtr, value.data(), value.size());
		NAPI_STATUS_THROWS(::napi_create_int32(env, value.size(), &result));
		return result;
	}

	// otherwise, create a new buffer and return it
	NAPI_STATUS_THROWS(::napi_create_buffer_copy(
		env,
		value.size(),
		value.data(),
		nullptr,
		&result
	));

	return result;
}

/**
 * Sets the default value buffer to be used for fast access. Creating new buffers (especially from C++/NAPI) is
 * *extremely* expensive, and by using a single shared buffer, we can avoid the overhead of buffer creation, and instead
 * copy directly from the database to the shared buffer. So this sets the value buffer that will be used for transferring
 * smaller values to and from JavaScript to C++. Note that we generally still use new buffers for larger values, as the
 * overhead of buffer creation is smaller compared to the cost of the copying of data.
 */
napi_value Database::SetDefaultValueBuffer(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	UNWRAP_DB_HANDLE();

	if (argc == 0) {
		(*dbHandle)->defaultValueBufferPtr = nullptr;
		(*dbHandle)->defaultValueBufferLength = 0;
		NAPI_RETURN_UNDEFINED();
	}

	void* data;
	size_t length;
	NAPI_STATUS_THROWS(::napi_get_buffer_info(env, argv[0], &data, &length));

	(*dbHandle)->defaultValueBufferPtr = (char*) data;
	(*dbHandle)->defaultValueBufferLength = length;

	NAPI_RETURN_UNDEFINED();
}

/**
 * Sets the default key buffer to be used for fast access. Creating new buffers (especially from C++/NAPI) is
 * *extremely* expensive, and by using a single shared buffer, we can avoid the overhead of buffer creation, and instead
 * place keys directly in a shared buffer that can be reused. This sets the key buffer that is used for transferring
 * keys to and from JavaScript to C++.
 */
napi_value Database::SetDefaultKeyBuffer(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	UNWRAP_DB_HANDLE();

	void* data;
	size_t length;
	NAPI_STATUS_THROWS(::napi_get_buffer_info(env, argv[0], &data, &length));

	(*dbHandle)->defaultKeyBufferPtr = (char*) data;
	(*dbHandle)->defaultKeyBufferLength = length;

	NAPI_RETURN_UNDEFINED();
}

/**
 * Gets or creates a buffer that an be shared across worker threads.
 */
napi_value Database::GetUserSharedBuffer(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(3);
	NAPI_GET_BUFFER(argv[0], key, "Key is required");
	UNWRAP_DB_HANDLE_AND_OPEN();
	std::string keyStr(key + keyStart, keyEnd - keyStart);

	// if we have a callback, add it as a listener
	napi_ref callbackRef = nullptr;
	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[2], &type));
	if (type != napi_undefined) {
		if (type == napi_function) {
			DEBUG_LOG("Database::GetUserSharedBuffer key start=%u end=%u:\n", keyStart, keyEnd);
			DEBUG_LOG_KEY_LN(keyStr);
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
	NAPI_METHOD_ARGV(1);
	NAPI_GET_BUFFER(argv[0], key, "Key is required");
	UNWRAP_DB_HANDLE_AND_OPEN();

	std::string keyStr(key + keyStart, keyEnd - keyStart);
	bool hasLock = (*dbHandle)->descriptor->lockExistsByKey(keyStr);

	napi_value result;
	NAPI_STATUS_THROWS(::napi_get_boolean(
		env,
		hasLock,
		&result
	));
	return result;
}

/**
 * Checks if the RocksDB database is open.
 */
napi_value Database::IsOpen(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_DB_HANDLE();

	napi_value result;
	NAPI_STATUS_THROWS(::napi_get_boolean(env, (*dbHandle)->opened(), &result));
	return result;
}

/**
 * Lists all transaction logs in the database.
 */
napi_value Database::ListLogs(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_DB_HANDLE_AND_OPEN();
	return (*dbHandle)->descriptor->listTransactionLogStores(env);
}

/**
 * Opens the RocksDB database. This must be called before any data methods are called.
 */
napi_value Database::Open(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2);
	UNWRAP_DB_HANDLE();

	if ((*dbHandle)->opened()) {
		// already open
		NAPI_RETURN_UNDEFINED();
	}

	NAPI_GET_STRING(argv[0], path, "Database path is required");
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
		DEBUG_LOG("%p Database::Open Error: %s\n", dbHandle->get(), e.what());
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}

	NAPI_RETURN_UNDEFINED();
}

/**
 * Purges transaction logs.
 */
napi_value Database::PurgeLogs(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	UNWRAP_DB_HANDLE_AND_OPEN();

	return (*dbHandle)->descriptor->purgeTransactionLogs(env, argv[0]);
}

/**
 * Puts a key-value pair into the RocksDB database.
 */
napi_value Database::PutSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(3);
	NAPI_GET_BUFFER(argv[0], key, "Key is required");
	NAPI_GET_BUFFER(argv[1], value, nullptr);
	UNWRAP_DB_HANDLE_AND_OPEN();

	rocksdb::Status status;

	napi_valuetype txnIdType;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[2], &txnIdType));

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);
	rocksdb::Slice valueSlice(value + valueStart, valueEnd - valueStart);

	DEBUG_LOG("%p Database::PutSync key:", dbHandle->get());
	DEBUG_LOG_KEY_LN(keySlice);

	DEBUG_LOG("%p Database::PutSync value:", dbHandle->get());
	DEBUG_LOG_KEY_LN(valueSlice);

	if (txnIdType == napi_number) {
		uint32_t txnId;
		NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[2], &txnId));

		auto txnHandle = (*dbHandle)->descriptor->transactionGet(txnId);
		if (!txnHandle) {
			std::string errorMsg = "Put sync failed: Transaction not found (txnId: " + std::to_string(txnId) + ")";
			::napi_throw_error(env, nullptr, errorMsg.c_str());
			NAPI_RETURN_UNDEFINED();
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
		ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, "Put failed");
		::napi_throw(env, error);
		return nullptr;
	}

	NAPI_RETURN_UNDEFINED();
}


/**
 * Removes a key from the RocksDB database.
 */
napi_value Database::RemoveSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2);
	NAPI_GET_BUFFER(argv[0], key, "Key is required");
	UNWRAP_DB_HANDLE_AND_OPEN();

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
			NAPI_RETURN_UNDEFINED();
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
		ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, "Remove failed");
		::napi_throw(env, error);
		return nullptr;
	}

	NAPI_RETURN_UNDEFINED();
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
	NAPI_METHOD_ARGV(2);
	NAPI_GET_BUFFER(argv[0], key, "Key is required");
	UNWRAP_DB_HANDLE_AND_OPEN();

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

	NAPI_STATUS_THROWS(::napi_get_boolean(env, isNewLock, &result));
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
	NAPI_METHOD_ARGV(1);
	NAPI_GET_BUFFER(argv[0], key, "Key is required");
	UNWRAP_DB_HANDLE_AND_OPEN();

	napi_value result;
	std::string keyStr(key + keyStart, keyEnd - keyStart);
	bool unlocked = (*dbHandle)->descriptor->lockReleaseByKey(keyStr);
	NAPI_STATUS_THROWS(::napi_get_boolean(env, unlocked, &result));
	return result;
}

/**
 * Get or create a transaction log.
 */
napi_value Database::UseLog(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	NAPI_GET_STRING(argv[0], name, "Name is required");
	UNWRAP_DB_HANDLE_AND_OPEN();

	return (*dbHandle)->useLog(env, jsThis, name);
}

/**
 * Mutually exclusive execution of a function across threads for a given key.
 */
napi_value Database::WithLock(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2);
	NAPI_GET_BUFFER(argv[0], key, "Key is required");

	// Create a promise first, then check if database is open
	napi_deferred deferred;
	napi_value promise;
	NAPI_STATUS_THROWS(::napi_create_promise(env, &deferred, &promise));

	// Check if database is open
	std::shared_ptr<DBHandle>* dbHandle = nullptr;
	NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&dbHandle)));
	if (dbHandle == nullptr || !(*dbHandle)->opened()) {
		napi_value error;
		NAPI_STATUS_THROWS(::napi_create_string_utf8(env, "Database not open", NAPI_AUTO_LENGTH, &error));
		NAPI_STATUS_THROWS(::napi_reject_deferred(env, deferred, error));
		return promise;
	}

	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[1], &type));
	if (type != napi_function) {
		napi_value error;
		NAPI_STATUS_THROWS(::napi_create_string_utf8(env, "Callback must be a function", NAPI_AUTO_LENGTH, &error));
		NAPI_STATUS_THROWS(::napi_reject_deferred(env, deferred, error));
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
		{ "destroy", nullptr, Destroy, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "drop", nullptr, Drop, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "dropSync", nullptr, DropSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "flush", nullptr, Flush, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "flushSync", nullptr, FlushSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "get", nullptr, Get, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getCount", nullptr, GetCount, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getDBIntProperty", nullptr, GetDBIntProperty, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getDBProperty", nullptr, GetDBProperty, nullptr, nullptr, nullptr, napi_default, nullptr },
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
		{ "setDefaultValueBuffer", nullptr, SetDefaultValueBuffer, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "setDefaultKeyBuffer", nullptr, SetDefaultKeyBuffer, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "tryLock", nullptr, TryLock, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "unlock", nullptr, Unlock, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "useLog", nullptr, UseLog, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "withLock", nullptr, WithLock, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	auto className = "Database";
	constexpr size_t len = sizeof("Database") - 1;

	napi_ref exportsRef;
	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, exports, 1, &exportsRef));

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
	));

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, ctor));
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
	NAPI_STATUS_THROWS(::napi_get_global(env, &global));

	napi_value result;

	if (status.IsNotFound()) {
		napi_get_undefined(env, &result);
		NAPI_STATUS_THROWS(::napi_call_function(env, global, resolve, 1, &result, nullptr));
	} else if (!status.ok()) {
		ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, errorMsg);
		NAPI_STATUS_THROWS(::napi_call_function(env, global, reject, 1, &error, nullptr));
	} else {
		// TODO: when in "fast" mode, use the shared buffer
		NAPI_STATUS_THROWS(::napi_create_buffer_copy(
			env,
			value.size(),
			value.data(),
			nullptr,
			&result
		));
		NAPI_STATUS_THROWS(::napi_call_function(env, global, resolve, 1, &result, nullptr));
	}

	napi_value returnStatus;
	NAPI_STATUS_THROWS(::napi_create_uint32(env, 0, &returnStatus));
	return returnStatus;
}

} // namespace rocksdb_js
