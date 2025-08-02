#include <node_api.h>
#include <sstream>
#include <thread>
#include "database.h"
#include "db_handle.h"
#include "db_iterator.h"
#include "db_iterator_handle.h"
#include "macros.h"
#include "transaction.h"
#include "util.h"

#define UNWRAP_DB_HANDLE() \
    std::shared_ptr<DBHandle>* dbHandle = nullptr; \
    NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&dbHandle)))

#define UNWRAP_DB_HANDLE_AND_OPEN() \
    UNWRAP_DB_HANDLE() \
    if (dbHandle == nullptr || !(*dbHandle)->opened()) { \
		::napi_throw_error(env, nullptr, "Database not open"); \
		NAPI_RETURN_UNDEFINED() \
	}

namespace rocksdb_js {

/**
 * Creates a new `NativeDatabase` JavaScript object containing an database
 * handle to an unopened RocksDB database.
 *
 * @example
 * ```ts
 * const db = new NativeDatabase();
 * ```
 */
napi_value Database::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR("Database")

	// create shared_ptr on heap so it persists after function returns
	auto* dbHandle = new std::shared_ptr<DBHandle>(std::make_shared<DBHandle>());

	DEBUG_LOG("%p Database::Constructor Creating NativeDatabase use_count=%zu\n", dbHandle->get(), dbHandle->use_count())

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(dbHandle),
			[](napi_env env, void* data, void* hint) {
				DEBUG_LOG("Database::Constructor NativeDatabase GC'd dbHandle=%p\n", data)
				auto* dbHandle = static_cast<std::shared_ptr<DBHandle>*>(data);
				if (*dbHandle) {
					(*dbHandle).reset();
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
 * Closes the RocksDB database. If this is the last database instance for this
 * given path and column family, it will automatically be removed from the
 * registry.
 *
 * @example
 * ```ts
 * const db = new NativeDatabase();
 * db.close();
 * ```
 */
napi_value Database::Close(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB_HANDLE()

	if (*dbHandle) {
		DEBUG_LOG("%p Database::Close() closing database\n", dbHandle->get())
		(*dbHandle)->close();
	}

	NAPI_RETURN_UNDEFINED()
}

/**
 * Gets a value from the RocksDB database.
 *
 * @example
 * ```ts
 * const db = new NativeDatabase();
 * const value = await db.get('foo');
 * ```
 *
 * @example
 * ```ts
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
			::napi_throw_error(env, nullptr, "Transaction not found");
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
	auto state = new GetState<std::shared_ptr<DBHandle>>(env, *dbHandle, readOptions, keySlice);
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef))
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef))

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,       // node_env
		nullptr,   // async_resource
		name,      // async_resource_name
		[](napi_env env, void* data) { // execute
			auto state = reinterpret_cast<GetState<std::shared_ptr<DBHandle>>*>(data);
			state->status = state->handle->descriptor->db->Get(
				state->readOptions,
				state->handle->column.get(),
				state->keySlice,
				&state->value
			);
		},
		[](napi_env env, napi_status status, void* data) { // complete
			auto state = reinterpret_cast<GetState<std::shared_ptr<DBHandle>>*>(data);
			resolveGetResult(env, "Get failed", state->status, state->value, state->resolveRef, state->rejectRef);
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
 * ```ts
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
			::napi_throw_error(env, nullptr, "Transaction not found");
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

/**
 * Gets the oldest unreleased snapshot unix timestamp.
 *
 * @example
 * ```ts
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
 * ```ts
 * const db = new NativeDatabase();
 * const value = await db.get('foo');
 * ```
 *
 * @example
 * ```ts
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
			::napi_throw_error(env, nullptr, "Transaction not found");
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
 * Checks if the database has a lock on the given key.
 *
 * @example
 * ```ts
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

	std::string name;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "name", name));

	bool noBlockCache = false;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "noBlockCache", noBlockCache));

	int parallelismThreads = std::max<int>(1, std::thread::hardware_concurrency() / 2);
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "parallelismThreads", parallelismThreads));

	std::string modeName;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "mode", modeName));

	DBMode mode = DBMode::Optimistic;
	if (modeName == "pessimistic") {
		mode = DBMode::Pessimistic;
	}

	DBOptions dbHandleOptions { mode, name, noBlockCache, parallelismThreads };

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

#ifdef DEBUG
	fprintf(stderr, "[%04zu] Database::PutSync() Key:", std::hash<std::thread::id>{}(std::this_thread::get_id()) % 10000);
	for (size_t i = 0; i < keySlice.size(); i++) {
		fprintf(stderr, " %02x", (unsigned char)keySlice.data()[i]);
	}
	fprintf(stderr, "\n");
	fprintf(stderr, "[%04zu] Database::PutSync() Value:", std::hash<std::thread::id>{}(std::this_thread::get_id()) % 10000);
	for (size_t i = 0; i < valueSlice.size(); i++) {
		fprintf(stderr, " %02x", (unsigned char)valueSlice.data()[i]);
	}
	fprintf(stderr, "\n");
#endif

	if (txnIdType == napi_number) {
		uint32_t txnId;
		NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[2], &txnId));

		auto txnHandle = (*dbHandle)->descriptor->transactionGet(txnId);
		if (!txnHandle) {
			::napi_throw_error(env, nullptr, "Transaction not found");
			NAPI_RETURN_UNDEFINED()
		}
		status = txnHandle->putSync(
			keySlice,
			valueSlice,
			*dbHandle
		);
	} else {
		status = (*dbHandle)->descriptor->db->Put(
			rocksdb::WriteOptions(),
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
			::napi_throw_error(env, nullptr, "Transaction not found");
			NAPI_RETURN_UNDEFINED()
		}
		status = txnHandle->removeSync(keySlice, *dbHandle);
	} else {
		status = (*dbHandle)->descriptor->db->Delete(
			rocksdb::WriteOptions(),
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
 * ```ts
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
 * ```ts
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
 * Mutually exclusive execution of a function across threads for a given key.
 */
napi_value Database::WithLock(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	UNWRAP_DB_HANDLE_AND_OPEN()

	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[1], &type))
	if (type != napi_function) {
		::napi_throw_error(env, nullptr, "Callback must be a function");
		return nullptr;
	}

	std::string keyStr(key + keyStart, keyEnd - keyStart);
	(*dbHandle)->descriptor->lockCall(env, keyStr, argv[1], *dbHandle);

	NAPI_RETURN_UNDEFINED()
}

/**
 * Initializes the `NativeDatabase` JavaScript class.
 */
void Database::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "close", nullptr, Close, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "get", nullptr, Get, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getCount", nullptr, GetCount, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getOldestSnapshotTimestamp", nullptr, GetOldestSnapshotTimestamp, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getSync", nullptr, GetSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "hasLock", nullptr, HasLock, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "open", nullptr, Open, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "opened", nullptr, nullptr, IsOpen, nullptr, nullptr, napi_default, nullptr },
		{ "putSync", nullptr, PutSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "removeSync", nullptr, RemoveSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "tryLock", nullptr, TryLock, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "unlock", nullptr, Unlock, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "withLock", nullptr, WithLock, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	auto className = "Database";
	constexpr size_t len = sizeof("Database") - 1;

	napi_value ctor;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		className,    // className
		len,          // length of class name
		Constructor,  // constructor
		nullptr,      // constructor arg
		sizeof(properties) / sizeof(napi_property_descriptor), // number of properties
		properties,   // properties array
		&ctor         // [out] constructor
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

/**
 * Resolves the result of a `Get` operation.
 */
void resolveGetResult(
	napi_env env,
	const char* errorMsg,
	rocksdb::Status& status,
	std::string& value,
	napi_ref resolveRef,
	napi_ref rejectRef
) {
	napi_value result;
	napi_value global;
	NAPI_STATUS_THROWS_VOID(::napi_get_global(env, &global))

	if (status.IsNotFound()) {
		napi_get_undefined(env, &result);
		napi_value resolve;
		NAPI_STATUS_THROWS_VOID(::napi_get_reference_value(env, resolveRef, &resolve))
		NAPI_STATUS_THROWS_VOID(::napi_call_function(env, global, resolve, 1, &result, nullptr))
	} else if (!status.ok()) {
		ROCKSDB_STATUS_CREATE_NAPI_ERROR_VOID(status, "Get failed")
		napi_value reject;
		NAPI_STATUS_THROWS_VOID(::napi_get_reference_value(env, rejectRef, &reject))
		NAPI_STATUS_THROWS_VOID(::napi_call_function(env, global, reject, 1, &error, nullptr))
	} else {
		// TODO: when in "fast" mode, use the shared buffer
		NAPI_STATUS_THROWS_VOID(::napi_create_buffer_copy(env, value.size(), value.data(), nullptr, &result))
		napi_value resolve;
		NAPI_STATUS_THROWS_VOID(::napi_get_reference_value(env, resolveRef, &resolve))
		NAPI_STATUS_THROWS_VOID(::napi_call_function(env, global, resolve, 1, &result, nullptr))
	}
}

} // namespace rocksdb_js
