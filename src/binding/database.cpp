#include <sstream>
#include <node_api.h>
#include <thread>
#include "database.h"
#include "db_handle.h"
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

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(dbHandle),
			[](napi_env env, void* data, void* hint) {
				auto* ptr = static_cast<std::shared_ptr<DBHandle>*>(data);
				ptr->reset();
				delete ptr;
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

	if (*dbHandle != nullptr) {
		(*dbHandle)->close();
	}

	NAPI_RETURN_UNDEFINED()
}

/**
 * Creates a new `NativeTransaction` JavaScript object with the
 * `rocksdb::Transaction` active and ready to go.
 *
 * @example
 * ```ts
 * const db = new NativeDatabase();
 * const txn = db.createTransaction();
 * ```
 */
napi_value Database::CreateTransaction(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB_HANDLE_AND_OPEN()

	napi_value constructor;
	NAPI_STATUS_THROWS(::napi_get_reference_value(env, rocksdb_js::Transaction::constructor, &constructor))

	napi_value args[1];

	// create a new shared_ptr on the heap that shares ownership
	auto* txnDbHandle = new std::shared_ptr<DBHandle>(*dbHandle);

	NAPI_STATUS_THROWS(::napi_create_external(
		env,
		txnDbHandle,
		[](napi_env env, void* data, void* hint) {
			auto* ptr = static_cast<std::shared_ptr<DBHandle>*>(data);
			delete ptr;
		},
		nullptr,
		&args[0]
	))

	napi_value txn;
	NAPI_STATUS_THROWS(::napi_new_instance(env, constructor, 1, args, &txn))

	return txn;
}

/**
 * State for the `Get` async work.
 */
struct GetState final {
	GetState(
		napi_env env,
		std::shared_ptr<DBHandle> dbHandle,
		rocksdb::ReadOptions& readOptions,
		rocksdb::Slice& keySlice
	) :
		env(env),
		asyncWork(nullptr),
		resolveRef(nullptr),
		rejectRef(nullptr),
		dbHandle(dbHandle),
		readOptions(readOptions),
		keySlice(keySlice) {}

	~GetState() {
		NAPI_STATUS_THROWS_VOID(::napi_delete_reference(env, resolveRef))
		NAPI_STATUS_THROWS_VOID(::napi_delete_reference(env, rejectRef))
	}

	napi_env env;
	napi_async_work asyncWork;
	napi_ref resolveRef;
	napi_ref rejectRef;
	std::shared_ptr<DBHandle> dbHandle;
	rocksdb::ReadOptions readOptions;
	rocksdb::Slice keySlice;
	rocksdb::Status status;
	std::string value;
};

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

		auto txnHandle = (*dbHandle)->descriptor->getTransaction(txnId);
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
	NAPI_STATUS_THROWS(::napi_create_string_utf8(
		env,
		"database.get",
		NAPI_AUTO_LENGTH,
		&name
	))

	readOptions.read_tier = rocksdb::kReadAllTier;
	GetState* state = new GetState(env, *dbHandle, readOptions, keySlice);
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef))
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef))

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,       // node_env
		nullptr,   // async_resource
		name,      // async_resource_name
		[](napi_env env, void* data) { // execute
			GetState* state = reinterpret_cast<GetState*>(data);
			state->status = state->dbHandle->descriptor->db->Get(
				state->readOptions,
				state->dbHandle->column.get(),
				state->keySlice,
				&state->value
			);
		},
		[](napi_env env, napi_status status, void* data) { // complete
			GetState* state = reinterpret_cast<GetState*>(data);
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

		auto txnHandle = (*dbHandle)->descriptor->getTransaction(txnId);
		if (!txnHandle) {
			::napi_throw_error(env, nullptr, "Transaction not found");
			NAPI_RETURN_UNDEFINED()
		}
		status = txnHandle->getSync(keySlice, value, *dbHandle);
	} else {
		rocksdb::ReadOptions read_options;
		// TODO: try with kBlockCacheTier, if it fails, then retry with rocksdb::kReadAllTier
		// read_options.read_tier = rocksdb::kBlockCacheTier;

		status = (*dbHandle)->descriptor->db->Get(
			read_options,
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

	if (txnIdType == napi_number) {
		uint32_t txnId;
		NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[2], &txnId));

		auto txnHandle = (*dbHandle)->descriptor->getTransaction(txnId);
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

		auto txnHandle = (*dbHandle)->descriptor->getTransaction(txnId);
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
 * Initializes the `NativeDatabase` JavaScript class.
 */
void Database::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "close", nullptr, Close, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "createTransaction", nullptr, CreateTransaction, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "get", nullptr, Get, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getSync", nullptr, GetSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "open", nullptr, Open, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "opened", nullptr, nullptr, IsOpen, nullptr, nullptr, napi_default, nullptr },
		{ "putSync", nullptr, PutSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "removeSync", nullptr, RemoveSync, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	constexpr auto className = "Database";
	napi_value cons;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		className,              // className
		sizeof(className) - 1,  // length of class name (subtract 1 for null terminator)
		Constructor,            // constructor
		nullptr,                // constructor arg
		sizeof(properties) / sizeof(napi_property_descriptor), // number of properties
		properties,             // properties array
		&cons                   // [out] constructor
	))

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, cons))
}

} // namespace rocksdb_js::db_wrap
