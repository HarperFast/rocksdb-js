#include "database.h"
#include "db_handle.h"
#include "macros.h"
#include "transaction.h"
#include "util.h"
#include <thread>
#include <node_api.h>
#include <sstream>

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
				delete ptr;
			},
			nullptr, // finalize_hint
			nullptr  // result
		));

		return jsThis;
	} catch (const std::exception& e) {
		delete dbHandle;  // Clean up on error
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}
}

/**
 * Closes the RocksDB database. If this is the last database instance for this
 * given path and column family, it will automatically be removed from the
 * registry.
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
 * Gets a value from the RocksDB database.
 */
napi_value Database::Get(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_DB_HANDLE_AND_OPEN()

	std::string key;
	rocksdb_js::getString(env, argv[0], key);

	std::string value;
	auto column = (*dbHandle)->column.get();

	rocksdb::Status status = (*dbHandle)->db->Get(
		rocksdb::ReadOptions(),
		column,
		rocksdb::Slice(key),
		&value
	);

	if (status.IsNotFound()) {
		NAPI_RETURN_UNDEFINED()
	}

	if (!status.ok()) {
		::napi_throw_error(env, nullptr, status.ToString().c_str());
		return nullptr;
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, value.c_str(), value.size(), &result))

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

	NAPI_GET_STRING(argv[0], path)
	const napi_value options = argv[1];

	std::string name;
	rocksdb_js::getProperty(env, options, "name", name);

	int parallelism = std::max<int>(1, std::thread::hardware_concurrency() / 2);
	rocksdb_js::getProperty(env, options, "parallelism", parallelism);

	std::string modeName;
	rocksdb_js::getProperty(env, options, "mode", modeName);

	DBMode mode = DBMode::Optimistic;
	if (modeName == "pessimistic") {
		mode = DBMode::Pessimistic;
	}

	DBOptions dbHandleOptions { mode, name, parallelism };

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
napi_value Database::Put(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2) // 4
	NAPI_GET_STRING(argv[0], key)
	NAPI_GET_STRING(argv[1], value)
	// napi_value resolve = argv[2];
	// napi_value reject = argv[3];
	UNWRAP_DB_HANDLE_AND_OPEN()

	// TODO: use async worker

	ROCKSDB_STATUS_THROWS((*dbHandle)->db->Put(
		rocksdb::WriteOptions(),
		(*dbHandle)->column.get(),
		rocksdb::Slice(key),
		value
	), "Put failed")

	NAPI_RETURN_UNDEFINED()
}

/**
 * Removes a key from the RocksDB database.
 */
napi_value Database::Remove(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	NAPI_GET_STRING(argv[0], key)
	UNWRAP_DB_HANDLE_AND_OPEN()

	ROCKSDB_STATUS_THROWS((*dbHandle)->db->Delete(
		rocksdb::WriteOptions(),
		(*dbHandle)->column.get(),
		rocksdb::Slice(key)
	), "Remove failed")

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
		{ "open", nullptr, Open, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "opened", nullptr, nullptr, IsOpen, nullptr, nullptr, napi_default, nullptr },
		{ "put", nullptr, Put, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "remove", nullptr, Remove, nullptr, nullptr, nullptr, napi_default, nullptr }
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
