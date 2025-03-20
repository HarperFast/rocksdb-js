#include "database.h"
#include "db_registry.h"
#include "macros.h"
#include "transaction.h"
#include "util.h"
#include <thread>
#include <node_api.h>

namespace rocksdb_js {

napi_value Database::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR("Database")

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(new RocksDBHandle()),
			[](napi_env env, void* data, void* hint) { // finalize_cb
				RocksDBHandle* ptr = reinterpret_cast<RocksDBHandle*>(data);
				delete ptr;
			},
			nullptr, // finalize_hint
			nullptr  // result
		));

		return jsThis;
	} catch (const std::exception& e) {
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}
}

napi_value Database::Close(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB_HANDLE()

	if (dbHandle != nullptr) {
		dbHandle->close();
		dbHandle = nullptr;
	}
	
	NAPI_RETURN_UNDEFINED()
}

napi_value Database::CreateTransaction(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB_HANDLE()

	napi_value constructor;
    NAPI_STATUS_THROWS(::napi_get_reference_value(env, rocksdb_js::Transaction::constructor, &constructor))

	// TODO: pass in the dbHandle?
	napi_value txn;
	NAPI_STATUS_THROWS(::napi_new_instance(env, constructor, 0, nullptr, &txn))

	return txn;
}

napi_value Database::Get(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_DB_HANDLE_AND_OPEN()

	std::string key;
	rocksdb_js::getString(env, argv[0], key);

	std::string value;
	auto column = dbHandle->column.get();
	rocksdb::Status status = dbHandle->db->Get(
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
 * Opens the RocksDB database. This must be called before any data methods are called.
 */
napi_value Database::Open(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	UNWRAP_DB_HANDLE()

	if (dbHandle->opened()) {
		// already open
		NAPI_RETURN_UNDEFINED()
	}

	NAPI_GET_STRING(argv[0], path)
	const napi_value options = argv[1];

	std::string name;
	rocksdb_js::getProperty(env, options, "name", name);

	int parallelism = std::max<int>(1, std::thread::hardware_concurrency() / 2);
	rocksdb_js::getProperty(env, options, "parallelism", parallelism);

	DBOptions dbHandleOptions { name, parallelism };
	dbHandle->open(path, dbHandleOptions);

	NAPI_RETURN_UNDEFINED()
}

napi_value Database::IsOpen(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB_HANDLE()

	napi_value result;
	NAPI_STATUS_THROWS(::napi_get_boolean(env, dbHandle->opened(), &result))
	return result;
}

napi_value Database::Put(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	NAPI_GET_STRING(argv[0], key)
	NAPI_GET_STRING(argv[1], value)
	UNWRAP_DB_HANDLE_AND_OPEN()

	rocksdb::Status status = dbHandle->db->Put(
		rocksdb::WriteOptions(),
		dbHandle->column.get(),
		rocksdb::Slice(key),
		value
	);
	if (!status.ok()) {
		::napi_throw_error(env, nullptr, status.ToString().c_str());
	}

	NAPI_RETURN_UNDEFINED()
}

napi_value Database::Remove(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	NAPI_GET_STRING(argv[0], key)
	UNWRAP_DB_HANDLE_AND_OPEN()

	rocksdb::Status status = dbHandle->db->Delete(
		rocksdb::WriteOptions(),
		dbHandle->column.get(),
		rocksdb::Slice(key)
	);
	if (!status.ok()) {
		::napi_throw_error(env, nullptr, status.ToString().c_str());
	}

	NAPI_RETURN_UNDEFINED()
}

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