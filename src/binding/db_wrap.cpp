#include "db_registry.h"
#include "db_wrap.h"
#include "macros.h"
#include "util.h"
#include <thread>
#include <node_api.h>

namespace rocksdb_js::db_wrap {

napi_value close(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB_HANDLE()

	if (dbHandle != nullptr) {
		dbHandle->close();
		dbHandle = nullptr;
	}
	
	NAPI_RETURN_UNDEFINED()
}

napi_value get(napi_env env, napi_callback_info info) {
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
napi_value open(napi_env env, napi_callback_info info) {
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

napi_value opened(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB_HANDLE()

	napi_value result;
	NAPI_STATUS_THROWS(::napi_get_boolean(env, dbHandle->opened(), &result))
	return result;
}

napi_value put(napi_env env, napi_callback_info info) {
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

napi_value remove(napi_env env, napi_callback_info info) {
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

napi_value transaction(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_DB_HANDLE()

	// create a transaction JS object
	napi_value txn;
	NAPI_STATUS_THROWS(::napi_create_object(env, &txn))

	// call the callback with the transaction JS object
	napi_value callback = argv[0];
	napi_value result;
	NAPI_STATUS_THROWS(::napi_call_function(env, nullptr, callback, 1, &txn, &result))

	return result;
}

/**
 * Initializes the `DB` JavaScript class.
 */
void init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "close", nullptr, close, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "get", nullptr, get, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "open", nullptr, open, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "opened", nullptr, nullptr, opened, nullptr, nullptr, napi_default, nullptr },
		{ "put", nullptr, put, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "remove", nullptr, remove, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "transaction", nullptr, transaction, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	constexpr auto className = "DB";
	napi_value cons;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		className,              // className
		sizeof(className) - 1,  // length of class name (subtract 1 for null terminator)
		[](napi_env env, napi_callback_info info) -> napi_value {
			// constructor
			NAPI_METHOD()
			try {
				NAPI_STATUS_THROWS(::napi_wrap(
					env,
					jsThis,
					reinterpret_cast<void*>(new RocksDBHandle()),
					[](napi_env env, void* data, void* hint) {
						RocksDBHandle* ptr = reinterpret_cast<RocksDBHandle*>(data);
						delete ptr;
					},
					nullptr,
					nullptr
				));

				return jsThis;
			} catch (const std::exception& e) {
				::napi_throw_error(env, nullptr, e.what());
				return nullptr;
			}
		},                      // constructor as lambda
		nullptr,                // constructor arg
		sizeof(properties) / sizeof(napi_property_descriptor), // number of properties
		properties,             // properties array
		&cons                   // [out] constructor
	))

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, cons))
}

} // namespace rocksdb_js::db_wrap