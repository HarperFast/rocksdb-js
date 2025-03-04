#include "dbi.h"
#include "dbi_wrap.h"
#include "macros.h"
#include "registry.h"
#include "util.h"
#include <thread>
#include <node_api.h>

namespace rocksdb_js::dbi_wrap {

napi_value close(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DBI()

	if (dbi != nullptr) {
		dbi->close();
		dbi = nullptr;
	}
	
	NAPI_RETURN_UNDEFINED()
}

napi_value get(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_DBI_OPEN()

	std::string key;
	rocksdb_js::getString(env, argv[0], key);

	std::string value;
	rocksdb::Status status = dbi->db->Get(
		rocksdb::ReadOptions(),
		dbi->column.get(),
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
	UNWRAP_DBI()

	if (dbi->opened()) {
		// already open
		NAPI_RETURN_UNDEFINED()
	}

	NAPI_GET_STRING(argv[0], path)
	const napi_value options = argv[1];

	std::string name;
	rocksdb_js::getProperty(env, options, "name", name);

	int parallelism = std::max<int>(1, std::thread::hardware_concurrency() / 2);
	rocksdb_js::getProperty(env, options, "parallelism", parallelism);

	DBOptions dbiOptions { name, parallelism };
	dbi->open(path, dbiOptions);

	NAPI_RETURN_UNDEFINED()
}

napi_value opened(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DBI()

	napi_value result;
	NAPI_STATUS_THROWS(::napi_get_boolean(env, dbi->opened(), &result))
	return result;
}

napi_value put(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	NAPI_GET_STRING(argv[0], key)
	NAPI_GET_STRING(argv[1], value)
	UNWRAP_DBI_OPEN()

	rocksdb::Status status = dbi->db->Put(
		rocksdb::WriteOptions(),
		dbi->column.get(),
		rocksdb::Slice(key),
		value
	);
	if (!status.ok()) {
		::napi_throw_error(env, nullptr, status.ToString().c_str());
	}

	NAPI_RETURN_UNDEFINED()
}

void init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "close", nullptr, close, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "get", nullptr, get, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "open", nullptr, open, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "opened", nullptr, nullptr, opened, nullptr, nullptr, napi_default, nullptr },
		{ "put", nullptr, put, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	constexpr auto className = "DBI";
	napi_value cons;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		className,                  // className
		sizeof(className) - 1,      // length of class name (subtract 1 for null terminator)
		[](napi_env env, napi_callback_info info) -> napi_value {
			// constructor
			NAPI_METHOD()
			try {
				NAPI_STATUS_THROWS(::napi_wrap(
					env,
					jsThis,
					reinterpret_cast<void*>(new DBI()),
					[](napi_env env, void* data, void* hint) {
						DBI* ptr = reinterpret_cast<DBI*>(data);
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

} // namespace rocksdb_js::dbi_wrap