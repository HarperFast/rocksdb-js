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
	UNWRAP_DB()

	if (dbi != nullptr && dbi->db != nullptr) {
		dbi->db->db->Close();
		dbi->db = nullptr;
	}
	
	NAPI_RETURN_UNDEFINED()
}

napi_value get(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_DB()
	ASSERT_DBI_OPEN(env, dbi)

	std::string key;
	rocksdb_js::getString(env, argv[0], key);

	std::string value;
	rocksdb::Status status = dbi->db->db->Get(rocksdb::ReadOptions(), key, &value);

	if (!status.ok()) {
		::napi_throw_error(env, nullptr, status.ToString().c_str());
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, value.c_str(), value.size(), &result))

	return result;
}

napi_value isOpen(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB()

	if (dbi == nullptr || dbi->db == nullptr) {
		napi_value result;
		NAPI_STATUS_THROWS(::napi_get_boolean(env, false, &result))
		return result;
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_get_boolean(env, dbi->db != nullptr, &result))
	return result;
}

/**
 * Opens the RocksDB database. This must be called before any data methods are called.
 */
napi_value open(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_DB()

	if (dbi->db != nullptr) {
		// already open
		NAPI_RETURN_UNDEFINED()
	}

	const napi_value options = argv[0];

	int parallelism = std::max<int>(1, std::thread::hardware_concurrency() / 2);
	rocksdb_js::getProperty(env, options, "parallelism", parallelism);

	std::string name;
	rocksdb_js::getProperty(env, options, "name", name);

	fprintf(stderr, "open... got options\n");

	NAPI_RETURN_UNDEFINED()
}

napi_value put(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	NAPI_GET_STRING(argv[0], key)
	NAPI_GET_STRING(argv[1], value)
	UNWRAP_DB()
	fprintf(stderr, "put key: %s, value: %s\n", key.c_str(), value.c_str());
	// ASSERT_DBI_OPEN(env, dbi)

	fprintf(stderr, "db is good\n");

	rocksdb::Status status = dbi->db->db->Put(rocksdb::WriteOptions(), key, value);
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
		{ "opened", nullptr, nullptr, isOpen, nullptr, nullptr, napi_default, nullptr },
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
			NAPI_METHOD_ARGV(2)
			NAPI_GET_STRING(argv[0], path)
			NAPI_GET_STRING(argv[1], name)

			try {
				DBI* dbi = Registry::open(path, name);
				
				NAPI_STATUS_THROWS(::napi_wrap(
					env,
					jsThis,
					reinterpret_cast<void*>(dbi),
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