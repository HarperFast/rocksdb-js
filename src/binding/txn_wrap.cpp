#include "db_registry.h"
#include "macros.h"
#include "txn.h"
#include "txn_wrap.h"
#include "util.h"

namespace rocksdb_js::txn_wrap {

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

void init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		// abort?
		// commit?
		{ "get", nullptr, get, nullptr, nullptr, nullptr, napi_default, nullptr },
		// merge?
		// { "put", nullptr, put, nullptr, nullptr, nullptr, napi_default, nullptr },
		// { "remove", nullptr, remove, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	constexpr auto className = "Txn";
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
					reinterpret_cast<void*>(new TxnHandle()),
					[](napi_env env, void* data, void* hint) {
						TxnHandle* ptr = reinterpret_cast<TxnHandle*>(data);
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

} // namespace rocksdb_js::txn_wrap

