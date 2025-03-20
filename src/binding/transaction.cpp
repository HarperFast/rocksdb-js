#include "db_registry.h"
#include "macros.h"
#include "transaction.h"
#include "util.h"

namespace rocksdb_js {

napi_ref Transaction::constructor = nullptr;

napi_value Transaction::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR("Transaction")

	// TODO: TransactionHandle needs the dbHandle

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(new TransactionHandle()),
			[](napi_env env, void* data, void* hint) {
				TransactionHandle* ptr = reinterpret_cast<TransactionHandle*>(data);
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
}

napi_value Transaction::Abort(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB_HANDLE_AND_OPEN()

	// TODO

	NAPI_RETURN_UNDEFINED()
}

napi_value Transaction::Commit(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB_HANDLE_AND_OPEN()

	// TODO

	NAPI_RETURN_UNDEFINED()
}

napi_value Transaction::Get(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_DB_HANDLE_AND_OPEN()

	TransactionHandle* handle = nullptr; \
    NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&handle)))

	std::string key;
	rocksdb_js::getString(env, argv[0], key);

	std::string value;
	auto column = dbHandle->column.get();
	rocksdb::Status status = handle->txn->Get(
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

napi_value Transaction::Put(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	UNWRAP_DB_HANDLE_AND_OPEN()

	// TODO

	NAPI_RETURN_UNDEFINED()
}

napi_value Transaction::Remove(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_DB_HANDLE_AND_OPEN()

	// TODO

	NAPI_RETURN_UNDEFINED()
}

void Transaction::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "abort", nullptr, Abort, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "commit", nullptr, Commit, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "get", nullptr, Get, nullptr, nullptr, nullptr, napi_default, nullptr },
		// merge?
		{ "put", nullptr, Put, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "remove", nullptr, Remove, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	constexpr auto className = "Txn";
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

	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, cons, 1, &constructor))

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, cons))
}

} // namespace rocksdb_js

