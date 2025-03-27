#include "db_handle.h"
#include "macros.h"
#include "transaction.h"
#include "util.h"
#include <sstream>

#define UNWRAP_TRANSACTION_HANDLE(fnName) \
	TransactionHandle* handle = nullptr; \
	NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&handle))) \
	if (!handle->txn) { \
		::napi_throw_error(env, nullptr, fnName " failed: Transaction has already been closed"); \
		return nullptr; \
	}

namespace rocksdb_js {

/**
 * Initialize the constructor reference for the `NativeTransaction` class. We
 * need to do this because the constructor is static and we need to access it
 * in the static methods.
 */
napi_ref Transaction::constructor = nullptr;

/**
 * Creates a new `NativeTransaction` object.
 *
 * @param env - The NAPI environment.
 * @param info - The callback info.
 * @returns The new `NativeTransaction` object.
 */
napi_value Transaction::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR_ARGV("Transaction", 1)

	// we need to do some null pointer tricks because we're passing around a shared pointer
	void* ptr = nullptr;
	NAPI_STATUS_THROWS(::napi_get_value_external(env, args[0], &ptr));
	std::shared_ptr<DBHandle> dbHandle = *reinterpret_cast<std::shared_ptr<DBHandle>*>(ptr);

	if (!dbHandle || !dbHandle->opened()) {
		::napi_throw_error(env, nullptr, "Database not open");
		return nullptr;
	}

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(new TransactionHandle(dbHandle)),
			[](napi_env env, void* data, void* hint) {
				auto* ptr = reinterpret_cast<TransactionHandle*>(data);
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

/**
 * Aborts the transaction.
 */
napi_value Transaction::Abort(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_TRANSACTION_HANDLE("Abort")

	ROCKSDB_STATUS_THROWS(handle->txn->Rollback(), "Transaction rollback failed")
	handle->release();

	NAPI_RETURN_UNDEFINED()
}

/**
 * Commits the transaction.
 */
napi_value Transaction::Commit(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_TRANSACTION_HANDLE("Commit")

	// TODO: queue this as async work
	ROCKSDB_STATUS_THROWS(handle->txn->Commit(), "Transaction commit failed")
	handle->release();

	NAPI_RETURN_UNDEFINED()
}

/**
 * Retrieves a value for the given key.
 */
napi_value Transaction::Get(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_TRANSACTION_HANDLE("Get")

	std::string key;
	rocksdb_js::getString(env, argv[0], key);

	auto readOptions = rocksdb::ReadOptions();
	readOptions.snapshot = handle->txn->GetSnapshot();

	auto column = handle->dbHandle->column.get();
	std::string value;

	// TODO: should this be GetForUpdate?
	rocksdb::Status status = handle->txn->Get(
		readOptions,
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
 * Puts a value for the given key.
 */
napi_value Transaction::Put(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	UNWRAP_TRANSACTION_HANDLE("Put")

	std::string key;
	rocksdb_js::getString(env, argv[0], key);

	std::string value;
	rocksdb_js::getString(env, argv[1], value);

	ROCKSDB_STATUS_THROWS(handle->txn->Put(rocksdb::Slice(key), rocksdb::Slice(value)), "Transaction put failed")

	NAPI_RETURN_UNDEFINED()
}

/**
 * Removes a value for the given key.
 */
napi_value Transaction::Remove(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_TRANSACTION_HANDLE("Remove")

	std::string key;
	rocksdb_js::getString(env, argv[0], key);

	ROCKSDB_STATUS_THROWS(handle->txn->Delete(rocksdb::Slice(key)), "Transaction remove failed")

	NAPI_RETURN_UNDEFINED()
}

/**
 * Initializes the `NativeTransaction` JavaScript class.
 */
void Transaction::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "abort", nullptr, Abort, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "commit", nullptr, Commit, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "get", nullptr, Get, nullptr, nullptr, nullptr, napi_default, nullptr },
		// merge?
		{ "put", nullptr, Put, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "remove", nullptr, Remove, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	constexpr auto className = "Transaction";
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

