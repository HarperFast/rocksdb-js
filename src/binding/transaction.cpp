#include "db_handle.h"
#include "macros.h"
#include "transaction.h"
#include "txn_registry.h"
#include "util.h"
#include <sstream>

#define UNWRAP_TRANSACTION_HANDLE(fnName) \
	TransactionHandle* txnHandle = nullptr; \
	NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&txnHandle))) \
	if (txnHandle == nullptr || !txnHandle->txn) { \
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

	TransactionHandle* txnHandle = new TransactionHandle(dbHandle);
	TxnRegistry::getInstance()->addTransaction(txnHandle);

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(txnHandle),
			[](napi_env env, void* data, void* hint) {
				TransactionHandle* txnHandle = reinterpret_cast<TransactionHandle*>(data);
				TxnRegistry::getInstance()->removeTransaction(txnHandle);
				delete txnHandle;
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

	ROCKSDB_STATUS_THROWS(txnHandle->txn->Rollback(), "Transaction rollback failed")
	txnHandle->release();

	NAPI_RETURN_UNDEFINED()
}

/**
 * State for the `Commit` async work.
 */
struct TransactionCommitState final {
	TransactionCommitState(napi_env env, TransactionHandle* txnHandle)
		: asyncWork(nullptr), resolveRef(nullptr), rejectRef(nullptr), txnHandle(txnHandle) {}

	napi_async_work asyncWork;
	napi_ref resolveRef;
	napi_ref rejectRef;
	TransactionHandle* txnHandle;
	rocksdb::Status status;
};

/**
 * Commits the transaction.
 */
napi_value Transaction::Commit(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	napi_value resolve = argv[0];
	napi_value reject = argv[1];
	UNWRAP_TRANSACTION_HANDLE("Commit")

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(
		env,
		"transaction.commit",
		NAPI_AUTO_LENGTH,
		&name
	))

	TransactionCommitState* state = new TransactionCommitState(env, txnHandle);
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef))
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef))

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,       // node_env
		nullptr,   // async_resource
		name,      // async_resource_name
		[](napi_env env, void* data) { // execute
			TransactionCommitState* state = reinterpret_cast<TransactionCommitState*>(data);
			state->status = state->txnHandle->txn->Commit();
			if (state->status.ok()) {
				state->txnHandle->release();
			}
		},
		[](napi_env env, napi_status status, void* data) { // complete
			TransactionCommitState* state = reinterpret_cast<TransactionCommitState*>(data);

			napi_value global;
			NAPI_STATUS_THROWS_VOID(::napi_get_global(env, &global))

			if (state->status.ok()) {
				napi_value resolve;
				NAPI_STATUS_THROWS_VOID(::napi_get_reference_value(env, state->resolveRef, &resolve))
				NAPI_STATUS_THROWS_VOID(::napi_call_function(env, global, resolve, 0, nullptr, nullptr))
			} else {
				napi_value reject;
				NAPI_STATUS_THROWS_VOID(::napi_get_reference_value(env, state->rejectRef, &reject))
				ROCKSDB_STATUS_THROW_NAPI_ERROR_VOID(state->status, "Transaction commit failed")
				NAPI_STATUS_THROWS_VOID(::napi_call_function(env, global, reject, 1, &error, nullptr))
			}

			NAPI_STATUS_THROWS_VOID(::napi_delete_reference(env, state->resolveRef))
			NAPI_STATUS_THROWS_VOID(::napi_delete_reference(env, state->rejectRef))

			delete state;
		},
		state,     // data
		&state->asyncWork // -> result
	));

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork))

	NAPI_RETURN_UNDEFINED()
}

/**
 * Retrieves a value for the given key.
 */
napi_value Transaction::Get(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	NAPI_GET_STRING(argv[0], key)
	UNWRAP_TRANSACTION_HANDLE("Get")

	std::string value;
	rocksdb::Status status = txnHandle->get(key, value);

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
 * Retrieves the ID of the transaction.
 */
napi_value Transaction::Id(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_TRANSACTION_HANDLE("Id")

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_uint32(
		env,
		txnHandle->id,
		&result
	))
	return result;
}

/**
 * Puts a value for the given key.
 */
napi_value Transaction::Put(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	NAPI_GET_STRING(argv[0], key)
	NAPI_GET_STRING(argv[1], value)
	UNWRAP_TRANSACTION_HANDLE("Put")

	ROCKSDB_STATUS_THROWS(txnHandle->put(key, value), "Transaction put failed")

	NAPI_RETURN_UNDEFINED()
}

/**
 * Removes a value for the given key.
 */
napi_value Transaction::Remove(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	NAPI_GET_STRING(argv[0], key)
	UNWRAP_TRANSACTION_HANDLE("Remove")

	ROCKSDB_STATUS_THROWS(txnHandle->remove(key), "Transaction remove failed")

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
		{ "id", nullptr, nullptr, Id, nullptr, nullptr, napi_default, nullptr },
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

