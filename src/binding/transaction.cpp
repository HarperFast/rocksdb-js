#include "database.h"
#include "db_handle.h"
#include "macros.h"
#include "transaction.h"
#include "transaction_handle.h"
#include "util.h"
#include <sstream>

#define UNWRAP_TRANSACTION_HANDLE(fnName) \
	std::shared_ptr<TransactionHandle>* txnHandle = nullptr; \
	NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&txnHandle))) \
	if (!txnHandle || !(*txnHandle)) { \
		::napi_throw_error(env, nullptr, fnName " failed: Transaction has already been closed"); \
		return nullptr; \
	} \

namespace rocksdb_js {

/**
 * Creates a new `NativeTransaction` object.
 *
 * @param env - The NAPI environment.
 * @param info - The callback info.
 * @returns The new `NativeTransaction` object.
 */
napi_value Transaction::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR_ARGV_WITH_DATA("Transaction", 1)

	napi_ref exportsRef = reinterpret_cast<napi_ref>(data);
	napi_value exports;
	NAPI_STATUS_THROWS(::napi_get_reference_value(env, exportsRef, &exports))

	napi_value databaseCtor;
	NAPI_STATUS_THROWS(::napi_get_named_property(env, exports, "Database", &databaseCtor))

	bool isDatabase = false;
	NAPI_STATUS_THROWS(::napi_instanceof(env, args[0], databaseCtor, &isDatabase))

	std::shared_ptr<TransactionHandle>* txnHandle = nullptr;

	if (isDatabase) {
		std::shared_ptr<DBHandle>* dbHandle = nullptr;
		NAPI_STATUS_THROWS(::napi_unwrap(env, args[0], reinterpret_cast<void**>(&dbHandle)))
		DEBUG_LOG("Transaction::Constructor Initializing transaction handle with Database instance (dbHandle=%p, use_count=%zu)\n", (*dbHandle).get(), (*dbHandle).use_count())
		if (dbHandle == nullptr || !(*dbHandle)->opened()) {
			::napi_throw_error(env, nullptr, "Database not open");
			return nullptr;
		}
		txnHandle = new std::shared_ptr<TransactionHandle>(std::make_shared<TransactionHandle>(*dbHandle));
		(*dbHandle)->descriptor->transactionAdd(*txnHandle);
	} else {
		DEBUG_LOG("Transaction::Constructor Using existing transaction handle\n")
		napi_value transactionCtor;
		NAPI_STATUS_THROWS(::napi_get_named_property(env, exports, "Transaction", &transactionCtor))

		bool isTransaction = false;
		NAPI_STATUS_THROWS(::napi_instanceof(env, args[0], transactionCtor, &isTransaction))

		if (isTransaction) {
			DEBUG_LOG("Transaction::Constructor Received Transaction instance\n")
			NAPI_STATUS_THROWS(::napi_unwrap(env, args[0], reinterpret_cast<void**>(&txnHandle)))
		} else {
			napi_valuetype type;
			NAPI_STATUS_THROWS(::napi_typeof(env, args[0], &type))
			std::string errorMsg = "Invalid context, expected Database or Transaction instance, got type " + std::to_string(type);
			::napi_throw_error(env, nullptr, errorMsg.c_str());
			return nullptr;
		}
	}

	DEBUG_LOG("Transaction::Constructor txnHandle=%p *txnHandle=%p dbHandle use_count=%zu\n", txnHandle, (*txnHandle).get(), (*txnHandle)->dbHandle.use_count())

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(txnHandle),
			[](napi_env env, void* data, void* hint) {
				DEBUG_LOG("Transaction::Constructor NativeTransaction GC'd txnHandle=%p\n", data)
				auto* txnHandle = static_cast<std::shared_ptr<TransactionHandle>*>(data);
				if (*txnHandle) {
					(*txnHandle).reset();
				}
				delete txnHandle;
			},
			nullptr, // finalize_hint
			nullptr  // result
		));

		return jsThis;
	} catch (const std::exception& e) {
		delete txnHandle;
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

	ROCKSDB_STATUS_THROWS_ERROR_LIKE((*txnHandle)->txn->Rollback(), "Transaction rollback failed")
	DEBUG_LOG("Transaction::Abort closing txnHandle=%p\n", (*txnHandle).get())
	(*txnHandle)->close();

	NAPI_RETURN_UNDEFINED()
}

/**
 * State for the `Commit` async work.
 */
struct TransactionCommitState final {
	TransactionCommitState(napi_env env, std::shared_ptr<TransactionHandle> txnHandle)
		: asyncWork(nullptr), resolveRef(nullptr), rejectRef(nullptr), txnHandle(txnHandle) {}

	~TransactionCommitState() = default;

	napi_async_work asyncWork;
	napi_ref resolveRef;
	napi_ref rejectRef;
	std::shared_ptr<TransactionHandle> txnHandle;
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

	TransactionCommitState* state = new TransactionCommitState(env, *txnHandle);
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef))
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef))

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,       // node_env
		nullptr,   // async_resource
		name,      // async_resource_name
		[](napi_env env, void* data) { // execute
			TransactionCommitState* state = reinterpret_cast<TransactionCommitState*>(data);
			state->status = state->txnHandle->txn->Commit();
		},
		[](napi_env env, napi_status status, void* data) { // complete
			TransactionCommitState* state = reinterpret_cast<TransactionCommitState*>(data);

			napi_value global;
			NAPI_STATUS_THROWS_VOID(::napi_get_global(env, &global))

			if (state->status.ok()) {
				DEBUG_LOG("Transaction::Commit complete closing txnHandle=%p\n", state->txnHandle.get())
				state->txnHandle->close();

				DEBUG_LOG("Transaction::Commit complete calling resolve\n")
				napi_value resolve;
				NAPI_STATUS_THROWS_VOID(::napi_get_reference_value(env, state->resolveRef, &resolve))
				NAPI_STATUS_THROWS_VOID(::napi_call_function(env, global, resolve, 0, nullptr, nullptr))
			} else {
				napi_value reject;
				napi_value error;
				NAPI_STATUS_THROWS_VOID(::napi_get_reference_value(env, state->rejectRef, &reject))
				ROCKSDB_CREATE_ERROR_LIKE_VOID(error, state->status, "Transaction commit failed")
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
 * Commits the transaction synchronously.
 */
napi_value Transaction::CommitSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_TRANSACTION_HANDLE("CommitSync")

	rocksdb::Status status = (*txnHandle)->txn->Commit();
	if (status.ok()) {
		DEBUG_LOG("Transaction::CommitSync closing txnHandle=%p\n", (*txnHandle).get())
		(*txnHandle)->close();
	} else {
		napi_value error;
		ROCKSDB_CREATE_ERROR_LIKE_VOID(error, status, "Transaction commit failed")
		NAPI_STATUS_THROWS(::napi_throw(env, error))
	}

	NAPI_RETURN_UNDEFINED()
}

/**
 * Retrieves a value for the given key.
 */
napi_value Transaction::Get(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(3)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	napi_value resolve = argv[1];
	napi_value reject = argv[2];
	UNWRAP_TRANSACTION_HANDLE("Get")

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);
	return (*txnHandle)->get(env, keySlice, resolve, reject);
}

/**
 * Retrieves a value for the given key.
 */
napi_value Transaction::GetSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	UNWRAP_TRANSACTION_HANDLE("GetSync")

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);
	std::string value;
	rocksdb::Status status = (*txnHandle)->getSync(keySlice, value);

	if (status.IsNotFound()) {
		NAPI_RETURN_UNDEFINED()
	}

	if (!status.ok()) {
		::napi_throw_error(env, nullptr, status.ToString().c_str());
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
 * Retrieves the ID of the transaction.
 */
napi_value Transaction::Id(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_TRANSACTION_HANDLE("Id")

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_uint32(
		env,
		(*txnHandle)->id,
		&result
	))
	return result;
}

/**
 * Puts a value for the given key.
 */
napi_value Transaction::PutSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	NAPI_GET_BUFFER(argv[1], value, nullptr)
	UNWRAP_TRANSACTION_HANDLE("Put")

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);
	rocksdb::Slice valueSlice(value + valueStart, valueEnd - valueStart);

	ROCKSDB_STATUS_THROWS_ERROR_LIKE((*txnHandle)->putSync(keySlice, valueSlice), "Transaction put failed")

	NAPI_RETURN_UNDEFINED()
}

/**
 * Removes a value for the given key.
 */
napi_value Transaction::RemoveSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	UNWRAP_TRANSACTION_HANDLE("Remove")

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);

	ROCKSDB_STATUS_THROWS_ERROR_LIKE((*txnHandle)->removeSync(keySlice), "Transaction remove failed")

	NAPI_RETURN_UNDEFINED()
}

/**
 * Initializes the `NativeTransaction` JavaScript class.
 */
void Transaction::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "abort", nullptr, Abort, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "commit", nullptr, Commit, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "commitSync", nullptr, CommitSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "get", nullptr, Get, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getSync", nullptr, GetSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "id", nullptr, nullptr, Id, nullptr, nullptr, napi_default, nullptr },
		// merge?
		{ "putSync", nullptr, PutSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "removeSync", nullptr, RemoveSync, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	auto className = "Transaction";
	constexpr size_t len = sizeof("Transaction") - 1;

	napi_ref exportsRef;
	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, exports, 1, &exportsRef))

	napi_value ctor;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		className,         // className
		len,               // length of class name
		Constructor,       // constructor
		(void*)exportsRef, // constructor arg
		sizeof(properties) / sizeof(napi_property_descriptor), // number of properties
		properties,        // properties array
		&ctor              // [out] constructor
	))

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, ctor))
}

} // namespace rocksdb_js

