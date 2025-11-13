#include <sstream>
#include <thread>
#include "database.h"
#include "db_descriptor.h"
#include "db_handle.h"
#include "db_iterator.h"
#include "macros.h"
#include "transaction.h"
#include "transaction_handle.h"
#include "util.h"

#define UNWRAP_TRANSACTION_HANDLE(fnName) \
	std::shared_ptr<TransactionHandle>* txnHandle = nullptr; \
	NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&txnHandle))) \
	if (!txnHandle || !(*txnHandle)) { \
		::napi_throw_error(env, nullptr, fnName " failed: Transaction has already been closed"); \
		return nullptr; \
	}

#define NAPI_THROW_JS_ERROR(code, message) \
	napi_value error; \
	rocksdb_js::createJSError(env, code, message, error); \
	::napi_throw(env, error); \
	return nullptr;

namespace rocksdb_js {

/**
 * Creates a new `NativeTransaction` object.
 *
 * @param env - The NAPI environment.
 * @param info - The callback info.
 * @returns The new `NativeTransaction` object.
 *
 * @example
 * ```typescript
 * const db = RocksDatabase.open('/path/to/database');
 * const txn = new NativeTransaction(db);
 * txn.putSync('key', 'value');
 * await txn.commit();
 * ```
 */
napi_value Transaction::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR_ARGV_WITH_DATA("Transaction", 2)

	napi_ref exportsRef = reinterpret_cast<napi_ref>(data);
	napi_value exports;
	NAPI_STATUS_THROWS(::napi_get_reference_value(env, exportsRef, &exports))

	napi_value databaseCtor;
	bool isDatabase = false;
	NAPI_STATUS_THROWS(::napi_get_named_property(env, exports, "Database", &databaseCtor))
	NAPI_STATUS_THROWS(::napi_instanceof(env, argv[0], databaseCtor, &isDatabase))
	if (!isDatabase) {
		::napi_throw_error(env, nullptr, "Invalid argument, expected Database instance");
		return nullptr;
	}

	std::shared_ptr<DBHandle>* dbHandle = nullptr;
	NAPI_STATUS_THROWS(::napi_unwrap(env, argv[0], reinterpret_cast<void**>(&dbHandle)))

	if (dbHandle == nullptr || !(*dbHandle)->opened()) {
		::napi_throw_error(env, nullptr, "Database not open");
		return nullptr;
	}

	if ((*dbHandle)->descriptor->closing.load()) {
		::napi_throw_error(env, nullptr, "Database is closing!");
		return nullptr;
	}

	bool disableSnapshot = false;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, argv[1], "disableSnapshot", disableSnapshot));

	napi_ref jsDatabaseRef;
	NAPI_STATUS_THROWS(::napi_create_reference(env, argv[0], 0, &jsDatabaseRef))

	// create shared_ptr on heap so it persists after function returns
	std::shared_ptr<TransactionHandle>* txnHandle = new std::shared_ptr<TransactionHandle>(
		std::make_shared<TransactionHandle>(*dbHandle, env, jsDatabaseRef, disableSnapshot)
	);

	(*dbHandle)->descriptor->transactionAdd(*txnHandle);

	DEBUG_LOG(
		"%p Transaction::Constructor Intializing transaction %u (dbHandle=%p, dbDescriptor=%p, use_count=%ld)\n",
		(*txnHandle).get(),
		(*txnHandle)->id,
		(*txnHandle)->dbHandle.get(),
		(*txnHandle)->dbHandle->descriptor.get(),
		(*txnHandle)->dbHandle.use_count()
	)

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(txnHandle),
			[](napi_env env, void* data, void* hint) {
				DEBUG_LOG("Transaction::Constructor NativeTransaction GC'd txnHandle=%p\n", data)
				auto* txnHandle = static_cast<std::shared_ptr<TransactionHandle>*>(data);
				[[maybe_unused]] auto id = (*txnHandle)->id;
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

	TransactionState txnState = (*txnHandle)->state;
	if (txnState == TransactionState::Aborted) {
		// already aborted
		return nullptr;
	}
	if (txnState == TransactionState::Committing || txnState == TransactionState::Committed) {
		NAPI_THROW_JS_ERROR("ERR_ALREADY_COMMITTED", "Transaction has already been committed")
	}
	(*txnHandle)->state = TransactionState::Aborted;

	ROCKSDB_STATUS_THROWS_ERROR_LIKE((*txnHandle)->txn->Rollback(), "Transaction rollback failed")
	DEBUG_LOG("Transaction::Abort closing txnHandle=%p txnId=%u\n", (*txnHandle).get(), (*txnHandle)->id)
	(*txnHandle)->close();

	NAPI_RETURN_UNDEFINED()
}

/**
 * State for the `Commit` async work.
 */
typedef BaseAsyncState<std::shared_ptr<TransactionHandle>> TransactionCommitState;

/**
 * Commits the transaction.
 */
napi_value Transaction::Commit(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	napi_value resolve = argv[0];
	napi_value reject = argv[1];
	UNWRAP_TRANSACTION_HANDLE("Commit")

	TransactionCommitState* state = new TransactionCommitState(env, *txnHandle);
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef))
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef))

	TransactionState txnState = (*txnHandle)->state;
	if (txnState == TransactionState::Aborted) {
		NAPI_THROW_JS_ERROR("ERR_ALREADY_ABORTED", "Transaction has already been aborted")
	}
	if (txnState == TransactionState::Committing || txnState == TransactionState::Committed) {
		// already committed
		napi_value global;
		NAPI_STATUS_THROWS(::napi_get_global(env, &global))
		NAPI_STATUS_THROWS(::napi_call_function(env, global, resolve, 0, nullptr, nullptr))
		delete state;
		return nullptr;
	}
	DEBUG_LOG("%p Transaction::Commit setting state to committing\n", (*txnHandle).get(), (*txnHandle)->id)
	(*txnHandle)->state = TransactionState::Committing;

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(
		env,
		"transaction.commit",
		NAPI_AUTO_LENGTH,
		&name
	))

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,       // node_env
		nullptr,   // async_resource
		name,      // async_resource_name
		[](napi_env doNotUse, void* data) { // execute
			TransactionCommitState* state = reinterpret_cast<TransactionCommitState*>(data);
			auto txnHandle = state->handle;
			if (!txnHandle || !txnHandle->dbHandle || !txnHandle->dbHandle->opened() || txnHandle->dbHandle->isCancelled()) {
				DEBUG_LOG("%p Transaction::Commit called with nullptr txnHandle or dbHandle or dbHandle not opened or dbHandle cancelled\n", txnHandle.get())
				state->status = rocksdb::Status::Aborted("Database closed during transaction commit operation");
			} else {
				auto descriptor = txnHandle->dbHandle->descriptor;

				if (txnHandle->logEntryBatch) {
					DEBUG_LOG("%p Transaction::Commit committing log entries for transaction %u\n",
						txnHandle.get(), txnHandle->id);
					txnHandle->logEntryBatch->entries[0]->store->commit(*txnHandle->logEntryBatch);
				}

				state->status = txnHandle->txn->Commit();
				if (state->status.ok()) {
					DEBUG_LOG("%p Transaction::Commit emitted committed event txnId=%u\n", txnHandle.get(), txnHandle->id)
					txnHandle->state = TransactionState::Committed;
					descriptor->notify("committed", nullptr);
				}
			}
			// signal that execute handler is complete
			state->signalExecuteCompleted();
		},
		[](napi_env env, napi_status status, void* data) { // complete
			TransactionCommitState* state = reinterpret_cast<TransactionCommitState*>(data);

			// only process result if the work wasn't cancelled
			if (status != napi_cancelled) {
				napi_value global;
				NAPI_STATUS_THROWS_VOID(::napi_get_global(env, &global))

				if (state->status.ok()) {
					DEBUG_LOG("%p Transaction::Commit complete closing txnId=%u\n", state->handle.get(), state->handle->id)

					if (state->handle) {
						state->handle->close();
					} else {
						DEBUG_LOG("%p Transaction::Commit complete, but handle is null! txnId=%u\n", state->handle.get(), state->handle->id)
					}

					DEBUG_LOG("%p Transaction::Commit complete calling resolve txnId=%u\n", state->handle.get(), state->handle->id)
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
			}

			delete state;
		},
		state,     // data
		&state->asyncWork // -> result
	));

	// register the async work with the transaction handle
	(*txnHandle)->registerAsyncWork();

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork))

	NAPI_RETURN_UNDEFINED()
}

/**
 * Commits the transaction synchronously.
 */
napi_value Transaction::CommitSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_TRANSACTION_HANDLE("CommitSync")

	TransactionState txnState = (*txnHandle)->state;
	if (txnState == TransactionState::Aborted) {
		NAPI_THROW_JS_ERROR("ERR_ALREADY_ABORTED", "Transaction has already been aborted")
	}
	if (txnState == TransactionState::Committing || txnState == TransactionState::Committed) {
		NAPI_RETURN_UNDEFINED()
	}
	(*txnHandle)->state = TransactionState::Committing;

	if ((*txnHandle)->logEntryBatch) {
		DEBUG_LOG("%p Transaction::CommitSync committing log entries for transaction %u\n",
			(*txnHandle).get(), (*txnHandle)->id);
		(*txnHandle)->logEntryBatch->entries[0]->store->commit(*(*txnHandle)->logEntryBatch);
	}

	rocksdb::Status status = (*txnHandle)->txn->Commit();
	if (status.ok()) {
		DEBUG_LOG("%p Transaction::CommitSync emitted committed event txnId=%u\n", (*txnHandle).get(), (*txnHandle)->id)
		(*txnHandle)->state = TransactionState::Committed;
		(*txnHandle)->dbHandle->descriptor->notify("committed", nullptr);

		DEBUG_LOG("%p Transaction::CommitSync closing txnId=%u\n", (*txnHandle).get(), (*txnHandle)->id)
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
 * Gets the number of keys within a range or in the entire RocksDB database.
 *
 * @example
 * ```typescript
 * const txn = new NativeTransaction(db);
 * const total = txn.getCount();
 * const range = txn.getCount({ start: 'a', end: 'z' });
 * ```
 */
napi_value Transaction::GetCount(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_TRANSACTION_HANDLE("GetCount")

	DBIteratorOptions itOptions;
	itOptions.initFromNapiObject(env, argv[0]);
	itOptions.values = false;

	uint64_t count = 0;
	(*txnHandle)->getCount(itOptions, count);

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_int64(env, count, &result))
	return result;
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
 * Retrieves the timestamp of the transaction.
 */
napi_value Transaction::GetTimestamp(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_TRANSACTION_HANDLE("GetTimestamp")

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_int64(env, (*txnHandle)->startTimestamp, &result))
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

	DEBUG_LOG("%p Transaction::PutSync key:", txnHandle->get())
	DEBUG_LOG_KEY_LN(keySlice)

	DEBUG_LOG("%p Transaction::PutSync value:", txnHandle->get())
	DEBUG_LOG_KEY_LN(valueSlice)

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
 * Sets the timestamp of the transaction.
 */
napi_value Transaction::SetTimestamp(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_TRANSACTION_HANDLE("SetTimestamp")

	int64_t timestamp = 0;
	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[0], &type));

	if (type == napi_undefined) {
		// use current timestamp
		timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::system_clock::now().time_since_epoch()
		).count();
	} else if (type == napi_number) {
		NAPI_STATUS_THROWS(::napi_get_value_int64(env, argv[0], &timestamp));
		if (timestamp < 0) {
			::napi_throw_error(env, nullptr, "Invalid timestamp, expected positive number");
			return nullptr;
		}
	} else {
		::napi_throw_error(env, nullptr, "Invalid timestamp, expected positive number");
		return nullptr;
	}

	(*txnHandle)->startTimestamp = timestamp;

	NAPI_RETURN_UNDEFINED()
}

/**
 * Creates a new transaction log instance bound to this transaction.
 */
napi_value Transaction::UseLog(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	NAPI_GET_STRING(argv[0], name, "Name is required")
	UNWRAP_TRANSACTION_HANDLE("UseLog")

	// check if transaction is already bound to a different log store
	auto boundStore = (*txnHandle)->boundLogStore.lock();
	if (boundStore && boundStore->name != name) {
		::napi_throw_error(env, nullptr, "Log already bound to a transaction");
		return nullptr;
	}

	// resolve the store and bind if not already bound
	auto store = (*txnHandle)->dbHandle->descriptor->resolveTransactionLogStore(name);
	if (!boundStore) {
		(*txnHandle)->boundLogStore = store;
		DEBUG_LOG("%p Transaction::UseLog Binding transaction %u to log store \"%s\"\n",
			(*txnHandle).get(), (*txnHandle)->id, name.c_str());
	}

	// this needs to create a new TransactionLog instance that is not tracked by
	// the DBHandle and is bound to this transaction
	napi_value exports;
	NAPI_STATUS_THROWS(::napi_get_reference_value(env, (*txnHandle)->dbHandle->exportsRef, &exports))

	napi_value transactionLogCtor;
	NAPI_STATUS_THROWS(::napi_get_named_property(env, exports, "TransactionLog", &transactionLogCtor))

	napi_value jsDatabase;
	NAPI_STATUS_THROWS(::napi_get_reference_value(env, (*txnHandle)->jsDatabaseRef, &jsDatabase))

	napi_value args[2];
	args[0] = jsDatabase;

	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, name.c_str(), name.size(), &args[1]))

	napi_value instance;
	NAPI_STATUS_THROWS(::napi_new_instance(env, transactionLogCtor, 2, args, &instance))

	return instance;
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
		{ "getCount", nullptr, GetCount, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getSync", nullptr, GetSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getTimestamp", nullptr, GetTimestamp, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "id", nullptr, nullptr, Id, nullptr, nullptr, napi_default, nullptr },
		{ "putSync", nullptr, PutSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "removeSync", nullptr, RemoveSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "setTimestamp", nullptr, SetTimestamp, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "useLog", nullptr, UseLog, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	auto className = "Transaction";
	constexpr size_t len = sizeof("Transaction") - 1;

	napi_ref exportsRef;
	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, exports, 1, &exportsRef))

	napi_value ctor;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		className,                // className
		len,                      // length of class name
		Transaction::Constructor, // constructor
		(void*)exportsRef,        // constructor arg
		sizeof(properties) / sizeof(napi_property_descriptor), // number of properties
		properties,               // properties array
		&ctor                     // [out] constructor
	))

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, ctor))
}

} // namespace rocksdb_js

