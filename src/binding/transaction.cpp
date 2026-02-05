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
	do { \
		NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&txnHandle))); \
		if (!txnHandle || !(*txnHandle)) { \
			::napi_throw_error(env, nullptr, fnName " failed: Transaction has already been closed"); \
			return nullptr; \
		} \
	} while (0)

#define NAPI_THROW_JS_ERROR(code, message) \
	napi_value error; \
	rocksdb_js::createJSError(env, code, message, error); \
	::napi_throw(env, error); \
	return nullptr

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
	NAPI_CONSTRUCTOR_ARGV_WITH_DATA("Transaction", 2);

	napi_ref exportsRef = reinterpret_cast<napi_ref>(data);
	napi_value exports;
	NAPI_STATUS_THROWS(::napi_get_reference_value(env, exportsRef, &exports));

	napi_value databaseCtor;
	bool isDatabase = false;
	NAPI_STATUS_THROWS(::napi_get_named_property(env, exports, "Database", &databaseCtor));
	NAPI_STATUS_THROWS(::napi_instanceof(env, argv[0], databaseCtor, &isDatabase));
	if (!isDatabase) {
		::napi_throw_error(env, nullptr, "Invalid argument, expected Database instance");
		return nullptr;
	}

	std::shared_ptr<DBHandle>* dbHandle = nullptr;
	NAPI_STATUS_THROWS(::napi_unwrap(env, argv[0], reinterpret_cast<void**>(&dbHandle)));

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
	NAPI_STATUS_THROWS(::napi_create_reference(env, argv[0], 0, &jsDatabaseRef));

	// create shared_ptr on heap so it persists after function returns
	std::shared_ptr<TransactionHandle>* txnHandle = new std::shared_ptr<TransactionHandle>(
		std::make_shared<TransactionHandle>(*dbHandle, env, jsDatabaseRef, disableSnapshot)
	);

	(*dbHandle)->descriptor->transactionAdd(*txnHandle);

	DEBUG_LOG(
		"%p Transaction::Constructor Initializing transaction %u (dbHandle=%p, dbDescriptor=%p, use_count=%ld)\n",
		(*txnHandle).get(),
		(*txnHandle)->id,
		(*txnHandle)->dbHandle.get(),
		(*txnHandle)->dbHandle->descriptor.get(),
		(*txnHandle)->dbHandle.use_count()
	);

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(txnHandle),
			[](napi_env env, void* data, void* hint) {
				auto* txnHandle = static_cast<std::shared_ptr<TransactionHandle>*>(data);
				DEBUG_LOG("Transaction::Constructor NativeTransaction GC'd (txnHandle=%p, ref count=%ld)\n",
					data, txnHandle->use_count());
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
	NAPI_METHOD();
	UNWRAP_TRANSACTION_HANDLE("Abort");

	TransactionState txnState = (*txnHandle)->state;
	if (txnState == TransactionState::Aborted) {
		// already aborted
		return nullptr;
	}
	if (txnState == TransactionState::Committing || txnState == TransactionState::Committed) {
		NAPI_THROW_JS_ERROR("ERR_ALREADY_COMMITTED", "Transaction has already been committed");
	}
	(*txnHandle)->state = TransactionState::Aborted;

	ROCKSDB_STATUS_THROWS_ERROR_LIKE((*txnHandle)->txn->Rollback(), "Transaction rollback failed");
	DEBUG_LOG("Transaction::Abort closing txnHandle=%p txnId=%u\n", (*txnHandle).get(), (*txnHandle)->id);
	(*txnHandle)->close();

	NAPI_RETURN_UNDEFINED();
}

/**
 * State for the `Commit` async work.
 */
typedef BaseAsyncState<std::shared_ptr<TransactionHandle>> TransactionCommitState;

/**
 * Commits the transaction.
 */
napi_value Transaction::Commit(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2);
	napi_value resolve = argv[0];
	napi_value reject = argv[1];
	UNWRAP_TRANSACTION_HANDLE("Commit");

	TransactionCommitState* state = new TransactionCommitState(env, *txnHandle);
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef));
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef));

	TransactionState txnState = (*txnHandle)->state;
	if (txnState == TransactionState::Aborted) {
		NAPI_THROW_JS_ERROR("ERR_ALREADY_ABORTED", "Transaction has already been aborted");
	}
	if (txnState == TransactionState::Committing || txnState == TransactionState::Committed) {
		// already committed
		napi_value global;
		NAPI_STATUS_THROWS(::napi_get_global(env, &global));
		NAPI_STATUS_THROWS(::napi_call_function(env, global, resolve, 0, nullptr, nullptr));
		delete state;
		return nullptr;
	}
	DEBUG_LOG("%p Transaction::Commit Setting state to committing\n", (*txnHandle).get(), (*txnHandle)->id);
	(*txnHandle)->state = TransactionState::Committing;

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(
		env,
		"transaction.commit",
		NAPI_AUTO_LENGTH,
		&name
	));

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,       // node_env
		nullptr,   // async_resource
		name,      // async_resource_name
		[](napi_env doNotUse, void* data) { // execute
			TransactionCommitState* state = reinterpret_cast<TransactionCommitState*>(data);
			auto txnHandle = state->handle;
			if (!txnHandle) {
				DEBUG_LOG("%p Transaction::Commit ERROR: Called with nullptr txnHandle\n", txnHandle.get());
				state->status = rocksdb::Status::Aborted("Database closed during transaction commit operation");
			} else if (txnHandle->isCancelled()) {
				DEBUG_LOG("%p Transaction::Commit ERROR: Called with txnHandle cancelled\n", txnHandle.get());
				state->status = rocksdb::Status::Aborted("Database closed during transaction commit operation");
			} else if (!txnHandle->dbHandle) {
				DEBUG_LOG("%p Transaction::Commit ERROR: Called with nullptr dbHandle\n", txnHandle.get());
				state->status = rocksdb::Status::Aborted("Database closed during transaction commit operation");
			} else if (!txnHandle->dbHandle->opened()) {
				DEBUG_LOG("%p Transaction::Commit ERROR: Called with dbHandle not opened\n", txnHandle.get());
				state->status = rocksdb::Status::Aborted("Database closed during transaction commit operation");
			} else {
				auto descriptor = txnHandle->dbHandle->descriptor;

				if (txnHandle->logEntryBatch) {
					DEBUG_LOG("%p Transaction::Commit Committing log entries for transaction %u\n",
						txnHandle.get(), txnHandle->id);
					auto store = txnHandle->boundLogStore.lock();
					if (store) {
						// write the batch to the store
						store->writeBatch(*txnHandle->logEntryBatch, txnHandle->committedPosition);
					} else {
						DEBUG_LOG("%p Transaction::Commit ERROR: Log store not found for transaction %u\n", txnHandle.get(), txnHandle->id);
						state->status = rocksdb::Status::Aborted("Log store not found for transaction");
					}
				}

				state->status = txnHandle->txn->Commit();
				if (txnHandle->committedPosition.logSequenceNumber > 0 && !state->status.IsBusy()) {
					auto store = txnHandle->boundLogStore.lock();
					if (store) {
						store->commitFinished(txnHandle->committedPosition, descriptor->db->GetLatestSequenceNumber());
					} else {
						DEBUG_LOG("%p Transaction::Commit ERROR: Log store not found for transaction, log number: %u id: %u\n", txnHandle.get(), txnHandle->committedPosition.logSequenceNumber, txnHandle->id);
						state->status = rocksdb::Status::Aborted("Log store not found for transaction");
					}
				}

				if (state->status.ok()) {
					DEBUG_LOG("%p Transaction::Commit Emitted committed event (txnId=%u)\n", txnHandle.get(), txnHandle->id);
					txnHandle->state = TransactionState::Committed;
					descriptor->notify("committed", nullptr);
				} else if (state->status.IsBusy()) {
					// clear/delete the previous transaction and create a new transaction so that it can be retried
					txnHandle->txn->ClearSnapshot();
					delete txnHandle->txn;
					txnHandle->logEntryBatch = nullptr;
					txnHandle->createTransaction();
				}
			}
			// signal that execute handler is complete
			state->signalExecuteCompleted();
		},
		[](napi_env env, napi_status status, void* data) { // complete
			TransactionCommitState* state = reinterpret_cast<TransactionCommitState*>(data);

			DEBUG_LOG("%p Transaction::Commit Complete callback entered (status=%d, txnId=%d)\n",
				state->handle.get(), status, state->handle->id);

			state->deleteAsyncWork();

			// only process result if the work wasn't cancelled
			if (status != napi_cancelled) {
				if (state->status.ok()) {
					if (state->handle) {
						DEBUG_LOG("%p Transaction::Commit Complete closing (txnId=%u)\n", state->handle.get(), state->handle->id);
						state->handle->close();
						DEBUG_LOG("%p Transaction::Commit Complete closed (txnId=%u)\n", state->handle.get(), state->handle->id);
					} else {
						DEBUG_LOG("%p Transaction::Commit Complete, but handle is null! (txnId=%u)\n", state->handle.get(), state->handle->id);
					}

					state->callResolve();
				} else {
					state->handle->state = TransactionState::Pending;
					napi_value error;
					ROCKSDB_CREATE_ERROR_LIKE_VOID(error, state->status, "Transaction commit failed");
					state->callReject(error);
				}
			}

			delete state;
		},
		state,     // data
		&state->asyncWork // -> result
	));

	// register the async work with the transaction handle
	(*txnHandle)->registerAsyncWork();

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork));

	NAPI_RETURN_UNDEFINED();
}

/**
 * Commits the transaction synchronously.
 */
napi_value Transaction::CommitSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_TRANSACTION_HANDLE("CommitSync");

	TransactionState txnState = (*txnHandle)->state;
	if (txnState == TransactionState::Aborted) {
		NAPI_THROW_JS_ERROR("ERR_ALREADY_ABORTED", "Transaction has already been aborted");
	}
	if (txnState == TransactionState::Committing || txnState == TransactionState::Committed) {
		NAPI_RETURN_UNDEFINED();
	}
	(*txnHandle)->state = TransactionState::Committing;

	if ((*txnHandle)->logEntryBatch) {
		DEBUG_LOG("%p Transaction::CommitSync Committing log entries for transaction %u\n",
			(*txnHandle).get(), (*txnHandle)->id);
		auto store = (*txnHandle)->boundLogStore.lock();
		if (store) {
			store->writeBatch(*(*txnHandle)->logEntryBatch, (*txnHandle)->committedPosition);
		} else {
			DEBUG_LOG("%p Transaction::CommitSync ERROR: Log store not found for transaction %u\n", (*txnHandle).get(), (*txnHandle)->id);
			NAPI_THROW_JS_ERROR("ERR_LOG_STORE_NOT_FOUND", "Log store not found for transaction");
		}
	}

	rocksdb::Status status = (*txnHandle)->txn->Commit();

	if ((*txnHandle)->committedPosition.logSequenceNumber > 0 && !status.IsBusy()) {
		auto store = (*txnHandle)->boundLogStore.lock();
		if (store) {
			store->commitFinished((*txnHandle)->committedPosition, (*txnHandle)->dbHandle->descriptor->db->GetLatestSequenceNumber());
		} else {
			DEBUG_LOG("%p Transaction::Commit ERROR: Log store not found for transaction, log number: %u id: %u\n", txnHandle.get(), txnHandle->committedPosition.logSequenceNumber, txnHandle->id);
			status = rocksdb::Status::Aborted("Log store not found for transaction");
		}
	}

	if (status.ok()) {
		DEBUG_LOG("%p Transaction::CommitSync Emitted committed event (txnId=%u)\n", (*txnHandle).get(), (*txnHandle)->id);
		(*txnHandle)->state = TransactionState::Committed;
		(*txnHandle)->dbHandle->descriptor->notify("committed", nullptr);

		DEBUG_LOG("%p Transaction::CommitSync Closing transaction (txnId=%u)\n", (*txnHandle).get(), (*txnHandle)->id);
		(*txnHandle)->close();
	} else {
		if (status.IsBusy()) {
			// clear/delete the previous transaction and create a new transaction so that it can be retried
			(*txnHandle)->txn->ClearSnapshot();
			delete (*txnHandle)->txn;
			(*txnHandle)->logEntryBatch = nullptr;
			(*txnHandle)->createTransaction();
		}
		(*txnHandle)->state = TransactionState::Pending;
		napi_value error;
		ROCKSDB_CREATE_ERROR_LIKE_VOID(error, status, "Transaction commit failed");
		NAPI_STATUS_THROWS(::napi_throw(env, error));
	}

	NAPI_RETURN_UNDEFINED();
}

/**
 * Asynchronously gets a value through the transaction. The first argument, that specifies the key, can be a buffer or a number
 * indicating the length of the key that was written to the shared buffer.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const txn = new NativeTransaction(db);
 * const value = await txn.get('foo');
 * ```
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const txn = new NativeTransaction(db);
 * const b = Buffer.alloc(1024);
 * db.setDefaultKeyBuffer(b);
 * b.utf8Write('foo');
 * const value = await txn.get(3);
 * ```
 */
napi_value Transaction::Get(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(3);
	napi_value resolve = argv[1];
	napi_value reject = argv[2];
	UNWRAP_TRANSACTION_HANDLE("Get");
	UNWRAP_DB_HANDLE_AND_OPEN();
	rocksdb::Slice keySlice;
	if (!rocksdb_js::getSliceFromArg(env, argv[0], keySlice, (*txnHandle)->dbHandle->defaultKeyBufferPtr, "Key must be a buffer")) {
		return nullptr;
	}
	// storing in std::string so it can live through the async process
	std::string key(keySlice.data(), keySlice.size());

	return (*txnHandle)->get(env, key, resolve, reject);
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
	NAPI_METHOD_ARGV(1);
	UNWRAP_TRANSACTION_HANDLE("GetCount");

	DBIteratorOptions itOptions;
	itOptions.initFromNapiObject(env, argv[0]);
	itOptions.values = false;

	uint64_t count = 0;
	(*txnHandle)->getCount(itOptions, count);

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_int64(env, count, &result));
	return result;
}

/**
 * Synchronously gets a value through the transaction. The first argument, that specifies the key, can be a buffer or a number
 * indicating the length of the key that was written to the shared buffer.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const txn = new NativeTransaction(db);
 * const value = txn.getSync('foo');
 * ```
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const txn = new NativeTransaction(db);
 * const b = Buffer.alloc(1024);
 * db.setDefaultKeyBuffer(b);
 * b.utf8Write('foo');
 * const value = txn.getSync(3);
 * ```
 */
napi_value Transaction::GetSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2);
	UNWRAP_TRANSACTION_HANDLE("GetSync");
	rocksdb::Slice keySlice;
	if (!rocksdb_js::getSliceFromArg(env, argv[0], keySlice, (*txnHandle)->dbHandle->defaultKeyBufferPtr, "Key must be a buffer")) {
		return nullptr;
	}
	int32_t flags;
	NAPI_STATUS_THROWS(::napi_get_value_int32(env, argv[1], &flags));
	rocksdb::PinnableSlice value;
	rocksdb::ReadOptions readOptions;
	if (flags & ONLY_IF_IN_MEMORY_CACHE_FLAG) {
		readOptions.read_tier = rocksdb::kBlockCacheTier;
	}
	rocksdb::Status status = (*txnHandle)->getSync(keySlice, value, readOptions);

	if (status.IsNotFound()) {
		NAPI_RETURN_UNDEFINED();
	}

	if (!status.ok()) {
		::napi_throw_error(env, nullptr, status.ToString().c_str());
		return nullptr;
	}

	napi_value result;
	if (status.IsIncomplete()) {
		NAPI_STATUS_THROWS(::napi_create_int32(env, NOT_IN_MEMORY_CACHE_FLAG, &result));
		return result;
	}
	if (!(flags & ALWAYS_CREATE_NEW_BUFFER_FLAG) &&
			(*txnHandle)->dbHandle->defaultValueBufferPtr != nullptr &&
			value.size() <= (*txnHandle)->dbHandle->defaultValueBufferLength) {
		// if it fits in the default value buffer, copy the data and just return the length
		::memcpy((*txnHandle)->dbHandle->defaultValueBufferPtr, value.data(), value.size());
		NAPI_STATUS_THROWS(::napi_create_int32(env, value.size(), &result));
		return result;
	}

	NAPI_STATUS_THROWS(::napi_create_buffer_copy(
		env,
		value.size(),
		value.data(),
		nullptr,
		&result
	));

	return result;
}

/**
 * Retrieves the timestamp of the transaction in milliseconds.
 */
napi_value Transaction::GetTimestamp(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_TRANSACTION_HANDLE("GetTimestamp");

	napi_value result;
	NAPI_STATUS_THROWS_ERROR(::napi_create_double(env, (*txnHandle)->startTimestamp, &result), "Failed to get timestamp");
	return result;
}

/**
 * Retrieves the ID of the transaction.
 */
napi_value Transaction::Id(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_TRANSACTION_HANDLE("Id");

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_uint32(
		env,
		(*txnHandle)->id,
		&result
	));
	return result;
}

/**
 * Puts a value for the given key.
 */
napi_value Transaction::PutSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2);
	NAPI_GET_BUFFER(argv[0], key, "Key is required");
	NAPI_GET_BUFFER(argv[1], value, nullptr);
	UNWRAP_TRANSACTION_HANDLE("Put");

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);
	rocksdb::Slice valueSlice(value + valueStart, valueEnd - valueStart);

	DEBUG_LOG("%p Transaction::PutSync key:", txnHandle->get());
	DEBUG_LOG_KEY_LN(keySlice);

	DEBUG_LOG("%p Transaction::PutSync value:", txnHandle->get());
	DEBUG_LOG_KEY_LN(valueSlice);

	ROCKSDB_STATUS_THROWS_ERROR_LIKE((*txnHandle)->putSync(keySlice, valueSlice), "Transaction put failed");

	NAPI_RETURN_UNDEFINED();
}

/**
 * Removes a value for the given key.
 */
napi_value Transaction::RemoveSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	NAPI_GET_BUFFER(argv[0], key, "Key is required");
	UNWRAP_TRANSACTION_HANDLE("Remove");

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);

	ROCKSDB_STATUS_THROWS_ERROR_LIKE((*txnHandle)->removeSync(keySlice), "Transaction remove failed");

	NAPI_RETURN_UNDEFINED();
}

/**
 * Sets the timestamp of the transaction.
 */
napi_value Transaction::SetTimestamp(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	UNWRAP_TRANSACTION_HANDLE("SetTimestamp");

	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[0], &type));

	if (type == napi_undefined) {
		// use current timestamp
		(*txnHandle)->startTimestamp = rocksdb_js::getMonotonicTimestamp();
	} else if (type == napi_number) {
		double timestampMs = 0.0;
		NAPI_STATUS_THROWS_ERROR(::napi_get_value_double(env, argv[0], &timestampMs),
			"Invalid timestamp, expected positive number");
		if (timestampMs <= 0) {
			::napi_throw_error(env, nullptr, "Invalid timestamp, expected positive number");
			return nullptr;
		}
		(*txnHandle)->startTimestamp = timestampMs;
	} else {
		::napi_throw_error(env, nullptr, "Invalid timestamp, expected positive number");
		return nullptr;
	}

	NAPI_RETURN_UNDEFINED();
}

/**
 * Creates a new transaction log instance bound to this transaction.
 */
napi_value Transaction::UseLog(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	NAPI_GET_STRING(argv[0], name, "Name is required");
	UNWRAP_TRANSACTION_HANDLE("UseLog");

	// check if transaction is already bound to a different log store
	auto boundStore = (*txnHandle)->boundLogStore.lock();
	if (boundStore && boundStore->name != name) {
		::napi_throw_error(env, nullptr, "Log already bound to a transaction");
		return nullptr;
	}

	// resolve the store and bind if not already bound
	std::shared_ptr<TransactionLogStore> store;
	try {
		store = (*txnHandle)->dbHandle->descriptor->resolveTransactionLogStore(name);
	} catch (const std::runtime_error& e) {
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}
	if (!boundStore) {
		(*txnHandle)->boundLogStore = store;
		DEBUG_LOG("%p Transaction::UseLog Binding transaction %u to log store \"%s\"\n",
			(*txnHandle).get(), (*txnHandle)->id, name.c_str());
	}

	// this needs to create a new TransactionLog instance that is not tracked by
	// the DBHandle and is bound to this transaction
	napi_value exports;
	NAPI_STATUS_THROWS_ERROR(::napi_get_reference_value(env, (*txnHandle)->dbHandle->exportsRef, &exports), "Failed to get 'exports' reference");

	napi_value transactionLogCtor;
	NAPI_STATUS_THROWS_ERROR(::napi_get_named_property(env, exports, "TransactionLog", &transactionLogCtor), "Failed to get 'TransactionLog' constructor");

	napi_value jsDatabase;
	NAPI_STATUS_THROWS_ERROR(::napi_get_reference_value(env, (*txnHandle)->jsDatabaseRef, &jsDatabase), "Failed to get 'jsDatabase' reference");

	napi_value args[2];
	args[0] = jsDatabase;

	NAPI_STATUS_THROWS_ERROR(::napi_create_string_utf8(env, name.c_str(), name.size(), &args[1]), "Invalid log name");

	napi_value instance;
	NAPI_STATUS_THROWS_ERROR(::napi_new_instance(env, transactionLogCtor, 2, args, &instance), "Failed to create new TransactionLog instance");

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
	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, exports, 1, &exportsRef));

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
	));

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, ctor));
}

} // namespace rocksdb_js

