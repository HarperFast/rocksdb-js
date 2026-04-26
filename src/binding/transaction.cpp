#include "database.h"
#include "db_descriptor.h"
#include "db_handle.h"
#include "db_iterator.h"
#include "db_settings.h"
#include "macros.h"
#include "transaction.h"
#include "transaction_handle.h"
#include "util.h"
#include "verification_table.h"

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

	// THROW_IF_READONLY((*dbHandle)->descriptor, "");

	bool disableSnapshot = false;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, argv[1], "disableSnapshot", disableSnapshot));

	bool coordinatedRetry = false;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, argv[1], "coordinatedRetry", coordinatedRetry));

	napi_ref jsDatabaseRef;
	NAPI_STATUS_THROWS(::napi_create_reference(env, argv[0], 0, &jsDatabaseRef));

	// create shared_ptr on heap so it persists after function returns
	std::shared_ptr<TransactionHandle>* txnHandle = new std::shared_ptr<TransactionHandle>(
		std::make_shared<TransactionHandle>(*dbHandle, env, jsDatabaseRef, disableSnapshot)
	);
	(*txnHandle)->coordinatedRetry = coordinatedRetry;

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
	bool hadLogWrites = (*txnHandle)->committedPosition.logSequenceNumber > 0;

	(*txnHandle)->state = TransactionState::Aborted;

	ROCKSDB_STATUS_THROWS_ERROR_LIKE((*txnHandle)->txn->Rollback(), "Transaction rollback failed");
	DEBUG_LOG("Transaction::Abort closing txnHandle=%p txnId=%u\n", (*txnHandle).get(), (*txnHandle)->id);
	(*txnHandle)->close();

	if (hadLogWrites) {
		NAPI_THROW_JS_ERROR("ERR_TRANSACTION_ABANDONED", "Transaction was abandoned after writing to the transaction log");
	}

	NAPI_RETURN_UNDEFINED();
}

/**
 * Context passed through the RETRY_NOW TSFN; owns the resolve/reject refs.
 * Freed by retryNowFinalize after the TSFN fires.
 */
struct RetryNowContext {
	napi_ref resolveRef;
	napi_ref rejectRef;
};

static void retryNowCallJs(napi_env env, napi_value /*func*/, void* context, void* /*data*/) {
	auto* ctx = reinterpret_cast<RetryNowContext*>(context);
	napi_value global, resolveFn, retryVal;
	::napi_get_global(env, &global);
	::napi_get_reference_value(env, ctx->resolveRef, &resolveFn);
	::napi_create_int32(env, RETRY_NOW_VALUE, &retryVal);
	::napi_call_function(env, global, resolveFn, 1, &retryVal, nullptr);
}

static void retryNowFinalize(napi_env env, void* finalizeData, void* /*hint*/) {
	auto* ctx = reinterpret_cast<RetryNowContext*>(finalizeData);
	if (ctx->resolveRef) ::napi_delete_reference(env, ctx->resolveRef);
	if (ctx->rejectRef) ::napi_delete_reference(env, ctx->rejectRef);
	delete ctx;
}

/**
 * State for the `Commit` async work.
 */
struct TransactionCommitState final : BaseAsyncState<std::shared_ptr<TransactionHandle>> {
	bool hasLog;
	// Slot pointers captured before releaseIntent() for coordinated-retry parking.
	std::vector<std::atomic<uint64_t>*> savedSlots;

	TransactionCommitState(
		napi_env env,
		std::shared_ptr<TransactionHandle> handle
	) :
		BaseAsyncState<std::shared_ptr<TransactionHandle>>(env, handle),
		hasLog(false) {}
};

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
				std::shared_ptr<TransactionLogStore> store = nullptr;

				if (txnHandle->logEntryBatch) {
					DEBUG_LOG("%p Transaction::Commit Committing log entries for transaction %u\n",
						txnHandle.get(), txnHandle->id);
					store = txnHandle->boundLogStore.lock();
					if (store) {
						try {
							// write the batch to the store
							store->writeBatch(*txnHandle->logEntryBatch, txnHandle->committedPosition);
							// free the batch after writing to avoid memory leak
							txnHandle->logEntryBatch.reset();
							state->hasLog = true;
						} catch (const std::exception& e) {
							DEBUG_LOG("%p Transaction::Commit ERROR: writeBatch failed for transaction %u: %s\n", txnHandle.get(), txnHandle->id, e.what());
							state->status = rocksdb::Status::Aborted(e.what());
						}
					} else {
						DEBUG_LOG("%p Transaction::Commit ERROR: Log store not found for transaction %u\n", txnHandle.get(), txnHandle->id);
						state->status = rocksdb::Status::Aborted("Log store not found for transaction");
					}
				}

				// ensure we haven't errored above
				if (state->status.ok()) {
					state->status = txnHandle->txn->Commit();

					// For coordinated retry: save slot pointers before
					// releaseIntent() clears them so the complete callback
					// can park on any new lock installed on those slots.
					if (state->status.IsBusy() && txnHandle->coordinatedRetry) {
						state->savedSlots = txnHandle->lockedVTSlots;
					}

					// Release VT locks that were installed at putSync/removeSync
					// time, regardless of commit outcome (success or IsBusy).
					if (!txnHandle->lockedVTSlots.empty()) {
						txnHandle->releaseIntent();
					}
				}

				if (txnHandle->committedPosition.logSequenceNumber > 0 && !state->status.IsBusy()) {
					if (!store) {
						store = txnHandle->boundLogStore.lock();
					}
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
					DEBUG_LOG("%p Transaction::Commit ERROR: Commit failed with IsBusy, resetting transaction\n", txnHandle.get());
					// clear/delete the previous transaction and create a new transaction so that it can be retried
					txnHandle->resetTransaction();
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
				} else if (
					state->status.IsBusy() &&
					state->handle &&
					state->handle->coordinatedRetry
				) {
					// Coordinated-retry path: signal RETRY_NOW to JS instead of
					// rejecting. The native layer may park on an active VT lock
					// before firing the resolve, so JS retries only after the
					// conflicting transaction has released its write intent.
					if (state->handle->state == TransactionState::Committing) {
						state->handle->state = TransactionState::Pending;
					}

					// Transfer resolve/reject refs from state to a RetryNowContext
					// so the TSFN finalize can clean them up.
					auto* ctx = new RetryNowContext{state->resolveRef, state->rejectRef};
					state->resolveRef = nullptr;
					state->rejectRef = nullptr;

					bool parked = false;
					for (auto* slot : state->savedSlots) {
						uint64_t v = slot->load(std::memory_order_acquire);
						if (!vtIsLock(v)) continue;

						LockTracker* t = vtDecodeLock(v);
						uint16_t gen = vtGenFromLock(v);

						// Grab a temporary ref to prevent deletion during ABA check.
						t->refcount.fetch_add(1, std::memory_order_acquire);
						uint64_t v2 = slot->load(std::memory_order_acquire);
						if (!vtIsLock(v2) || vtDecodeLock(v2) != t || vtGenFromLock(v2) != gen) {
							if (t->refcount.fetch_sub(1, std::memory_order_release) == 1) delete t;
							continue;
						}

						// Create a TSFN that calls resolve(RETRY_NOW) when fired.
						napi_value resource_name;
						::napi_create_string_latin1(env, "transaction.retry", NAPI_AUTO_LENGTH, &resource_name);
						napi_threadsafe_function tsfn;
						::napi_create_threadsafe_function(
							env, nullptr, nullptr, resource_name,
							0, 1,
							ctx, retryNowFinalize,
							ctx, retryNowCallJs,
							&tsfn
						);
						::napi_unref_threadsafe_function(env, tsfn);

						// Register wake callback; if the tracker already fired wake()
						// before we got here, addWakeCallback returns false and we
						// call+release the TSFN immediately (async on the JS thread).
						bool registered = t->addWakeCallback([tsfn]() {
							::napi_call_threadsafe_function(tsfn, nullptr, napi_tsfn_nonblocking);
							::napi_release_threadsafe_function(tsfn, napi_tsfn_release);
						});
						if (!registered) {
							::napi_call_threadsafe_function(tsfn, nullptr, napi_tsfn_nonblocking);
							::napi_release_threadsafe_function(tsfn, napi_tsfn_release);
						}

						if (t->refcount.fetch_sub(1, std::memory_order_release) == 1) delete t;
						parked = true;
						break;
					}

					if (!parked) {
						// No active lock found; resolve RETRY_NOW directly (we are on
						// the JS thread in this complete callback).
						napi_value global, resolveFn, retryVal;
						::napi_get_global(env, &global);
						::napi_get_reference_value(env, ctx->resolveRef, &resolveFn);
						::napi_create_int32(env, RETRY_NOW_VALUE, &retryVal);
						::napi_call_function(env, global, resolveFn, 1, &retryVal, nullptr);
						::napi_delete_reference(env, ctx->resolveRef);
						::napi_delete_reference(env, ctx->rejectRef);
						delete ctx;
					}
					// If parked, ctx is owned by the TSFN finalize; do not free here.
				} else {
					// Normal error path: reset to Pending so JS can retry.
					// Guard: keep Aborted if close() already set it (DB closing
					// during commit) — don't let Transaction::Abort call Rollback()
					// on a null txn.
					if (state->handle->state == TransactionState::Committing) {
						state->handle->state = TransactionState::Pending;
					}
					napi_value error;
					ROCKSDB_CREATE_ERROR_LIKE_VOID(error, state->status, "Transaction commit failed");
					napi_value hasLogValue;
					napi_status status = ::napi_get_boolean(env, state->hasLog, &hasLogValue);
					if (status == napi_ok) {
						::napi_set_named_property(env, error, "hasLog", hasLogValue);
					}
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

	std::shared_ptr<TransactionLogStore> store = nullptr;
	bool hasLog = false;

	if ((*txnHandle)->logEntryBatch) {
		DEBUG_LOG("%p Transaction::CommitSync Committing log entries for transaction %u\n",
			(*txnHandle).get(), (*txnHandle)->id);
		store = (*txnHandle)->boundLogStore.lock();
		if (store) {
			hasLog = true;
			store->writeBatch(*(*txnHandle)->logEntryBatch, (*txnHandle)->committedPosition);
			// free the batch after writing to avoid memory leak
			(*txnHandle)->logEntryBatch.reset();
		} else {
			DEBUG_LOG("%p Transaction::CommitSync ERROR: Log store not found for transaction %u\n", (*txnHandle).get(), (*txnHandle)->id);
			NAPI_THROW_JS_ERROR("ERR_LOG_STORE_NOT_FOUND", "Log store not found for transaction");
		}
	}

	rocksdb::Status status = (*txnHandle)->txn->Commit();

	if (!(*txnHandle)->lockedVTSlots.empty()) {
		(*txnHandle)->releaseIntent();
	}

	if ((*txnHandle)->committedPosition.logSequenceNumber > 0 && !status.IsBusy()) {
		if (!store) {
			store = (*txnHandle)->boundLogStore.lock();
		}
		if (store) {
			store->commitFinished((*txnHandle)->committedPosition, (*txnHandle)->dbHandle->descriptor->db->GetLatestSequenceNumber());
		} else {
			DEBUG_LOG("%p Transaction::Commit ERROR: Log store not found for transaction, log number: %u id: %u\n", (*txnHandle).get(), (*txnHandle)->committedPosition.logSequenceNumber, (*txnHandle)->id);
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
			DEBUG_LOG("%p Transaction::CommitSync ERROR: Commit failed with IsBusy, resetting transaction\n", (*txnHandle).get());
			// clear/delete the previous transaction and create a new transaction so that it can be retried
			(*txnHandle)->resetTransaction();
		}
		if ((*txnHandle)->state == TransactionState::Committing) {
			(*txnHandle)->state = TransactionState::Pending;
		}
		napi_value error;
		ROCKSDB_CREATE_ERROR_LIKE_VOID(error, status, "Transaction commit failed");
		napi_value hasLogValue;
		NAPI_STATUS_THROWS(::napi_get_boolean(env, hasLog, &hasLogValue));
		NAPI_STATUS_THROWS(::napi_set_named_property(env, error, "hasLog", hasLogValue));
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
	NAPI_METHOD_ARGV(5);
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

	// argv[3]: txnId (ignored — the transaction is self)
	// argv[4]: optional expectedVersion for VT check and populate
	bool hasExpectedVersion = false;
	uint64_t expectedVersion = 0;
	if (argc >= 5) {
		hasExpectedVersion = parseExpectedVersion(env, argv[4], expectedVersion);
	}

	std::atomic<uint64_t>* vtSlot = nullptr;
	if (hasExpectedVersion) {
		vtSlot = vtSlotFor((*txnHandle)->dbHandle, DBSettings::getInstance().getVerificationTableRaw(), keySlice);
	}

	return (*txnHandle)->get(env, key, resolve, reject, nullptr, vtSlot, hasExpectedVersion, expectedVersion);
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
	NAPI_METHOD_ARGV(4);
	UNWRAP_TRANSACTION_HANDLE("GetSync");
	rocksdb::Slice keySlice;
	if (!rocksdb_js::getSliceFromArg(env, argv[0], keySlice, (*txnHandle)->dbHandle->defaultKeyBufferPtr, "Key must be a buffer")) {
		return nullptr;
	}
	int32_t flags;
	NAPI_STATUS_THROWS(::napi_get_value_int32(env, argv[1], &flags));
	// argv[2]: txnId (ignored — the transaction is self)
	// argv[3]: optional expectedVersion for VT check and populate
	bool hasExpectedVersion = false;
	uint64_t expectedVersion = 0;
	if (argc >= 4) {
		hasExpectedVersion = parseExpectedVersion(env, argv[3], expectedVersion);
	}

	bool wantsPopulate = (flags & POPULATE_VERSION_FLAG) != 0;

	std::atomic<uint64_t>* vtSlot = nullptr;
	if (hasExpectedVersion || wantsPopulate) {
		vtSlot = vtSlotFor((*txnHandle)->dbHandle, DBSettings::getInstance().getVerificationTableRaw(), keySlice);
	}

	// VT fast-path: caller-supplied version matches the table → return FRESH
	if (vtSlot && hasExpectedVersion && VerificationTable::verifyVersion(vtSlot, expectedVersion)) {
		napi_value result;
		NAPI_STATUS_THROWS(::napi_create_int32(env, FRESH_VERSION_FLAG, &result));
		return result;
	}

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

	// Soft VT miss: value still carries the expected version → populate + FRESH
	if (hasExpectedVersion && vtSlot) {
		uint64_t extracted = VerificationTable::extractVersionFromValue(value);
		if (extracted == expectedVersion) {
			VerificationTable::populateVersion(vtSlot, expectedVersion);
			NAPI_STATUS_THROWS(::napi_create_int32(env, FRESH_VERSION_FLAG, &result));
			return result;
		}
	}
	// Populate: explicit flag or implicit when checking an expected version
	if (vtSlot && (wantsPopulate || hasExpectedVersion)) {
		uint64_t version = VerificationTable::extractVersionFromValue(value);
		if (version != 0 && !vtIsLock(version)) {
			VerificationTable::populateVersion(vtSlot, version);
		}
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
	// THROW_IF_READONLY((*txnHandle)->dbHandle->descriptor, "Put failed: ");

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
	// THROW_IF_READONLY((*txnHandle)->dbHandle->descriptor, "Remove failed: ");

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
		// Bind under transactionBindMutex so the bind+increment is atomic with
		// respect to tryClose()'s phase-3 check-and-mark-closing sequence.
		// transactionBindMutex is never held during I/O, so this cannot stall
		// the event loop the way holding writeMutex here would.
		std::lock_guard<std::mutex> lock(store->transactionBindMutex);
		if (store->isClosing.load(std::memory_order_relaxed)) {
			::napi_throw_error(env, nullptr, "Transaction log store is closed");
			return nullptr;
		}
		(*txnHandle)->boundLogStore = store;
		store->pendingTransactionCount++;
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

