#include <sstream>
#include "transaction_handle.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

/**
 * Creates a new RocksDB transaction, enables snapshots, and sets the
 * transaction id.
 */
TransactionHandle::TransactionHandle(std::shared_ptr<DBHandle> dbHandle) :
	dbHandle(dbHandle),
	txn(nullptr)
{
	if (dbHandle->descriptor->mode == DBMode::Pessimistic) {
		auto* tdb = static_cast<rocksdb::TransactionDB*>(dbHandle->descriptor->db.get());
		rocksdb::TransactionOptions txnOptions;
		this->txn = tdb->BeginTransaction(rocksdb::WriteOptions(), txnOptions);
	} else if (dbHandle->descriptor->mode == DBMode::Optimistic) {
		auto* odb = static_cast<rocksdb::OptimisticTransactionDB*>(dbHandle->descriptor->db.get());
		rocksdb::OptimisticTransactionOptions txnOptions;
		this->txn = odb->BeginTransaction(rocksdb::WriteOptions(), txnOptions);
	} else {
		throw std::runtime_error("Invalid database");
	}
	this->txn->SetSnapshot();
	this->id = this->txn->GetId() & 0xffffffff;
}

/**
 * Destroys the handle's RocksDB transaction.
 */
TransactionHandle::~TransactionHandle() {
	this->release();
}

/**
 * State for the `Get` async work.
 */
struct TransactionGetState final {
	TransactionGetState(
		napi_env env,
		TransactionHandle* txnHandle,
		rocksdb::ReadOptions& readOptions,
		rocksdb::Slice& keySlice
	) :
		asyncWork(nullptr),
		resolveRef(nullptr),
		rejectRef(nullptr),
		txnHandle(txnHandle),
		readOptions(readOptions),
		keySlice(keySlice) {}

	napi_async_work asyncWork;
	napi_ref resolveRef;
	napi_ref rejectRef;
	std::shared_ptr<TransactionHandle> txnHandle;
	rocksdb::ReadOptions readOptions;
	rocksdb::Slice keySlice;
	rocksdb::Status status;
	std::string value;
};

/**
 * Get a value using the specified database handle.
 */
napi_value TransactionHandle::get(
	napi_env env,
	rocksdb::Slice& key,
	napi_value resolve,
	napi_value reject,
	std::shared_ptr<DBHandle> dbHandleOverride
) {
	std::string value;
	napi_value returnStatus;
	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;

	rocksdb::ReadOptions readOptions;
	readOptions.snapshot = this->txn->GetSnapshot();
	readOptions.read_tier = rocksdb::kBlockCacheTier;

	rocksdb::Status status = this->txn->Get(
		readOptions,
		dbHandle->column.get(),
		key,
		&value
	);

	if (!status.IsIncomplete()) {
		// found it in the block cache!
		napi_value result;
		napi_value global;
		NAPI_STATUS_THROWS(::napi_get_global(env, &global))

		if (status.IsNotFound()) {
			napi_get_undefined(env, &result);
			NAPI_STATUS_THROWS(::napi_call_function(env, global, resolve, 1, &result, nullptr))
		} else if (!status.ok()) {
			ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, "Transaction get failed")
			NAPI_STATUS_THROWS(::napi_call_function(env, global, reject, 1, &error, nullptr))
		} else {
			// TODO: when in "fast" mode, use the shared buffer
			NAPI_STATUS_THROWS(::napi_create_buffer_copy(
				env,
				value.size(),
				value.data(),
				nullptr,
				&result
			))
			NAPI_STATUS_THROWS(::napi_call_function(env, global, resolve, 1, &result, nullptr))
		}

		NAPI_STATUS_THROWS(::napi_create_uint32(env, 0, &returnStatus))
		return returnStatus;
	}

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(
		env,
		"transaction.get",
		NAPI_AUTO_LENGTH,
		&name
	))

	readOptions.read_tier = rocksdb::kReadAllTier;
	TransactionGetState* state = new TransactionGetState(env, this, readOptions, key);
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef))
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef))

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,       // node_env
		nullptr,   // async_resource
		name,      // async_resource_name
		[](napi_env env, void* data) { // execute
			TransactionGetState* state = reinterpret_cast<TransactionGetState*>(data);
			state->status = state->txnHandle->txn->Get(
				state->readOptions,
				state->txnHandle->dbHandle->column.get(),
				state->keySlice,
				&state->value
			);
		},
		[](napi_env env, napi_status status, void* data) { // complete
			TransactionGetState* state = reinterpret_cast<TransactionGetState*>(data);

			napi_value global;
			NAPI_STATUS_THROWS_VOID(::napi_get_global(env, &global))

			napi_value result;

			if (state->status.IsNotFound()) {
				napi_get_undefined(env, &result);
				napi_value resolve;
				NAPI_STATUS_THROWS_VOID(::napi_get_reference_value(env, state->resolveRef, &resolve))
				NAPI_STATUS_THROWS_VOID(::napi_call_function(env, global, resolve, 1, &result, nullptr))
			} else if (!state->status.ok()) {
				ROCKSDB_STATUS_CREATE_NAPI_ERROR_VOID(state->status, "Get failed")
				napi_value reject;
				NAPI_STATUS_THROWS_VOID(::napi_get_reference_value(env, state->rejectRef, &reject))
				NAPI_STATUS_THROWS_VOID(::napi_call_function(env, global, reject, 1, &error, nullptr))
			} else {
				// TODO: when in "fast" mode, use the shared buffer
				NAPI_STATUS_THROWS_VOID(::napi_create_buffer_copy(
					env,
					state->value.size(),
					state->value.data(),
					nullptr,
					&result
				))
				napi_value resolve;
				NAPI_STATUS_THROWS_VOID(::napi_get_reference_value(env, state->resolveRef, &resolve))
				NAPI_STATUS_THROWS_VOID(::napi_call_function(env, global, resolve, 1, &result, nullptr))
			}

			NAPI_STATUS_THROWS_VOID(::napi_delete_reference(env, state->resolveRef))
			NAPI_STATUS_THROWS_VOID(::napi_delete_reference(env, state->rejectRef))

			delete state;
		},
		state,     // data
		&state->asyncWork // -> result
	));

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork))

	NAPI_STATUS_THROWS(::napi_create_uint32(env, 1, &returnStatus))
	return returnStatus;
}

/**
 * Get a value using the specified database handle.
 */
rocksdb::Status TransactionHandle::getSync(
	rocksdb::Slice& key,
	std::string& result,
	std::shared_ptr<DBHandle> dbHandleOverride
) {
	auto readOptions = rocksdb::ReadOptions();
	readOptions.snapshot = this->txn->GetSnapshot();

	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
	auto column = dbHandle->column.get();

	// TODO: should this be GetForUpdate?
	return this->txn->Get(readOptions, column, key, &result);
}

/**
 * Put a value using the specified database handle.
 */
rocksdb::Status TransactionHandle::putSync(
	rocksdb::Slice& key,
	rocksdb::Slice& value,
	std::shared_ptr<DBHandle> dbHandleOverride
) {
	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
	auto column = dbHandle->column.get();
	return this->txn->Put(column, key, value);
}

/**
 * Remove a value using the specified database handle.
 */
rocksdb::Status TransactionHandle::removeSync(
	rocksdb::Slice& key,
	std::shared_ptr<DBHandle> dbHandleOverride
) {
	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
	auto column = dbHandle->column.get();
	return this->txn->Delete(column, key);
}

/**
 * Release the transaction. This is called after successful commit, after
 * the transaction has been aborted, or when the transaction is destroyed.
 */
void TransactionHandle::release() {
	if (this->txn) {
		this->txn->ClearSnapshot();
		delete this->txn;
		this->txn = nullptr;
	}
}

} // namespace rocksdb_js
