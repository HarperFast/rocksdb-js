#include <node_api.h>
#include <algorithm>
#include <climits>
#include <cstring>
#include <new>
#include "database/database.h"
#include "database/db_descriptor.h"
#include "database/db_handle.h"
#include "database/multi_get.h"
#include "napi/helpers.h"
#include "napi/macros.h"
#include "transaction/transaction_handle.h"

namespace rocksdb_js {

bool MultiGetBatch::copyKeys(const std::vector<rocksdb::Slice>& source) noexcept {
	if (source.empty()) {
		return true;
	}
	const char* base = source.front().data();
	const char* end = source.back().data() + source.back().size();
	try {
		this->keyData.assign(base, static_cast<size_t>(end - base));
		this->keys.reserve(source.size());
	} catch (const std::bad_alloc&) {
		return false;
	}
	for (const auto& key : source) {
		this->keys.emplace_back(this->keyData.data() + (key.data() - base), key.size());
	}
	return true;
}

bool MultiGetBatch::allocateResults() noexcept {
	const size_t count = this->keys.size();
	try {
		this->values.reset(new rocksdb::PinnableSlice[count]);
		this->statuses.reset(new rocksdb::Status[count]);
	} catch (const std::bad_alloc&) {
		return false;
	}
	return true;
}

// A value's length travels as an int32, so the whole result is bounded the same way.
static const size_t MAX_RESULT_SIZE = static_cast<size_t>(INT32_MAX);

size_t MultiGetBatch::findFailure(size_t& resultSize) const {
	const size_t count = this->size();
	// Saturates just past the cap rather than wrapping, so the callers' cap check holds on any
	// size_t width (the same pinned value can be requested many times).
	resultSize = count > MAX_RESULT_SIZE / sizeof(int32_t) ? MAX_RESULT_SIZE + 1 : count * sizeof(int32_t);
	for (size_t i = 0; i < count; i++) {
		const rocksdb::Status& status = this->statuses[i];
		if (status.ok()) {
			const size_t size = this->values[i].size();
			resultSize = size > MAX_RESULT_SIZE - std::min(resultSize, MAX_RESULT_SIZE)
				? MAX_RESULT_SIZE + 1
				: resultSize + size;
		} else if (!status.IsNotFound()) {
			return i;
		}
	}
	return count;
}

void MultiGetBatch::writeResult(char* target) const {
	const size_t count = this->size();
	size_t offset = count * sizeof(int32_t);
	for (size_t i = 0; i < count; i++) {
		int32_t length = -1;
		if (this->statuses[i].ok()) {
			const size_t size = this->values[i].size();
			::memcpy(target + offset, this->values[i].data(), size);
			offset += size;
			length = static_cast<int32_t>(size);
		}
		::memcpy(target + i * sizeof(int32_t), &length, sizeof(int32_t));
	}
}

bool readMultiGetKeys(
	napi_env env,
	napi_value keysArg,
	napi_value endsArg,
	napi_value countArg,
	std::vector<rocksdb::Slice>& keys
) {
	bool isBuffer = false;
	NAPI_STATUS_THROWS_RVAL(::napi_is_buffer(env, keysArg, &isBuffer), false);
	if (!isBuffer) {
		::napi_throw_type_error(env, nullptr, "Keys must be a buffer");
		return false;
	}
	char* keysData = nullptr;
	size_t keysLength = 0;
	NAPI_STATUS_THROWS_RVAL(::napi_get_buffer_info(env, keysArg, reinterpret_cast<void**>(&keysData), &keysLength), false);

	bool isTypedArray = false;
	NAPI_STATUS_THROWS_RVAL(::napi_is_typedarray(env, endsArg, &isTypedArray), false);
	napi_typedarray_type type = napi_uint8_array;
	size_t endsLength = 0;
	void* endsData = nullptr;
	if (isTypedArray) {
		NAPI_STATUS_THROWS_RVAL(::napi_get_typedarray_info(env, endsArg, &type, &endsLength, &endsData, nullptr, nullptr), false);
	}
	if (!isTypedArray || type != napi_int32_array) {
		::napi_throw_type_error(env, nullptr, "Key offsets must be an Int32Array");
		return false;
	}

	uint32_t count = 0;
	NAPI_STATUS_THROWS_RVAL(::napi_get_value_uint32(env, countArg, &count), false);
	if (count > endsLength) {
		::napi_throw_range_error(env, nullptr, "Key count exceeds the key offsets");
		return false;
	}

	const int32_t* ends = static_cast<const int32_t*>(endsData);
	try {
		keys.reserve(count);
	} catch (const std::bad_alloc&) {
		::napi_throw_error(env, nullptr, "Get many failed: out of memory");
		return false;
	}
	size_t start = 0;
	for (uint32_t i = 0; i < count; i++) {
		const int32_t end = ends[i];
		if (end < 0 || static_cast<size_t>(end) < start || static_cast<size_t>(end) > keysLength) {
			::napi_throw_range_error(env, nullptr, "Invalid key offset");
			return false;
		}
		keys.emplace_back(keysData + start, static_cast<size_t>(end) - start);
		start = static_cast<size_t>(end);
	}
	return true;
}

napi_value multiGetSyncResult(napi_env env, const char* errorMsg, const MultiGetBatch& batch) {
	napi_value result;
	size_t resultSize = 0;
	const size_t failed = batch.findFailure(resultSize);
	if (failed < batch.size()) {
		if (batch.statuses[failed].IsIncomplete()) {
			NAPI_STATUS_THROWS(::napi_create_int32(env, NOT_IN_MEMORY_CACHE_FLAG, &result));
			return result;
		}
		ROCKSDB_STATUS_CREATE_NAPI_ERROR(batch.statuses[failed], errorMsg);
		::napi_throw(env, error);
		return nullptr;
	}
	if (resultSize > MAX_RESULT_SIZE) {
		::napi_throw_range_error(env, nullptr, "Get many failed: values exceed 2 GiB");
		return nullptr;
	}
	void* data = nullptr;
	NAPI_STATUS_THROWS(::napi_create_buffer(env, resultSize, &data, &result));
	batch.writeResult(static_cast<char*>(data));
	return result;
}

void finishMultiGet(rocksdb::Status& status, MultiGetBatch& batch, std::string& result) noexcept {
	size_t resultSize = 0;
	const size_t failed = batch.findFailure(resultSize);
	if (failed < batch.size()) {
		status = batch.statuses[failed];
	} else if (resultSize > MAX_RESULT_SIZE) {
		status = rocksdb::Status::InvalidArgument("values exceed 2 GiB");
	} else {
		try {
			result.resize(resultSize);
			batch.writeResult(result.data());
		} catch (const std::bad_alloc&) {
			status = rocksdb::Status::MemoryLimit("could not allocate the result");
		}
	}
	batch.values.reset();
}

/**
 * Synchronously reads a batch of keys with one RocksDB `MultiGet`. See `readMultiGetKeys()` for the
 * arguments and `multiGetSyncResult()` for the return value. `ONLY_IF_IN_MEMORY_CACHE_FLAG` in
 * `flags` limits the read to memory, as for `getSync()`.
 *
 * @example
 * ```typescript
 * const result = db.getManySync(keysBuffer, keyEnds, count, flags, txnId);
 * ```
 */
napi_value Database::GetManySync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(5);
	UNWRAP_DB_HANDLE_AND_OPEN();
	ACQUIRE_OPERATIONS_LOCK();

	MultiGetBatch batch;
	if (!readMultiGetKeys(env, argv[0], argv[1], argv[2], batch.keys)) {
		return nullptr;
	}
	int32_t flags;
	NAPI_STATUS_THROWS(::napi_get_value_int32(env, argv[3], &flags));

	rocksdb::ReadOptions readOptions;
	if (flags & ONLY_IF_IN_MEMORY_CACHE_FLAG) {
		readOptions.read_tier = rocksdb::kBlockCacheTier;
	}
	if (!batch.allocateResults()) {
		::napi_throw_error(env, nullptr, "Get many failed: out of memory");
		return nullptr;
	}

	napi_valuetype txnIdType;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[4], &txnIdType));
	if (txnIdType == napi_number) {
		uint64_t txnId;
		if (!rocksdb_js::readTransactionId(env, argv[4], txnId)) {
			return nullptr;
		}
		auto txnHandle = getOwnedTransaction(env, *(*dbHandle)->descriptor, txnId, "Get many failed");
		if (!txnHandle) {
			return nullptr;
		}
		rocksdb::Status status = txnHandle->multiGetSync(batch, readOptions, *dbHandle);
		if (!status.ok()) {
			ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, "Get many failed");
			::napi_throw(env, error);
			return nullptr;
		}
	} else {
		(*dbHandle)->descriptor->db->MultiGet(
			readOptions,
			(*dbHandle)->getColumnFamilyHandle(),
			batch.size(),
			batch.keys.data(),
			batch.values.get(),
			batch.statuses.get()
		);
	}

	return multiGetSyncResult(env, "Get many failed", batch);
}

/**
 * Asynchronously reads a batch of keys with one RocksDB `MultiGet` on a worker thread, then calls
 * `resolve` with a buffer in `MultiGetBatch::writeResult()`'s layout. The keys are copied, so the
 * caller may reuse its buffers as soon as this returns.
 *
 * @example
 * ```typescript
 * db.getMany(keysBuffer, keyEnds, count, resolve, reject, txnId);
 * ```
 */
napi_value Database::GetMany(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(6);
	UNWRAP_DB_HANDLE_AND_OPEN();
	ACQUIRE_OPERATIONS_LOCK();

	std::vector<rocksdb::Slice> keys;
	if (!readMultiGetKeys(env, argv[0], argv[1], argv[2], keys)) {
		return nullptr;
	}
	napi_value resolve = argv[3];
	napi_value reject = argv[4];

	napi_valuetype txnIdType;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[5], &txnIdType));
	if (txnIdType == napi_number) {
		uint64_t txnId;
		if (!rocksdb_js::readTransactionId(env, argv[5], txnId)) {
			return nullptr;
		}
		auto txnHandle = getOwnedTransaction(env, *(*dbHandle)->descriptor, txnId, "Get many failed");
		if (!txnHandle) {
			return nullptr;
		}
		return txnHandle->multiGet(env, keys, resolve, reject, *dbHandle);
	}

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_latin1(env, "rocksdb-js.getMany", NAPI_AUTO_LENGTH, &name));

	rocksdb::ReadOptions readOptions;
	auto state = new AsyncMultiGetState<std::shared_ptr<DBHandle>>(env, *dbHandle, readOptions);
	// Not admitted yet, so a setup failure must delete the state without unregistering anything.
	state->completed.store(true);
	PendingAsyncState pendingState(env, state);
	if (!state->batch.copyKeys(keys) || !state->batch.allocateResults()) {
		::napi_throw_error(env, nullptr, "Get many failed: out of memory");
		return nullptr;
	}
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef));
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef));

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,
		nullptr,
		name,
		[](napi_env doNotUse, void* data) { // execute
			auto state = reinterpret_cast<AsyncMultiGetState<std::shared_ptr<DBHandle>>*>(data);
			if (!state->handle || !state->handle->opened() || state->handle->isCancelled()) {
				state->status = rocksdb::Status::Aborted("Database closed during get many operation");
				state->batch.values.reset();
			} else {
				state->handle->descriptor->db->MultiGet(
					state->readOptions,
					state->handle->getColumnFamilyHandle(),
					state->batch.size(),
					state->batch.keys.data(),
					state->batch.values.get(),
					state->batch.statuses.get()
				);
				finishMultiGet(state->status, state->batch, state->result);
			}
			state->signalExecuteCompleted();
		},
		[](napi_env env, napi_status status, void* data) { // complete
			auto state = reinterpret_cast<AsyncMultiGetState<std::shared_ptr<DBHandle>>*>(data);
			state->deleteAsyncWork();
			if (status != napi_cancelled) {
				resolveMultiGetResult(env, "Get many failed", state);
			}
			delete state;
		},
		state,
		&state->asyncWork
	));

	// From here admitAsyncWorkOrReject()/queueAsyncWorkOrReject() own the state's cleanup.
	pendingState.release();
	state->completed.store(false);
	// Balances the unregisterAsyncWork() in signalExecuteCompleted(), so close() waits for this read.
	if (!admitAsyncWorkOrReject(env, (*dbHandle).get(), state, "Database is closing")) {
		NAPI_RETURN_UNDEFINED();
	}
	queueAsyncWorkOrReject(env, state, "Failed to queue get many work");
	NAPI_RETURN_UNDEFINED();
}

} // namespace rocksdb_js
