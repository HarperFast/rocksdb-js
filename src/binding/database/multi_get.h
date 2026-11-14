#ifndef __DATABASE_MULTI_GET_H__
#define __DATABASE_MULTI_GET_H__

#include <memory>
#include <node_api.h>
#include <string>
#include <vector>
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "napi/async.h"
#include "napi/helpers.h"
#include "napi/macros.h"

namespace rocksdb_js {

struct ColumnFamilyDescriptor;
struct DBHandle;

/**
 * One `MultiGet` call's keys and results. The sync path points `keys` into the caller's JS buffer;
 * the async path copies them with `copyKeys()` so they outlive the call.
 */
struct MultiGetBatch {
	std::string keyData;
	std::vector<rocksdb::Slice> keys;
	std::unique_ptr<rocksdb::PinnableSlice[]> values;
	std::unique_ptr<rocksdb::Status[]> statuses;

	// Both return false when an allocation fails. `source` must be contiguous, as `readMultiGetKeys()`
	// produces it.
	bool copyKeys(const std::vector<rocksdb::Slice>& source) noexcept;
	bool allocateResults() noexcept;
	size_t size() const { return this->keys.size(); }

	/**
	 * Returns the index of the first key whose read failed (neither found nor not found), or
	 * `size()` when none did, and sets `resultSize` to the bytes `writeResult()` needs, or to just
	 * past `INT32_MAX` when that exceeds the int32 cap.
	 */
	size_t findFailure(size_t& resultSize) const;

	/**
	 * Writes the layout `Store.getMany()` unpacks: one int32 per key, in host byte order, holding
	 * the value's length or -1 when the key was not found, then the found values back to back.
	 */
	void writeResult(char* target) const;
};

/**
 * Reads the key batch `getMany()` passes to native: `keysArg` is a buffer holding the encoded keys
 * back to back and `endsArg` is an `Int32Array` whose first `countArg` entries are each key's end
 * offset in that buffer. The slices point into the JS buffer, so they are only valid for the
 * duration of the call.
 *
 * Returns false with a pending JS exception if the batch is malformed.
 */
bool readMultiGetKeys(
	napi_env env,
	napi_value keysArg,
	napi_value endsArg,
	napi_value countArg,
	std::vector<rocksdb::Slice>& keys
);

/**
 * Returns a sync `MultiGet` result in `MultiGetBatch::writeResult()`'s layout, or
 * `NOT_IN_MEMORY_CACHE_FLAG` if any key could not be read from memory (a `kBlockCacheTier` read).
 * Like `getSync()`, a result that fits is written into `handle`'s default value buffer and its
 * length returned, unless `flags` has `ALWAYS_CREATE_NEW_BUFFER_FLAG`; otherwise it is a new buffer.
 * Throws if any key's read failed.
 */
napi_value multiGetSyncResult(
	napi_env env,
	const char* errorMsg,
	const MultiGetBatch& batch,
	int32_t flags,
	const DBHandle& handle
);

/**
 * State for the async `getMany()` work, for both `DBHandle` and `TransactionHandle`.
 */
template <typename T>
struct AsyncMultiGetState : BaseAsyncState<T> {
	AsyncMultiGetState(napi_env env, T handle, const rocksdb::ReadOptions& readOptions) :
		BaseAsyncState<T>(env, handle),
		readOptions(readOptions) {}

	rocksdb::ReadOptions readOptions;
	MultiGetBatch batch;
	// Filled on the worker by `finishMultiGet()`, in `MultiGetBatch::writeResult()`'s layout.
	std::string result;

	// A transaction belongs to the database rather than a single column family, so the caller's
	// descriptor is pinned until the worker finishes, then released before completion is signaled.
	std::shared_ptr<ColumnFamilyDescriptor> readColumnDescriptor;
};

/**
 * Runs on the worker after `MultiGet`: records the first failed key's status, or writes the result,
 * then releases every pinned value. A `PinnableSlice` can hold a block-cache handle, and the
 * completion runs after `signalExecuteCompleted()` may have let teardown destroy the database that
 * owns it.
 */
void finishMultiGet(rocksdb::Status& status, MultiGetBatch& batch, std::string& result) noexcept;

template <typename T>
void resolveMultiGetResult(napi_env env, const char* errorMsg, AsyncMultiGetState<T>* state) {
	if (!state->status.ok()) {
		ROCKSDB_STATUS_CREATE_NAPI_ERROR_VOID(state->status, errorMsg);
		state->callReject(error);
		return;
	}
	napi_value result;
	if (::napi_create_buffer_copy(env, state->result.size(), state->result.data(), nullptr, &result) != napi_ok) {
		// The promise must still settle (invariant 15). A failed call can leave an exception pending,
		// which createRocksDBError() takes as the error so that callReject() can run.
		napi_value error = nullptr;
		createRocksDBError(env, rocksdb::Status::MemoryLimit("could not allocate the result"), errorMsg, error);
		state->callReject(error);
		return;
	}
	state->callResolve(result);
}

} // namespace rocksdb_js

#endif
