#ifndef __DATABASE_H__
#define __DATABASE_H__

#include <cstring>
#include <node_api.h>
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "database/db_handle.h"
#include "napi/macros.h"
#include "core/platform.h"
#include "napi/helpers.h"
#include "napi/async.h"
#include "core/verification_table.h"

namespace rocksdb_js {

/**
 * Parses an expectedVersion from a napi_value double argument (bit-cast from
 * IEEE 754 to uint64). Returns true and sets `out` when the arg is a valid
 * non-zero, non-lock-tagged version. Returns false and leaves `out` unchanged
 * for undefined/null args, non-numbers, zero, or lock-tagged values.
 */
inline bool parseExpectedVersion(napi_env env, napi_value arg, uint64_t& out) {
	napi_valuetype t;
	if (::napi_typeof(env, arg, &t) != napi_ok || t != napi_number) return false;
	double d;
	if (::napi_get_value_double(env, arg, &d) != napi_ok) return false;
	uint64_t v;
	::memcpy(&v, &d, sizeof(v));
	if (vtIsLock(v) || v == 0) return false;
	out = v;
	return true;
}

/**
 * Returns the verification-table slot for (dbHandle, key), or nullptr if the
 * table pointer is null or the slot maps outside the table bounds.
 */
inline std::atomic<uint64_t>* vtSlotFor(
	const std::shared_ptr<DBHandle>& dbHandle,
	VerificationTable* vt,
	const rocksdb::Slice& key
) {
	if (!vt) return nullptr;
	uintptr_t dbPtr = reinterpret_cast<uintptr_t>(dbHandle->descriptor.get());
	uint32_t cfId = dbHandle->getColumnFamilyHandle()->GetID();
	return vt->slotFor(dbPtr, cfId, key);
}

#define ONLY_IF_IN_MEMORY_CACHE_FLAG 0x40000000
#define NOT_IN_MEMORY_CACHE_FLAG 0x40000000
#define ALWAYS_CREATE_NEW_BUFFER_FLAG 0x20000000
// Set on getSync to opt in to populating the verification table after a
// successful read. Extracts the first 8 bytes of the value as a big-endian
// float64 version (Harper's record-encoder format) and CASes it into the
// slot. Has no effect when no version is found (e.g., not-found, value < 8
// bytes) or when the slot is currently lock-tagged.
#define POPULATE_VERSION_FLAG 0x10000000
// Returned by getSync when the caller-supplied expectedVersion matches the
// verification-table slot for the key. Distinct from NOT_IN_MEMORY_CACHE_FLAG
// and from any byte-length value returned via the default value buffer.
#define FRESH_VERSION_FLAG 0x08000000
// Resolved (not rejected) value for commit() when coordinatedRetry is true
// and the transaction experienced an IsBusy conflict. JS should retry the
// transaction body immediately without any backoff delay.
#define RETRY_NOW_VALUE 0x04000000

#define UNWRAP_DB_HANDLE() \
	std::shared_ptr<DBHandle>* dbHandle = nullptr; \
	NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&dbHandle)))

#define UNWRAP_DB_HANDLE_AND_OPEN() \
	UNWRAP_DB_HANDLE(); \
	do { \
		if (dbHandle == nullptr || !(*dbHandle)->opened()) { \
			::napi_throw_error(env, nullptr, "Database not open"); \
			NAPI_RETURN_UNDEFINED(); \
		} \
	} while (0)

/**
 * RAII guard that tracks in-flight operations on a DBDescriptor.
 * Increments counter on construction, decrements on destruction.
 * Notifies waiters via atomic::notify_all() when count reaches zero.
 */
struct OperationGuard {
	std::shared_ptr<DBDescriptor> descriptor;

	explicit OperationGuard(std::shared_ptr<DBDescriptor> desc) : descriptor(std::move(desc)) {
		if (descriptor) {
			++descriptor->operationsInFlight;
		}
	}

	~OperationGuard() {
		if (descriptor) {
			if (--descriptor->operationsInFlight == 0 && descriptor->isClosing()) {
				descriptor->operationsInFlight.notify_all();
			}
		}
	}

	// Non-copyable, non-movable
	OperationGuard(const OperationGuard&) = delete;
	OperationGuard& operator=(const OperationGuard&) = delete;
	OperationGuard(OperationGuard&&) = delete;
	OperationGuard& operator=(OperationGuard&&) = delete;
};

/**
 * Registers an in-flight operation to prevent use-after-free during shutdown.
 * Also checks if the database is closing and throws an error if so.
 *
 * Use this macro after UNWRAP_DB_HANDLE_AND_OPEN() in operations that
 * access descriptor->db or column family handles.
 *
 * Note: We copy the descriptor shared_ptr first to ensure the descriptor
 * stays alive even if another thread calls close() on our handle.
 */
#define ACQUIRE_OPERATIONS_LOCK() \
	if (!(*dbHandle)->descriptor) { \
		::napi_throw_error(env, nullptr, "Database not open"); \
		NAPI_RETURN_UNDEFINED(); \
	} \
	OperationGuard __operationGuard((*dbHandle)->descriptor); \
	do { \
		if ((*dbHandle)->descriptor->isClosing()) { \
			::napi_throw_error(env, nullptr, "Database is closing"); \
			NAPI_RETURN_UNDEFINED(); \
		} \
	} while (0)

/**
 * The `NativeDatabase` JavaScript class implementation.
 *
 * @example
 * ```js
 * const db = new RocksDatabase();
 * db.open('/tmp/testdb');
 * db.put('foo', 'bar');
 * ```
 */
struct Database final {
	static napi_value Constructor(napi_env env, napi_callback_info info);
	static napi_value AddListener(napi_env env, napi_callback_info info);
	static napi_value Clear(napi_env env, napi_callback_info info);
	static napi_value ClearSync(napi_env env, napi_callback_info info);
	static napi_value Close(napi_env env, napi_callback_info info);
	static napi_value Columns(napi_env env, napi_callback_info info);
	static napi_value Destroy(napi_env env, napi_callback_info info);
	static napi_value Drop(napi_env env, napi_callback_info info);
	static napi_value DropSync(napi_env env, napi_callback_info info);
	static napi_value Compact(napi_env env, napi_callback_info info);
	static napi_value CompactSync(napi_env env, napi_callback_info info);
	static napi_value Flush(napi_env env, napi_callback_info info);
	static napi_value FlushSync(napi_env env, napi_callback_info info);
	static napi_value Get(napi_env env, napi_callback_info info);
	static napi_value GetCount(napi_env env, napi_callback_info info);
	static napi_value GetDBIntProperty(napi_env env, napi_callback_info info);
	static napi_value GetDBProperty(napi_env env, napi_callback_info info);
	static napi_value GetMonotonicTimestamp(napi_env env, napi_callback_info info);
	static napi_value GetOldestSnapshotTimestamp(napi_env env, napi_callback_info info);
	static napi_value GetStat(napi_env env, napi_callback_info info);
	static napi_value GetStats(napi_env env, napi_callback_info info);
	static napi_value GetSync(napi_env env, napi_callback_info info);
	static napi_value GetUserSharedBuffer(napi_env env, napi_callback_info info);
	static napi_value HasLock(napi_env env, napi_callback_info info);
	static napi_value IsOpen(napi_env env, napi_callback_info info);
	static napi_value Listeners(napi_env env, napi_callback_info info);
	static napi_value ListLogs(napi_env env, napi_callback_info info);
	static napi_value Notify(napi_env env, napi_callback_info info);
	static napi_value Open(napi_env env, napi_callback_info info);
	static napi_value PopulateVersion(napi_env env, napi_callback_info info);
	static napi_value PurgeLogs(napi_env env, napi_callback_info info);
	static napi_value PutSync(napi_env env, napi_callback_info info);
	static napi_value RemoveListener(napi_env env, napi_callback_info info);
	static napi_value RemoveSync(napi_env env, napi_callback_info info);
	static napi_value SetDefaultValueBuffer(napi_env env, napi_callback_info info);
	static napi_value SetDefaultKeyBuffer(napi_env env, napi_callback_info info);
	static napi_value SetIteratorState(napi_env env, napi_callback_info info);
	static napi_value TryLock(napi_env env, napi_callback_info info);
	static napi_value Unlock(napi_env env, napi_callback_info info);
	static napi_value UseLog(napi_env env, napi_callback_info info);
	static napi_value VerifyVersion(napi_env env, napi_callback_info info);
	static napi_value WithLock(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);
};

/**
 * State for the `Clear` async work.
 */
struct AsyncClearState final : BaseAsyncState<std::shared_ptr<DBHandle>> {
	const char* failureMsg;
	AsyncClearState(
		napi_env env,
		std::shared_ptr<DBHandle> handle,
		const char* failureMsg = "Clear failed"
	) :
		BaseAsyncState<std::shared_ptr<DBHandle>>(env, handle),
		failureMsg(failureMsg)
	{}
};

/**
 * State for the `CompactRange` async work.
 */
struct AsyncCompactState final : BaseAsyncState<std::shared_ptr<DBHandle>> {
	std::string startKey;
	std::string endKey;
	bool hasStart = false;
	bool hasEnd = false;

	AsyncCompactState(napi_env env, std::shared_ptr<DBHandle> handle)
		: BaseAsyncState<std::shared_ptr<DBHandle>>(env, handle) {}
};


/**
 * State for the `Flush` async work.
 */
struct AsyncFlushState final : BaseAsyncState<std::shared_ptr<DBHandle>> {
	AsyncFlushState(
		napi_env env,
		std::shared_ptr<DBHandle> handle
	) :
		BaseAsyncState<std::shared_ptr<DBHandle>>(env, handle) {}
};

/**
 * State for the `Get` async work. This is used for both `DBHandle` and
 * `TransactionHandle`.
 */
template<typename T>
struct AsyncGetState final : BaseAsyncState<T> {
	AsyncGetState(
		napi_env env,
		T handle,
		rocksdb::ReadOptions& readOptions,
		std::string key
	) :
		BaseAsyncState<T>(env, handle),
		readOptions(readOptions),
		key(std::move(key)) {}

	rocksdb::ReadOptions readOptions;
	// the data for key and value both need to be owned by AsyncGetState, so we need to use std::string (RocksDB Slice doesn't preserve ownership)
	std::string key;
	std::string value;

	// Verification table state for post-read check and populate.
	bool hasExpectedVersion = false;
	uint64_t expectedVersion = 0;
	bool wantsPopulate = false;
	std::atomic<uint64_t>* vtSlot = nullptr;
};

napi_value resolveGetSyncResult(
	napi_env env,
	const char* errorMsg,
	rocksdb::Status& status,
	std::string& value,
	napi_value resolve,
	napi_value reject
);

template<typename T>
void resolveGetResult(
	napi_env env,
	const char* errorMsg,
	AsyncGetState<T>* state
) {
	napi_value global;
	NAPI_STATUS_THROWS_VOID(::napi_get_global(env, &global));

	if (state->status.IsNotFound() || state->status.ok()) {
		if (state->status.ok() && state->vtSlot) {
			// VT check and populate for the async read result.
			rocksdb::Slice valueSlice(state->value.data(), state->value.size());
			uint64_t extracted = VerificationTable::extractVersionFromValue(valueSlice);
			if (state->hasExpectedVersion && extracted != 0 && extracted == state->expectedVersion) {
				// Soft miss: value still carries the expected version — signal FRESH.
				VerificationTable::populateVersion(state->vtSlot, state->expectedVersion);
				napi_value freshResult;
				::napi_create_int32(env, FRESH_VERSION_FLAG, &freshResult);
				state->callResolve(freshResult);
				return;
			}
			if ((state->wantsPopulate || state->hasExpectedVersion) && extracted != 0) {
				VerificationTable::populateVersion(state->vtSlot, extracted);
			}
		}
		napi_value result;
		if (state->status.IsNotFound()) {
			napi_get_undefined(env, &result);
		} else {
			// TODO: when in "fast" mode, use the shared buffer
			NAPI_STATUS_THROWS_VOID(::napi_create_buffer_copy(env, state->value.size(), state->value.data(), nullptr, &result));
		}

		state->callResolve(result);
	} else {
		ROCKSDB_STATUS_CREATE_NAPI_ERROR_VOID(state->status, "Get failed");
		state->callReject(error);
	}
}

} // namespace rocksdb_js

#endif
