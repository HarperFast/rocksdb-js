#ifndef __DATABASE_H__
#define __DATABASE_H__

#include <node_api.h>
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "database/db_handle.h"
#include "napi/macros.h"
#include "core/platform.h"
#include "napi/helpers.h"
#include "napi/async.h"

namespace rocksdb_js {

#define ONLY_IF_IN_MEMORY_CACHE_FLAG 0x40000000
#define NOT_IN_MEMORY_CACHE_FLAG 0x40000000
#define ALWAYS_CREATE_NEW_BUFFER_FLAG 0x20000000

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
	static napi_value Backup(napi_env env, napi_callback_info info);
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
