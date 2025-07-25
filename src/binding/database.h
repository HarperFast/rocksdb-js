#ifndef __DATABASE_H__
#define __DATABASE_H__

#include <node_api.h>
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "db_handle.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

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
	static napi_value Close(napi_env env, napi_callback_info info);
	static napi_value Get(napi_env env, napi_callback_info info);
	static napi_value GetCount(napi_env env, napi_callback_info info);
	static napi_value GetOldestSnapshotTimestamp(napi_env env, napi_callback_info info);
	static napi_value GetSync(napi_env env, napi_callback_info info);
	static napi_value IsOpen(napi_env env, napi_callback_info info);
	static napi_value Open(napi_env env, napi_callback_info info);
	static napi_value PutSync(napi_env env, napi_callback_info info);
	static napi_value RemoveSync(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);
};

/**
 * State for the `Clear` async work.
 */
struct AsyncClearState final : BaseAsyncState<std::shared_ptr<DBHandle>> {
	AsyncClearState(
		napi_env env,
		std::shared_ptr<DBHandle> handle,
		uint32_t batchSize
	) :
		BaseAsyncState<std::shared_ptr<DBHandle>>(env, handle),
		batchSize(batchSize) {}

	uint64_t deleted;
	uint32_t batchSize;
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
		rocksdb::Slice& keySlice
	) :
		BaseAsyncState<T>(env, handle),
		readOptions(readOptions),
		keySlice(keySlice) {}

	rocksdb::ReadOptions readOptions;
	rocksdb::Slice keySlice;
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

void resolveGetResult(
	napi_env env,
	const char* errorMsg,
	rocksdb::Status& status,
	std::string& value,
	napi_ref resolveRef,
	napi_ref rejectRef
);

} // namespace rocksdb_js

#endif
