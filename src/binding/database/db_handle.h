#ifndef __DB_HANDLE_H__
#define __DB_HANDLE_H__

#include <atomic>
#include <memory>
#include <vector>
#include <utility>
#include <string>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <node_api.h>
#include "rocksdb/db.h"
#include "database/db_descriptor.h"
#include "options/db_options.h"
#include "transaction_log/transaction_log_store.h"
#include "core/platform.h"
#include "napi/helpers.h"
#include "napi/async.h"

namespace rocksdb_js {

// forward declarations
struct ColumnFamilyDescriptor;
struct DBDescriptor;

/**
 * Handle for a RocksDB database and the selected column family. This handle is
 * returned by the Registry and is used by the `Database` class.
 *
 * This handle is for convenience since passing around a shared pointer is a
 * pain.
 */
struct DBHandle final : Closable, AsyncWorkHandle, public std::enable_shared_from_this<DBHandle> {
	/**
	 * The RocksDB database descriptor
	 */
	std::shared_ptr<DBDescriptor> descriptor;

	/**
	 * The RocksDB column family descriptor.
	 */
	std::shared_ptr<ColumnFamilyDescriptor> columnDescriptor;

	/**
	 * The path of the database.
	 */
	std::string path;

	/**
	 * Whether to disable WAL.
	 */
	bool disableWAL = false;

	/**
	 * Whether to register writes from this column family into the
	 * VerificationTable so that cached record versions are invalidated
	 * on commit. Set via the `verificationTable: true` open option.
	 *
	 * Only enable this for column families whose records are cached
	 * (typically the primary CF of a table). Secondary-index CFs should
	 * leave this false to avoid unnecessary VT contention.
	 */
	bool enableVerificationTable = false;

	/**
	 * Immutable VerificationTable address components for this open lifecycle.
	 * These let VT-only fast paths avoid dereferencing teardown-owned native
	 * descriptors or registering as in-flight database operations.
	 */
	uint64_t verificationTableDbId = 0;
	uint32_t verificationTableColumnFamilyId = 0;

	/**
	 * Cancellation token handed to `rocksdb::CompactRangeOptions::canceled` by
	 * an async `compact()` issued through this handle. It is the per-handle twin
	 * of `DBDescriptor::compactCancelRequested`, and the split is load-bearing:
	 * the two cancellations are armed by different owners at different points of
	 * teardown.
	 *
	 *  - The descriptor token is armed by `beginClose()`, which runs before
	 *    `finishClose()` drains `operationsInFlight` -- the wait that a
	 *    `compactSync()` blocks, since it holds an OperationGuard for its whole
	 *    duration.
	 *  - This token is armed by `close()`, which runs before
	 *    `waitForAsyncWorkCompletion()` -- the wait that an async `compact()`
	 *    blocks, since it released its OperationGuard at setup handoff and is
	 *    only awaited by this handle's async-work drain.
	 *
	 * A self-close (`db.close()` -> `DBRegistry::CloseDB`) reaches that drain
	 * *before* it reaches `beginClose()`, so the descriptor token cannot cover
	 * the async case: the JS thread would park for the compaction's full
	 * duration. Arming the descriptor token from `CloseDB` instead is not an
	 * option -- it is never cleared, so one handle closing would permanently
	 * kill manual compaction for every other handle sharing the descriptor.
	 *
	 * Unlike the descriptor token this one IS cleared, by `open()`, because a
	 * handle outlives its close and may be reopened.
	 *
	 * Covered by `test/fixtures/fork-compact-cancel-close.mts` (self-close) and
	 * `test/fixtures/fork-compact-cancel-async.mts` (foreign destroy, which
	 * reaches it through `finishClose()`'s closables sweep).
	 */
	std::atomic<bool> compactCancelRequested{false};

	/**
	 * The node environment.
	 */
	napi_env env;
	std::thread::id ownerThreadId;

	/**
	 * A reference to the main `rocksdb_js` exports object. This is needed to
	 * get the `TransactionLog` class.
	 */
	napi_ref exportsRef;
	std::mutex closeMutex;

	/**
	 * The default transaction log store.
	 */
	std::weak_ptr<TransactionLogStore> defaultLog;

	/**
	 * A map of transaction log store names to `TransactionLog` JavaScript
	 * instances.
	 */
	std::unordered_map<std::string, napi_ref> logRefs;

	/**
	 * The shared default value buffer and its length.
	 */
	char* defaultValueBufferPtr = nullptr;
	size_t defaultValueBufferLength = 0;

	/**
	 * The shared default key buffer and its length.
	 */
	char* defaultKeyBufferPtr = nullptr;
	size_t defaultKeyBufferLength = 0;

	/**
	 * Pointer to a shared Uint32Array buffer used by iterators to communicate
	 * key and value lengths back to JavaScript without per-iteration NAPI
	 * property accesses. Layout: [keyLength, valueLength].
	 */
	char* iteratorStatePtr = nullptr;
	size_t iteratorStateLength = 0;

	DBHandle(napi_env env, napi_ref exportsRef);
	~DBHandle();

	/**
	 * Clears the handle's column family, compacting afterwards to reclaim space.
	 * `compactCanceled` is the token that compaction hands RocksDB; pass the
	 * descriptor's for a synchronous caller and this handle's for an async one
	 * (see `compactCancelRequested`).
	 */
	rocksdb::Status clear(std::atomic<bool>* compactCanceled);
	void close() override;
	napi_value get(
		napi_env env,
		rocksdb::Slice& key,
		napi_value resolve,
		napi_value reject,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr
	);

	rocksdb::ColumnFamilyHandle* getColumnFamilyHandle() const;
	std::string getColumnFamilyName() const;

	napi_value getStat(napi_env env, const std::string& statName);
	napi_value getStats(napi_env env, bool all);

	/**
	 * Aggregates the summarized `txnlog.*` statistics across all of this
	 * database's transaction logs into `total` (a sum of per-store
	 * TransactionLogStoreStats fields) and `logCount` (the number of logs).
	 */
	void collectTransactionLogSummary(TransactionLogStoreStats& total, uint64_t& logCount);

	void open(const std::string& path, const DBOptions& options);
	bool opened() const;
	void unrefLog(const std::string& name);
	napi_value useLog(napi_env env, napi_value jsDatabase, std::string& name);
};

} // namespace rocksdb_js

#endif
