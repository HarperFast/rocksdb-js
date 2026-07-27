#ifndef __DB_HANDLE_H__
#define __DB_HANDLE_H__

#include <memory>
#include <vector>
#include <utility>
#include <string>
#include <unordered_map>
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
	 * The write options every write path on this handle uses, built once in
	 * open() rather than per write. Read it through writeOptions(), which
	 * documents why the flags matter.
	 */
	rocksdb::WriteOptions writeOpts;

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
	 * The node environment.
	 */
	napi_env env;

	/**
	 * A reference to the main `rocksdb_js` exports object. This is needed to
	 * get the `TransactionLog` class.
	 */
	napi_ref exportsRef;

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

	rocksdb::Status clear();
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

	/**
	 * The write options every write path on this handle must use.
	 *
	 * `ignore_missing_column_families` is the load-bearing one. Without it, a
	 * single write naming a dropped column family fails inside RocksDB *after*
	 * the batch has reached the WAL but before it reaches the memtables. RocksDB
	 * treats that as unrecoverable inconsistency and latches a background error
	 * on the whole environment, so from that instant every write to every column
	 * family on this path — including families created afterwards — fails with
	 * the same `Invalid argument: Invalid column family specified in write batch`
	 * until the environment is closed and reopened. One racing writer takes the
	 * whole database down. With the flag set, RocksDB skips the entries naming a
	 * dropped family and applies the rest of the batch, which is the documented
	 * behaviour for exactly this case.
	 *
	 * Note this must be set on EVERY writer, not just the one that races a drop:
	 * RocksDB merges concurrent writes into a group and applies the group under
	 * the *leader's* write options. Construct write options here and nowhere
	 * else.
	 */
	const rocksdb::WriteOptions& writeOptions() const;
};

} // namespace rocksdb_js

#endif
