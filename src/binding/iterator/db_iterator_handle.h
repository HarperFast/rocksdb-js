#ifndef __DB_ITERATOR_HANDLE_H__
#define __DB_ITERATOR_HANDLE_H__

#include "database/db_handle.h"
#include "iterator/db_iterator.h"
#include "transaction/transaction_handle.h"
#include "napi/macros.h"
#include "transaction/transaction.h"
#include "core/platform.h"
#include "napi/helpers.h"
#include "napi/async.h"

namespace rocksdb_js {

/**
 * A handle that owns the RocksDB iterator and is used to clean up the iterator
 * when the Iterator JS object is garbage collected.
 */
struct DBIteratorHandle final : Closable, public std::enable_shared_from_this<DBIteratorHandle> {
	/**
	 * Initializes the iterator handle using a database handle.
	 */
	DBIteratorHandle(std::shared_ptr<DBHandle> dbHandle, DBIteratorOptions& options);

	/**
	 * Initializes the iterator handle using a transaction handle.
	 *
	 * `dbHandleOverride` selects the column family to iterate. A transaction
	 * belongs to a database rather than a single column family, so a caller
	 * iterating through another DBI in the same database passes that DBI's
	 * handle here. Defaults to the transaction's own column family.
	 */
	DBIteratorHandle(
		TransactionHandle* txnHandle,
		DBIteratorOptions& options,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr
	);

	/**
	 * Cleans up the iterator handle.
	 */
	~DBIteratorHandle();

	/**
	 * Closes the iterator handle.
	 */
	void close() override;

	/**
	 * Initializes the iterator start and end key, then registers this handle
	 * to be closed when the DBDescriptor is closed.
	 */
	void init(DBIteratorOptions& options);

	std::shared_ptr<DBHandle> dbHandle;
	bool exclusiveStart;
	bool inclusiveEnd;
	bool reverse;
	bool values;
	bool needsStableValueBuffer;
	std::unique_ptr<rocksdb::Iterator> iterator;
	std::string startKeyStr;
	std::string endKeyStr;
	rocksdb::Slice startKey;
	rocksdb::Slice endKey;

private:
	/**
	 * Seeks the iterator to the first or last key, or the start key if
	 * `exclusiveStart` is true.
	 */
	void seek(DBIteratorOptions& options);
};

} // namespace rocksdb_js

#endif