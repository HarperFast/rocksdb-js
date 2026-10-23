#ifndef __DB_ITERATOR_HANDLE_H__
#define __DB_ITERATOR_HANDLE_H__

#include <atomic>
#include <mutex>
#include <string>
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
		std::shared_ptr<TransactionHandle> txnHandle,
		DBIteratorOptions& options,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr,
		bool pooled = false
	);

	/**
	 * Cleans up the iterator handle.
	 */
	~DBIteratorHandle();

	/**
	 * Closes the iterator handle.
	 */
	void close() override;
	bool closeIfOpen();

	/**
	 * Resolves the encoded start and end keys and installs them as the RocksDB
	 * read bounds.
	 */
	void init(DBIteratorOptions& options);

	/**
	 * Counts the entries from the current position to the end of the range.
	 *
	 * Returns false when the descriptor began closing mid-scan. The scan is
	 * unbounded and its caller holds an `OperationGuard`, which `finishClose()`
	 * drains with an untimed wait, so it must abort rather than block teardown
	 * for the length of the range. The partial count is never reported.
	 */
	[[nodiscard]] bool countRemaining(uint64_t& count);

	/**
	 * Whether the iterator is positioned on a key inside the requested range.
	 */
	bool valid() const;

	void advance() {
		if (this->reverse) {
			this->iterator->Prev();
		} else {
			this->iterator->Next();
		}
	}

	// Caller holds `iteratorMutex` and has checked the handle is still open
	// and `boundsMatch(options)`.
	void reuse(DBIteratorOptions& options);

	/**
	 * Whether `options` sets the same read bounds (lower, upper) as this
	 * handle's iterator was created with: a bound's presence is fixed for the
	 * life of a RocksDB iterator, only its value can change.
	 */
	bool boundsMatch(const DBIteratorOptions& options) const;

	/**
	 * Whether the column family this pooled handle reads is still `target`'s
	 * current one, and not cleared since. Only meaningful while the caller
	 * holds an `OperationGuard`.
	 */
	bool readsColumnOf(const std::shared_ptr<DBHandle>& target) const;

	std::shared_ptr<DBHandle> dbHandle;
	std::shared_ptr<TransactionHandle> txnHandle;
	bool exclusiveStart;
	bool inclusiveEnd;
	bool reverse;
	bool values;
	bool needsStableValueBuffer;
	bool enforceBounds;
	std::unique_ptr<rocksdb::Iterator> iterator;
	std::string startKeyStr;
	std::string endKeyStr;
	rocksdb::Slice startKey;
	rocksdb::Slice endKey;
	std::mutex iteratorMutex;
	bool transactionRegistered = false;

	const bool pooled;
	const bool hasLowerBound;
	const bool hasUpperBound;

	/**
	 * Its database draws on the process WriteBufferManager, so the handle may
	 * not idle in the pool while that manager can stall writers.
	 */
	bool writeBufferManagerAttached = false;

	// In the transaction's idle slots; not counted in `activeIteratorCount`.
	// Guarded, like `poolGeneration`, by the transaction's `iteratorsMutex`.
	bool idle = false;
	uint64_t poolGeneration = 0;

	// The descriptor whose closables this handle is attached to; `closeIfOpen`
	// detaches it.
	std::atomic<DBDescriptor*> attachedDescriptor{nullptr};

	// The transaction's `iteratorEpoch` when this handle was last lent out.
	uint64_t lentEpoch = 0;

private:
	/**
	 * Seeks the iterator to the first or last key, or the start key if
	 * `exclusiveStart` is true.
	 */
	void seek();

	std::weak_ptr<ColumnFamilyDescriptor> column;
	uint64_t clearEpoch = 0;
};

/**
 * Pool counters for the calling thread, for tests. Thread-local so workers
 * never contend on a shared cache line.
 */
struct IteratorPoolStats {
	uint64_t created = 0;
	uint64_t reused = 0;
};
extern thread_local IteratorPoolStats iteratorPoolStats;

/**
 * Whether transaction iterators may be pooled: RocksDB 11+ (the version
 * `test/native/transaction_iterator_reuse_test.cc` pins the reseek
 * semantics for) and not disabled with `ROCKSDB_JS_ITERATOR_POOL=0`.
 */
bool iteratorPoolEnabled();

} // namespace rocksdb_js

#endif
