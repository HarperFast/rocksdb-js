#include "database/db_descriptor.h"
#include "database/db_settings.h"
#include "transaction_log/transaction_log_store_registry.h"
#include "rocksdb/listener.h"
#include <algorithm>
#include <memory>

namespace rocksdb_js {

// forward declarations
static void callJsCallback(napi_env env, napi_value jsCallback, void* context, void* data);

struct JobTracker final {
	int columnFamilyCount = 0;
	rocksdb::SequenceNumber flushedSequence = 0;
};

/**
 * Custom event listener that handles flush completion events and notifies
 * transaction log stores to track what has been flushed to the database.
 */
class TransactionLogEventListener : public rocksdb::EventListener {
public:
	TransactionLogEventListener(std::shared_ptr<std::weak_ptr<DBDescriptor>> descriptorPtr)
		: descriptorPtr(descriptorPtr) {}

	void OnFlushBegin(rocksdb::DB* db, const rocksdb::FlushJobInfo& flush_info) override {
		if (!descriptorPtr) {
			return;
		}

		auto desc = descriptorPtr->lock();
		if (!desc) {
			return;
		}
		// Track flush job by job_id, so we can determine when all the flushes have completed for
		// With atomic flushes, there will be multiple flush events for each column family in the database
		// We we want to flush at the beginning of the flush job (for first time job_id appears)
		// And then we want to track the job so that we can determine when all the flushes have completed for
		// the database job.
		auto it = this->jobTrackers.find(flush_info.job_id);
		if (it == this->jobTrackers.end()) {
			// Create new entry
			JobTracker tracker;
			tracker.columnFamilyCount = 1;
			rocksdb::SequenceNumber flushedSequence = flush_info.largest_seqno;
			tracker.flushedSequence = flushedSequence;
			this->jobTrackers[flush_info.job_id] = tracker;
			DEBUG_LOG("%p TransactionLogEventListener::OnFlushBegin flushedSequence=%llu\n",
				desc.get(), (unsigned long long)flushedSequence);

			// Get stores from the registry
			auto stores = TransactionLogStoreRegistry::GetStores(desc->path);
			for (auto& store : stores) {
				store->databaseFlushBegin(flushedSequence);
			}
		} else {
			// Increment existing entry so we know how many column families are being flushed
			it->second.columnFamilyCount++;
		}
	}

	void OnFlushCompleted(rocksdb::DB* db, const rocksdb::FlushJobInfo& flush_info) override {
		if (!descriptorPtr) {
			return;
		}

		auto desc = descriptorPtr->lock();
		if (!desc) {
			return;
		}

		rocksdb::SequenceNumber flushedSequence = flush_info.largest_seqno;
		DEBUG_LOG("%p TransactionLogEventListener::OnFlushCompleted cf name=%s job id=%u flushedSequence=%llu\n",
			desc.get(), flush_info.cf_name.c_str(), flush_info.job_id, (unsigned long long)flushedSequence);

		// Track flush job by job_id
		auto it = this->jobTrackers.find(flush_info.job_id);
		if (it == this->jobTrackers.end()) {
			DEBUG_LOG("%p TransactionLogEventListener::OnFlushCompleted unable to find job id=%d\n",
				desc.get(), flush_info.job_id);
		} else {
			// we find the highest sequence number; this represents the overall sequence
			// number for the flush job
			if (flushedSequence > it->second.flushedSequence) {
				it->second.flushedSequence = flushedSequence;
			}
			// Decrement existing entry until we have completed all the flush actions for the job
			if (--it->second.columnFamilyCount == 0) {
				// The last CF flush has completed for the job, now signal that the database flush is done
				DEBUG_LOG("%p TransactionLogEventListener::OnFlushCompleted job completed name=%s job id=%d flushedSequence=%llu\n",
					desc.get(), flush_info.cf_name.c_str(), flush_info.job_id, (unsigned long long)it->second.flushedSequence);

				// Get stores from the registry
				auto stores = TransactionLogStoreRegistry::GetStores(desc->path);
				for (auto& store : stores) {
					store->databaseFlushed(it->second.flushedSequence);
				}
				this->jobTrackers.erase(it); // cleanup
			}
		}
	}

private:
	std::shared_ptr<std::weak_ptr<DBDescriptor>> descriptorPtr;
	std::unordered_map<int, JobTracker> jobTrackers;
};

/**
 * Creates a new database descriptor. This constructor is private. To create a
 * new DBDescriptor, use `DBDescriptor::open()`.
 */
DBDescriptor::DBDescriptor(
	const std::string& path,
	const DBOptions& options,
	std::shared_ptr<rocksdb::DB> db,
	std::unordered_map<std::string, std::shared_ptr<ColumnFamilyDescriptor>>&& columns,
	std::shared_ptr<rocksdb::Statistics> statistics
):
	path(path),
	mode(options.mode),
	readOnly(options.readOnly),
	db(db),
	columns(std::move(columns)),
	statistics(statistics)
{}

/**
 * Destroy the database descriptor and any resources associated to it
 * (transactions, iterators, etc).
 */
DBDescriptor::~DBDescriptor() {
	DEBUG_LOG("%p DBDescriptor::~DBDescriptor Closing \"%s\"\n", this, this->path.c_str());
	this->close();
}

/**
 * Close the database descriptor and any resources associated with it
 * (transactions, iterators, etc).
 */
void DBDescriptor::close() {
	// check if already closing
	if (this->closing.exchange(true)) {
		DEBUG_LOG("%p DBDescriptor::close Already closing \"%s\"\n", this, this->path.c_str());
		return;
	}

	DEBUG_LOG("%p DBDescriptor::close Closing \"%s\" (mode=%s read-only=%s closables=%zu columns=%zu transactions=%zu)\n",
		this, this->path.c_str(), this->mode == DBMode::Optimistic ? "optimistic" : "pessimistic", this->readOnly ? "true" : "false", this->closables.size(), this->columns.size(), this->transactions.size());

	// Wait for all in-flight operations to complete before cleanup.
	// The closing flag is already set, so new operations will fail with "Database is closing".
	// Existing operations will decrement operationsInFlight and notify us when done.
	DEBUG_LOG("%p DBDescriptor::close Waiting for %u in-flight operations \"%s\"\n", this, this->operationsInFlight.load(), this->path.c_str());
	uint32_t current;
	while ((current = this->operationsInFlight.load()) != 0) {
		this->operationsInFlight.wait(current);
	}
	DEBUG_LOG("%p DBDescriptor::close All operations complete \"%s\"\n", this, this->path.c_str());

	// We want to ensure that all in-memory data is written to disk
	this->flush();

	// Trigger manual compaction on all column families to reclaim space from
	// tombstones before closing
	if (!this->readOnly && DBSettings::getInstance().getCompactOnClose()) {
		// Snapshot under the columns mutex; a concurrent drop can erase from
		// the map while we compact.
		std::vector<std::shared_ptr<ColumnFamilyDescriptor>> pinnedColumns;
		{
			std::lock_guard<std::mutex> columnsLock(this->columnsMutex);
			pinnedColumns.reserve(this->columns.size());
			for (const auto& [name, columnDesc] : this->columns) {
				pinnedColumns.push_back(columnDesc);
			}
		}
		for (const auto& columnDesc : pinnedColumns) {
			if (columnDesc && columnDesc->column) {
				this->compactRange(columnDesc->column.get(), nullptr, nullptr);
			}
		}
	}

	// Wait for any outstanding (background threads) operations to complete.
	// Note that this is not setting the RocksDB `close_db` flag since active
	// references to the databases may still exist. Also, contrary to the
	// suggestions of the documentation, this method alone does not seem to
	// trigger a flush
	rocksdb::WaitForCompactOptions options;
	this->db->WaitForCompact(options);

	std::unique_lock<std::mutex> txnsLock(this->txnsMutex);

	// Close all handles that still exist and reset their descriptor references
	for (auto it = this->closables.begin(); it != this->closables.end(); ) {
		if (auto closable = it->second.lock()) {
			// Remove from map before closing to avoid re-entrant detach() calls
			it = this->closables.erase(it);

			// Release mutex during close to avoid deadlocks
			txnsLock.unlock();
			closable->close();
			txnsLock.lock();
		} else {
			// Handle was already GC'd, just remove the expired weak_ptr
			it = this->closables.erase(it);
		}
	}

	// Safety-net: cancel any VT locks still held by this DB after all
	// TransactionHandles have been closed. Under normal operation the
	// closable->close() calls above already call releaseIntent() + wake()
	// for every transaction; this is a defensive final pass.
	{
		auto* vt = DBSettings::getInstance().getVerificationTableRaw();
		if (vt) {
			vt->cancelForDB(reinterpret_cast<uintptr_t>(this));
		}
	}

	// Unregister from transaction log store registry - this will clean up stores
	// when the last descriptor for this path is closed
	TransactionLogStoreRegistry::Unregister(this->path);

	this->transactions.clear();
	{
		std::lock_guard<std::mutex> columnsLock(this->columnsMutex);
		this->columns.clear();
	}

	this->events.releaseAll();

	this->db.reset();
}

/**
 * Registers a database resource to be closed when the descriptor is closed.
 *
 * Important: The closable must be same smart_ptr that is napi-wrapped and
 * bound to the JavaScript class counterpart.
 */
void DBDescriptor::attach(std::shared_ptr<Closable> closable) {
	std::lock_guard<std::mutex> lock(this->txnsMutex);
	this->closables[closable.get()] = std::weak_ptr<Closable>(closable);
}

/**
 * Unregisters a database resource from being closed when the descriptor is
 * closed.
 */
void DBDescriptor::detach(std::shared_ptr<Closable> closable) {
	std::lock_guard<std::mutex> lock(this->txnsMutex);
	this->closables.erase(closable.get());
}

#define SET_DOUBLE_PROP(obj, name, value) \
	do { \
		napi_value jsValue; \
		NAPI_STATUS_THROWS(::napi_create_double(env, value, &jsValue)); \
		NAPI_STATUS_THROWS(::napi_set_named_property(env, obj, name, jsValue)); \
	} while (0)

#define SET_INT64_PROP(obj, name, value) \
	do { \
		napi_value jsValue; \
		NAPI_STATUS_THROWS(::napi_create_int64(env, value, &jsValue)); \
		NAPI_STATUS_THROWS(::napi_set_named_property(env, obj, name, jsValue)); \
	} while (0)

#define SET_HISTOGRAM_DATA_PROP(obj, name, histogram) \
	do { \
		rocksdb::HistogramData hist; \
		this->statistics->histogramData(histogram, &hist); \
		napi_value jsValue = buildHistogramDataObject(env, hist); \
		NAPI_STATUS_THROWS(::napi_set_named_property(env, obj, name, jsValue)); \
	} while (0)

napi_value buildHistogramDataObject(napi_env env, const rocksdb::HistogramData& hist) {
	napi_value obj;
	NAPI_STATUS_THROWS(::napi_create_object(env, &obj));

	SET_DOUBLE_PROP(obj, "average", hist.average);
	SET_INT64_PROP(obj, "count", hist.count);
	SET_DOUBLE_PROP(obj, "max", hist.max);
	SET_DOUBLE_PROP(obj, "median", hist.median);
	SET_DOUBLE_PROP(obj, "min", hist.min);
	SET_DOUBLE_PROP(obj, "percentile95", hist.percentile95);
	SET_DOUBLE_PROP(obj, "percentile99", hist.percentile99);
	SET_DOUBLE_PROP(obj, "standardDeviation", hist.standard_deviation);
	SET_INT64_PROP(obj, "sum", hist.sum);

	return obj;
}

napi_value DBDescriptor::getStat(napi_env env, const std::string& statName) {
	if (!this->statistics) {
		::napi_throw_error(env, nullptr, "Statistics are not enabled");
		NAPI_RETURN_UNDEFINED();
	}

	for (const auto& [ticker, name] : rocksdb::TickersNameMap) {
		if (name == statName) {
			uint64_t value = this->statistics->getTickerCount(ticker);
			napi_value result;
			NAPI_STATUS_THROWS(::napi_create_int64(env, value, &result));
			return result;
		}
	}

	for (const auto& [histogram, name] : rocksdb::HistogramsNameMap) {
		if (name == statName) {
			rocksdb::HistogramData hist;
			this->statistics->histogramData(histogram, &hist);
			return buildHistogramDataObject(env, hist);
		}
	}

	NAPI_RETURN_UNDEFINED();
}

bool DBDescriptor::getStats(napi_env env, bool all, napi_value* result) {
	if (!this->statistics) {
		return false;
	}

#undef NAPI_STATUS_THROWS
#define NAPI_STATUS_THROWS(call) NAPI_STATUS_THROWS_RVAL(call, false)

	NAPI_STATUS_THROWS(::napi_create_object(env, result));

	if (all) {
		// get all stats
		for (const auto& [ticker, name] : rocksdb::TickersNameMap) {
			napi_value value;
			NAPI_STATUS_THROWS(::napi_create_int64(env, this->statistics->getTickerCount(ticker), &value));
			napi_value key;
			NAPI_STATUS_THROWS(::napi_create_string_utf8(env, name.c_str(), name.size(), &key));
			NAPI_STATUS_THROWS(::napi_set_property(env, *result, key, value));
		}

		for (const auto& [histogram, name] : rocksdb::HistogramsNameMap) {
			rocksdb::HistogramData hist;
			this->statistics->histogramData(histogram, &hist);
			napi_value key;
			NAPI_STATUS_THROWS(::napi_create_string_utf8(env, name.c_str(), name.size(), &key));
			napi_value value = buildHistogramDataObject(env, hist);
			NAPI_STATUS_THROWS(::napi_set_property(env, *result, key, value));
		}
	} else {
		// get essential stats

		// block cache
		SET_INT64_PROP(*result, "rocksdb.block.cache.hit", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_HIT));
		SET_INT64_PROP(*result, "rocksdb.block.cache.miss", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_MISS));
		SET_INT64_PROP(*result, "rocksdb.block.cache.data.hit", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_DATA_HIT));
		SET_INT64_PROP(*result, "rocksdb.block.cache.data.miss", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_DATA_MISS));
		SET_INT64_PROP(*result, "rocksdb.block.cache.index.hit", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_INDEX_HIT));
		SET_INT64_PROP(*result, "rocksdb.block.cache.index.miss", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_INDEX_MISS));
		SET_INT64_PROP(*result, "rocksdb.block.cache.filter.hit", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_FILTER_HIT));
		SET_INT64_PROP(*result, "rocksdb.block.cache.filter.miss", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_FILTER_MISS));

		// bloom filter
		SET_INT64_PROP(*result, "rocksdb.bloom.filter.useful", this->statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL));
		SET_INT64_PROP(*result, "rocksdb.bloom.filter.full.positive", this->statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_FULL_POSITIVE));
		SET_INT64_PROP(*result, "rocksdb.bloom.filter.full.true.positive", this->statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_FULL_TRUE_POSITIVE));

		// iterators
		SET_INT64_PROP(*result, "rocksdb.db.iter.bytes.read", this->statistics->getTickerCount(rocksdb::Tickers::ITER_BYTES_READ));
		SET_INT64_PROP(*result, "rocksdb.number.reseeks.iteration", this->statistics->getTickerCount(rocksdb::Tickers::NUMBER_OF_RESEEKS_IN_ITERATION));

		// keys
		SET_INT64_PROP(*result, "rocksdb.number.keys.read", this->statistics->getTickerCount(rocksdb::Tickers::NUMBER_KEYS_READ));
		SET_INT64_PROP(*result, "rocksdb.number.keys.written", this->statistics->getTickerCount(rocksdb::Tickers::NUMBER_KEYS_WRITTEN));

		// values
		SET_INT64_PROP(*result, "rocksdb.bytes.read", this->statistics->getTickerCount(rocksdb::Tickers::BYTES_READ));
		SET_INT64_PROP(*result, "rocksdb.bytes.written", this->statistics->getTickerCount(rocksdb::Tickers::BYTES_WRITTEN));

		// memtable
		SET_INT64_PROP(*result, "rocksdb.memtable.hit", this->statistics->getTickerCount(rocksdb::Tickers::MEMTABLE_HIT));
		SET_INT64_PROP(*result, "rocksdb.memtable.miss", this->statistics->getTickerCount(rocksdb::Tickers::MEMTABLE_MISS));

		// transactions
		SET_INT64_PROP(*result, "rocksdb.txn.overhead.mutex.prepare", this->statistics->getTickerCount(rocksdb::Tickers::TXN_PREPARE_MUTEX_OVERHEAD));
		SET_INT64_PROP(*result, "rocksdb.txn.overhead.mutex.old.commit.map", this->statistics->getTickerCount(rocksdb::Tickers::TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD));
		SET_INT64_PROP(*result, "rocksdb.txn.overhead.mutex.snapshot", this->statistics->getTickerCount(rocksdb::Tickers::TXN_SNAPSHOT_MUTEX_OVERHEAD));

		// compaction
		SET_INT64_PROP(*result, "rocksdb.compact.read.bytes", this->statistics->getTickerCount(rocksdb::Tickers::COMPACT_READ_BYTES));
		SET_INT64_PROP(*result, "rocksdb.compact.write.bytes", this->statistics->getTickerCount(rocksdb::Tickers::COMPACT_WRITE_BYTES));
		SET_INT64_PROP(*result, "rocksdb.compaction.cancelled", this->statistics->getTickerCount(rocksdb::Tickers::COMPACTION_CANCELLED));
		SET_INT64_PROP(*result, "rocksdb.stall.micros", this->statistics->getTickerCount(rocksdb::Tickers::STALL_MICROS));

		// errors & i/o
		SET_INT64_PROP(*result, "rocksdb.no.file.errors", this->statistics->getTickerCount(rocksdb::Tickers::NO_FILE_ERRORS));
		SET_INT64_PROP(*result, "rocksdb.read.amp.estimate.useful.bytes", this->statistics->getTickerCount(rocksdb::Tickers::READ_AMP_ESTIMATE_USEFUL_BYTES));
		SET_INT64_PROP(*result, "rocksdb.read.amp.total.read.bytes", this->statistics->getTickerCount(rocksdb::Tickers::READ_AMP_TOTAL_READ_BYTES));

		// histogram data
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.db.get.micros", rocksdb::Histograms::DB_GET);
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.db.write.micros", rocksdb::Histograms::DB_WRITE);
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.db.seek.micros", rocksdb::Histograms::DB_SEEK);
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.db.flush.micros", rocksdb::Histograms::FLUSH_TIME);
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.db.write.stall", rocksdb::Histograms::WRITE_STALL);
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.blobdb.value.size", rocksdb::Histograms::BLOB_DB_VALUE_SIZE);
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.sst.read.micros", rocksdb::Histograms::SST_READ_MICROS);
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.compaction.times.micros", rocksdb::Histograms::COMPACTION_TIME);
	}

#undef NAPI_STATUS_THROWS
#define NAPI_STATUS_THROWS(call) NAPI_STATUS_THROWS_RVAL(call, nullptr)

	return true;
}

/**
 * Adds the callback to a queue to be executed mutually exclusive and if the
 * lock is available, executes it immediately followed by any newly queued
 * callbacks. Called by `db.withLock()`.
 */
void DBDescriptor::lockCall(
	napi_env env,
	std::string& key,
	napi_value callback,
	napi_deferred deferred,
	std::shared_ptr<DBHandle> owner
) {
	bool isNewLock = false;
	this->lockEnqueueCallback(
		env,       // env
		key,       // key
		callback,  // callback
		owner,     // owner
		false,     // skipEnqueueIfNewLock
		deferred,  // deferred
		&isNewLock // [out] isNewLock
	);

	if (!isNewLock) {
		DEBUG_LOG("%p DBDescriptor::lockCall callback queued for key:", this);
		DEBUG_LOG_KEY_LN(key);
		return;
	}

	// lock found
	std::unique_lock<std::mutex> locksMutex(this->locksMutex);
	auto lockHandle = this->locks.find(key);

	if (lockHandle == this->locks.end()) {
		DEBUG_LOG("%p DBDescriptor::lockCall no lock found for key:", this);
		DEBUG_LOG_KEY_LN(key);
		return;
	}

	auto& handle = lockHandle->second;

	// try to acquire the "lock" atomically
	bool expected = false;
	if (!handle->isRunning.compare_exchange_strong(expected, true)) {
		// another callback is already running
		DEBUG_LOG("%p DBDescriptor::lockCall another callback is already running for key:", this);
		DEBUG_LOG_KEY_LN(key);
		return;
	}

	// we now "own" the execution for this key
	if (handle->threadsafeCallbacks.empty()) {
		handle->isRunning.store(false);
		DEBUG_LOG("%p DBDescriptor::lockCall no callbacks left, removing lock for key:", this);
		DEBUG_LOG_KEY_LN(key);
		// remove the empty lock handle from the map
		this->locks.erase(key);
		return;
	}

	LockCallback lockCallback = handle->threadsafeCallbacks.front();
	handle->threadsafeCallbacks.pop();
	napi_threadsafe_function threadsafeCallback = lockCallback.callback;

	// release the mutex before calling the callback to avoid holding locks
	// during callback execution
	locksMutex.unlock();

	if (!threadsafeCallback) {
		DEBUG_LOG("%p DBDescriptor::lockCall threadsafe lock callback is null for key:", this);
		DEBUG_LOG_KEY_LN(key);
		return;
	}

	DEBUG_LOG("%p DBDescriptor::lockCall calling callback for key:", this);
	DEBUG_LOG_KEY_LN(key);

	// create callback data that includes the key for completion and deferred promise
	auto* callbackData = new LockCallbackCompletionData(key, weak_from_this(), lockCallback.deferred);

	// use threadsafe function instead of direct call
	napi_status status = ::napi_call_threadsafe_function(threadsafeCallback, callbackData, napi_tsfn_blocking);
	if (status != napi_ok && status != napi_closing) {
		DEBUG_LOG("%p DBDescriptor::lockCall failed to call threadsafe function\n", this);
		delete callbackData;
		this->onCallbackComplete(key);
	}

	// release the threadsafe function
	::napi_release_threadsafe_function(threadsafeCallback, napi_tsfn_release);
}

/**
 * Enqueues a callback to be called when a lock is acquired. Called by
 * `db.tryLock()` and `DBDescriptor::lockCall()`.
 */
void DBDescriptor::lockEnqueueCallback(
	napi_env env,
	std::string& key,
	napi_value callback,
	std::shared_ptr<DBHandle> owner,
	bool skipEnqueueIfNewLock,
	napi_deferred deferred,
	bool* isNewLock
) {
	std::lock_guard<std::mutex> lock(this->locksMutex);
	std::shared_ptr<LockHandle> lockHandle;
	auto lockHandleIterator = this->locks.find(key);

	if (lockHandleIterator == this->locks.end()) {
		// no lock found
		DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback no lock found for key:", this);
		DEBUG_LOG_KEY_LN(key);
		lockHandle = std::make_shared<LockHandle>(owner, env);
		this->locks.emplace(key, lockHandle);
		if (isNewLock != nullptr) {
			*isNewLock = true;
		}
		if (skipEnqueueIfNewLock) {
			DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback skipping enqueue because lock already exists\n", this);
			return;
		}
	} else {
		DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback lock found for key %s\n", this, key.c_str());
		lockHandle = lockHandleIterator->second;
	}

	// lock found
	napi_valuetype type;
	NAPI_STATUS_THROWS_VOID(::napi_typeof(env, callback, &type));
	if (type == napi_function) {
		napi_value resourceName;
		NAPI_STATUS_THROWS_VOID(::napi_create_string_latin1(
			env,
			"rocksdb-js.lock",
			NAPI_AUTO_LENGTH,
			&resourceName
		));

		napi_threadsafe_function threadsafeCallback;
		NAPI_STATUS_THROWS_VOID(::napi_create_threadsafe_function(
			env,                // env
			callback,           // func
			nullptr,            // async_resource
			resourceName,       // async_resource_name
			0,                  // max_queue_size
			1,                  // initial_thread_count
			nullptr,            // thread_finalize_data
			nullptr,            // thread_finalize_callback
			nullptr,            // context
			callJsCallback,     // call_js_cb
			&threadsafeCallback // [out] callback
		));

		DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback enqueuing callback %p\n", this, threadsafeCallback);
		NAPI_STATUS_THROWS_VOID(::napi_unref_threadsafe_function(env, threadsafeCallback));

		// Create LockCallback and add to queue
		lockHandle->threadsafeCallbacks.push(LockCallback(threadsafeCallback, deferred));
	}
}

/**
 * Checks if a lock exists for the given key. Called by `db.hasLock()`.
 */
bool DBDescriptor::lockExistsByKey(std::string& key) {
	std::lock_guard<std::mutex> lock(this->locksMutex);
	auto lockHandle = this->locks.find(key);
	bool exists = lockHandle != this->locks.end();
	DEBUG_LOG("%p DBDescriptor::hasLock %s lock for key \"%s\"\n", this, exists ? "found" : "not found", key.c_str());
	return exists;
}

/**
 * Releases a lock by key. Called by `db.unlock()`.
 */
bool DBDescriptor::lockReleaseByKey(std::string& key) {
	std::queue<LockCallback> threadsafeCallbacks;

	{
		std::lock_guard<std::mutex> lock(this->locksMutex);
		auto lockHandle = this->locks.find(key);

		if (lockHandle == this->locks.end()) {
			// no lock found
			DEBUG_LOG("%p DBDescriptor::lockReleaseByKey no lock found\n", this);
			return false;
		}

		// lock found, remove it
		threadsafeCallbacks = std::move(lockHandle->second->threadsafeCallbacks);
		DEBUG_LOG("%p DBDescriptor::lockReleaseByKey removing lock\n", this);
		this->locks.erase(key);
	}

	DEBUG_LOG("%p DBDescriptor::lockReleaseByKey calling %zu unlock callbacks\n", this, threadsafeCallbacks.size());

	// call the callbacks in order, but stop if any callback fails
	while (!threadsafeCallbacks.empty()) {
		auto lockCallback = threadsafeCallbacks.front();
		threadsafeCallbacks.pop();
		DEBUG_LOG("%p DBDescriptor::lockReleaseByKey calling callback %p\n", this, lockCallback.callback);
		napi_status status = ::napi_call_threadsafe_function(lockCallback.callback, nullptr, napi_tsfn_blocking);
		if (status == napi_closing) {
			continue;
		}
		::napi_release_threadsafe_function(lockCallback.callback, napi_tsfn_release);
	}

	return true;
}

/**
 * Releases all locks owned by the given handle. Called by `db.close()`.
 */
void DBDescriptor::lockReleaseByOwner(DBHandle* owner) {
	std::set<napi_threadsafe_function> threadsafeCallbacks;

	{
		std::lock_guard<std::mutex> lock(this->locksMutex);
			DEBUG_LOG("%p DBDescriptor::lockReleaseByOwner checking %zu locks if they are owned handle %p\n", this, this->locks.size(), owner);
		for (auto it = this->locks.begin(); it != this->locks.end();) {
			auto lockOwner = it->second->owner.lock();
			if (!lockOwner || lockOwner.get() == owner) {
				DEBUG_LOG("%p DBDescriptor::lockReleaseByOwner found lock %p with %zu callbacks\n", this, it->second.get(), it->second->threadsafeCallbacks.size());
				// move all callbacks from the queue
				while (!it->second->threadsafeCallbacks.empty()) {
					threadsafeCallbacks.insert(it->second->threadsafeCallbacks.front().callback);
					it->second->threadsafeCallbacks.pop();
				}
				it = this->locks.erase(it);
			} else {
				++it;
			}
		}
	}

	DEBUG_LOG("%p DBDescriptor::lockReleaseByOwner calling %zu unlock callbacks\n", this, threadsafeCallbacks.size());

	// call the callbacks in order, but stop if any callback fails
	for (auto& callback : threadsafeCallbacks) {
		DEBUG_LOG("%p DBDescriptor::lockReleaseByOwner calling callback %p\n", this, callback);
		napi_status status = ::napi_call_threadsafe_function(callback, nullptr, napi_tsfn_blocking);
		if (status == napi_closing) {
			continue;
		}
		::napi_release_threadsafe_function(callback, napi_tsfn_release);
	}
}

/**
 * Creates a new DBDescriptor.
 */
std::shared_ptr<DBDescriptor> DBDescriptor::open(const std::string& path, const DBOptions& options) {
	std::string name = options.name.empty() ? "default" : options.name;
	DEBUG_LOG("DBDescriptor::open Opening \"%s\" (column family: \"%s\", read-only: %s)\n", path.c_str(), name.c_str(), options.readOnly ? "true" : "false");

	// set or disable the block cache
	DBSettings& settings = DBSettings::getInstance();
	rocksdb::BlockBasedTableOptions tableOptions;
	if (options.noBlockCache) {
		tableOptions.no_block_cache = true;
	} else {
		tableOptions.block_cache = settings.getBlockCache();
	}

	// set the database options
	rocksdb::Options dbOptions;
	// we could also consider some testing around using atomic_flush
	dbOptions.atomic_flush = true; // this is necessary in order to ensure that we can track full flush jobs back to the corresponding sequence numbers
	dbOptions.comparator = rocksdb::BytewiseComparator();
	dbOptions.create_if_missing = !options.readOnly;
	dbOptions.create_missing_column_families = !options.readOnly;
	dbOptions.db_write_buffer_size = 32 << 20; // 32MB total database write buffer size (may want to make this configurable)
	// Attach the process-wide WriteBufferManager (if configured) so memtable
	// memory is bounded across all DBs in this process. With cost_to_cache,
	// active memtables share the block cache pool — the cache shrinks during
	// write bursts and reclaims room as memtables flush.
	if (auto wbm = settings.getWriteBufferManager()) {
		dbOptions.write_buffer_manager = wbm;
	}
	dbOptions.IncreaseParallelism(options.parallelismThreads);
	dbOptions.keep_log_file_num = 5; // these are informational log files that clutter up the database directory
	dbOptions.persist_user_defined_timestamps = true;
	if (options.enableStats) {
		dbOptions.statistics = rocksdb::CreateDBStatistics();
		dbOptions.statistics->set_stats_level(static_cast<rocksdb::StatsLevel>(options.statsLevel));
	} else {
		dbOptions.statistics = nullptr;
	}

	// Define base ColumnFamilyOptions that include blob settings
	rocksdb::ColumnFamilyOptions cfOptions;
	cfOptions.enable_blob_files = true;
	cfOptions.min_blob_size = 2048;
	cfOptions.enable_blob_garbage_collection = true;
	cfOptions.write_buffer_size = static_cast<size_t>(options.writeBufferSize);
	cfOptions.max_write_buffer_number = options.maxWriteBufferNumber;
	cfOptions.max_write_buffer_size_to_maintain = options.maxWriteBufferSizeToMaintain;
	cfOptions.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOptions));

	// create a shared pointer to hold the weak descriptor reference for the event listener
	auto descriptorWeakPtr = std::make_shared<std::weak_ptr<DBDescriptor>>();
	auto eventListener = std::make_shared<TransactionLogEventListener>(descriptorWeakPtr);
	dbOptions.listeners.push_back(eventListener);

	// prepare the column family stuff - first check if database exists
	std::vector<rocksdb::ColumnFamilyDescriptor> cfDescriptors;
	std::vector<std::string> columnFamilyNames;

	// try to list existing column families
	DEBUG_LOG("DBDescriptor::open Listing column families for \"%s\"\n", path.c_str());
	rocksdb::Status listStatus = rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(), path, &columnFamilyNames);
	if (listStatus.ok() && !columnFamilyNames.empty()) {
		// database exists, use existing column families
		for (const auto& cfName : columnFamilyNames) {
			DEBUG_LOG("DBDescriptor::open Opening column family \"%s\"\n", cfName.c_str());
			cfDescriptors.emplace_back(cfName, cfOptions); // Use cfOptions here
		}
	} else {
		// database doesn't exist or no column families found, use default
		DEBUG_LOG("DBDescriptor::open Database doesn't exist or no column families found, using default\n");
		cfDescriptors = {
			rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, cfOptions) // Use cfOptions here
		};
	}

	std::vector<rocksdb::ColumnFamilyHandle*> cfHandles;
	std::shared_ptr<rocksdb::DB> db;
	std::unordered_map<std::string, std::shared_ptr<ColumnFamilyDescriptor>> columns;

	if (options.readOnly) {
		std::unique_ptr<rocksdb::DB> rdb;
		DEBUG_LOG("DBDescriptor::open Opening readonly db for \"%s\"\n", path.c_str());
		rocksdb::Status status = rocksdb::DB::OpenForReadOnly(dbOptions, path, cfDescriptors, &cfHandles, &rdb);
		if (!status.ok()) {
			DEBUG_LOG("DBDescriptor::open Failed to open readonly db for \"%s\": %s\n", path.c_str(), status.ToString().c_str());
			if (status.IsIOError()) {
				DEBUG_LOG("DBDescriptor::open IOError: %s\n", status.ToString().c_str());
				throw rocksdb_js::DBException("Database does not exist");
			}
			throw rocksdb_js::DBException(status.ToString());
		}
		DEBUG_LOG("DBDescriptor::open Opened readonly db for \"%s\"\n", path.c_str());
		db = std::shared_ptr<rocksdb::DB>(rdb.release(), DBDeleter{});
	} else if (options.mode == DBMode::Pessimistic) {
		rocksdb::TransactionDBOptions txndbOptions;
		txndbOptions.default_lock_timeout = 10000;
		txndbOptions.transaction_lock_timeout = 10000;

		rocksdb::TransactionDB* rdb;
		DEBUG_LOG("DBDescriptor::open Opening pessimistic transaction db for \"%s\"\n", path.c_str());
		rocksdb::Status status = rocksdb::TransactionDB::Open(dbOptions, txndbOptions, path, cfDescriptors, &cfHandles, &rdb);
		if (!status.ok()) {
			DEBUG_LOG("DBDescriptor::open Failed to open pessimistic transaction db for \"%s\": %s\n", path.c_str(), status.ToString().c_str());
			throw rocksdb_js::DBException(status.ToString());
		}
		DEBUG_LOG("DBDescriptor::open Opened pessimistic transaction db for \"%s\"\n", path.c_str());
		db = std::shared_ptr<rocksdb::DB>(rdb, DBDeleter{});
	} else {
		rocksdb::OptimisticTransactionDB* rdb;
		DEBUG_LOG("DBDescriptor::open Opening optimistic transaction db for \"%s\"\n", path.c_str());
		rocksdb::Status status = rocksdb::OptimisticTransactionDB::Open(dbOptions, path, cfDescriptors, &cfHandles, &rdb);
		if (!status.ok()) {
			DEBUG_LOG("DBDescriptor::open Failed to open optimistic transaction db for \"%s\": %s\n", path.c_str(), status.ToString().c_str());
			throw rocksdb_js::DBException(status.ToString());
		}
		DEBUG_LOG("DBDescriptor::open Opened optimistic transaction db for \"%s\"\n", path.c_str());
		db = std::shared_ptr<rocksdb::DB>(rdb, DBDeleter{});
	}

	// figure out if desired column family exists and if not create it
	bool columnExists = false;
	for (size_t n = 0; n < cfHandles.size(); ++n) {
		auto column = std::shared_ptr<rocksdb::ColumnFamilyHandle>(cfHandles[n]);
		auto columnDescriptor = std::make_shared<ColumnFamilyDescriptor>(column);
		columns[cfDescriptors[n].name] = columnDescriptor;
		if (cfDescriptors[n].name == options.name) {
			columnExists = true;
		}
	}
	if (!columnExists) {
		auto column = rocksdb_js::createRocksDBColumnFamily(db, options.name);
		auto columnDescriptor = std::make_shared<ColumnFamilyDescriptor>(column);
		columns[options.name] = columnDescriptor;
	}

	DEBUG_LOG("DBDescriptor::open Creating DBDescriptor for \"%s\"\n", path.c_str());
	auto descriptor = std::shared_ptr<DBDescriptor>(new DBDescriptor(path, options, db, std::move(columns), dbOptions.statistics));

	// set the weak pointer for the event listener
	*descriptorWeakPtr = descriptor;

	// Register with the transaction log store registry
	TransactionLogStoreConfig logConfig;
	logConfig.transactionLogsPath = options.transactionLogsPath;
	logConfig.transactionLogMaxAgeThreshold = options.transactionLogMaxAgeThreshold;
	logConfig.transactionLogMaxSize = options.transactionLogMaxSize;
	logConfig.transactionLogRetentionMs = std::chrono::milliseconds(options.transactionLogRetentionMs);
	TransactionLogStoreRegistry::Register(path, logConfig);
	TransactionLogStoreRegistry::DiscoverStores(path);

	return descriptor;
}

/**
 * Adds a transaction to the registry.
 */
void DBDescriptor::transactionAdd(std::shared_ptr<TransactionHandle> txnHandle) {
	auto id = txnHandle->id;
	std::lock_guard<std::mutex> lock(this->txnsMutex);
	this->transactions.emplace(id, txnHandle);
	this->closables[txnHandle.get()] = std::weak_ptr<Closable>(txnHandle);
}

/**
 * Retrieves a transaction from the registry.
 */
std::shared_ptr<TransactionHandle> DBDescriptor::transactionGet(uint32_t id) {
	std::lock_guard<std::mutex> lock(this->txnsMutex);
	auto it = this->transactions.find(id);
	if (it != this->transactions.end()) {
		auto txnHandle = it->second;
		if (txnHandle && txnHandle->txn) {
			return txnHandle;
		}
	}
	return nullptr;
}

/**
 * Removes a transaction from the registry.
 */
void DBDescriptor::transactionRemove(std::shared_ptr<TransactionHandle> txnHandle) {
	std::lock_guard<std::mutex> lock(this->txnsMutex);
	this->closables.erase(txnHandle.get());

	auto it = this->transactions.find(txnHandle->id);
	if (it != this->transactions.end()) {
		if (it->second != txnHandle) {
			DEBUG_LOG("%p DBDescriptor::transactionRemove txnId %u mismatch! expected %p, got %p\n", this, txnHandle->id, it->second.get(), txnHandle.get());
		}
		this->transactions.erase(it);
	}
}

/**
 * Generates the next unique transaction ID for this database.
 */
uint32_t DBDescriptor::transactionGetNextId() {
	return ++this->nextTransactionId;
}

/**
 * Removes a dropped column family from the columns map so a later open-by-name
 * creates a fresh column family instead of reusing the dangling dropped
 * handle. DBHandles still holding the descriptor keep it alive via their
 * shared_ptr and can continue reading until they close; only the by-name
 * lookup is removed.
 */
void DBDescriptor::unregisterColumnFamily(const std::string& columnName) {
	std::lock_guard<std::mutex> lock(this->columnsMutex);
	if (this->columns.erase(columnName)) {
		DEBUG_LOG("%p DBDescriptor::unregisterColumnFamily unregistered column \"%s\"\n",
			this, columnName.c_str());
	} else {
		DEBUG_LOG("%p DBDescriptor::unregisterColumnFamily column \"%s\" not found\n",
			this, columnName.c_str());
	}
}

/**
 * Called when a lock callback completes (async or sync) to clean up the lock
 * handle and fire the next callback in the queue.
 */
void DBDescriptor::onCallbackComplete(const std::string& key) {
	// try to mark the current callback as complete and fire the next one
	// use a try-catch to handle the case where mutexes might be invalid
	try {
		std::lock_guard<std::mutex> lock(this->locksMutex);
		auto lockHandle = this->locks.find(key);
		if (lockHandle != this->locks.end()) {
			lockHandle->second->isRunning.store(false);
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete marking as complete (key=\"%s\")\n", this, key.c_str());
		} else {
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete lock already removed (key=\"%s\")\n", this, key.c_str());
			return; // lock was already cleaned up, nothing to do
		}
	} catch (const std::exception& e) {
		// the Visual C++ compiler complains that `e` is unused in release
		// builds, so we need to use the `maybe_unused` attribute and this will
		// be optimized out in release builds
		[[maybe_unused]] auto msg = e.what();
		DEBUG_LOG("%p DBDescriptor::onCallbackComplete failed to acquire lock (key=\"%s\"): %s\n", this, key.c_str(), msg);
		return; // mutex is invalid, descriptor is likely being destroyed
	}

	// fire the next callback in the queue
	DEBUG_LOG("%p DBDescriptor::onCallbackComplete firing next callback (key=\"%s\")\n", this, key.c_str());
	try {
		std::unique_lock<std::mutex> lock(this->locksMutex);
		auto lockHandle = this->locks.find(key);

		if (lockHandle == this->locks.end()) {
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete no lock found (key=\"%s\")\n", this, key.c_str());
			return;
		}

		auto& handle = lockHandle->second;

		// try to acquire the "lock" atomically
		bool expected = false;
		if (!handle->isRunning.compare_exchange_strong(expected, true)) {
			// another callback is already running
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete another callback is already running (key=\"%s\")\n", this, key.c_str());
			return;
		}

		// we now "own" the execution for this key
		if (handle->threadsafeCallbacks.empty()) {
			handle->isRunning.store(false);
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete no callbacks left (key=\"%s\"), removing lock\n", this, key.c_str());
			// remove the empty lock handle from the map
			this->locks.erase(key);
			return;
		}

		LockCallback lockCallback = handle->threadsafeCallbacks.front();
		handle->threadsafeCallbacks.pop();
		auto callback = lockCallback.callback;

		// release the mutex before calling the callback to avoid holding locks during callback execution
		lock.unlock();

		DEBUG_LOG("%p DBDescriptor::onCallbackComplete calling callback %p (key=\"%s\")\n", this, callback, key.c_str());

		// create callback data that includes the key for completion and deferred promise
		auto* callbackData = new LockCallbackCompletionData(key, weak_from_this(), lockCallback.deferred);

		// use threadsafe function instead of direct call
		napi_status status = ::napi_call_threadsafe_function(callback, callbackData, napi_tsfn_blocking);
		if (status != napi_ok && status != napi_closing) {
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete failed to call threadsafe function (key=\"%s\")\n", this, key.c_str());
			delete callbackData;
			this->onCallbackComplete(key);
		}

		// release the threadsafe function
		::napi_release_threadsafe_function(callback, napi_tsfn_release);
	} catch (const std::exception& e) {
		// the Visual C++ compiler complains that `e` is unused in release
		// builds, so we need to use the `maybe_unused` attribute and this will
		// be optimized out in release builds
		[[maybe_unused]] auto msg = e.what();
		DEBUG_LOG("%p DBDescriptor::onCallbackComplete failed to fire next callback (key=\"%s\"): %s\n", this, key.c_str(), msg);
	}
}

/**
 * `callJsCallback()` helper macros.
 */
#ifdef DEBUG
	#define CALL_JS_CB_DEBUG_LOG(msg, ...) \
		do { \
			std::string errorStr = rocksdb_js::getNapiExtendedError(env, status); \
			rocksdb_js::debugLog(true, "callJsCallback() " msg ": %s (key=\"%s\")", ##__VA_ARGS__, errorStr.c_str(), callbackData->key.c_str()); \
		} while (0)
#else
	#define CALL_JS_CB_DEBUG_LOG(msg, ...) \
		do { \
			; \
		} while (0)
#endif

#define CALL_JS_CB_NAPI_STATUS_CHECK(call, code, msg, ...) \
	do { \
		napi_status status = (call); \
		if (status != napi_ok) { \
			CALL_JS_CB_DEBUG_LOG(msg, ##__VA_ARGS__); \
			code; \
			return; \
		} \
	} while (0)

/**
 * Custom wrapper used by `napi_call_threadsafe_function()` to call user-
 * defined lock callback function. If the lock callback returns a Promise, it
 * is awaited before calling the `onCallbackComplete()` handler.
 *
 * For example, the callback passed into `db.tryLock()` or `db.withLock()` is
 * what is passed in as `jsCallback`. The code then invokes `jsCallback` and
 * checks if it returned a promise. If it did, it calls `then()` on the promise
 * with resolve and reject callbacks that call `onCallbackComplete()`.
 *
 * This mechanism is key to ensuring that only a single async lock callback
 * is running at a time.
 *
 * Note: Node.js runs this function which ever thread (main or worker) that
 * created the threadsafe function.
 */
static void callJsCallback(napi_env env, napi_value jsCallback, void* context, void* data) {
	if (env == nullptr || jsCallback == nullptr) {
		return;
	}

	// get the callback data from the function's data
	LockCallbackCompletionData* callbackData = static_cast<LockCallbackCompletionData*>(data);
	if (callbackData == nullptr) {
		DEBUG_LOG("callJsCallback callbackData is nullptr - calling js callback\n");
		// this is a tryLock callback - call it without completion callback
		napi_value global;
		napi_status status = ::napi_get_global(env, &global);
		if (status == napi_ok) {
			napi_value result;
			::napi_call_function(env, global, jsCallback, 0, nullptr, &result);
		}
		return;
	}

	// create shared_ptr from raw pointer for RAII management
	std::shared_ptr<LockCallbackCompletionData> callbackDataPtr(callbackData);

	// create a completion callback function
	napi_value completionCallback;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_create_function(
			env,
			"rocksdb-js.lock.callback.complete",
			NAPI_AUTO_LENGTH,
			[](napi_env env, napi_callback_info info) -> napi_value {
				// get the callback data from the function's data
				void* data;
				::napi_get_cb_info(env, info, nullptr, nullptr, nullptr, &data);
				LockCallbackCompletionData* callbackData = static_cast<LockCallbackCompletionData*>(data);

				if (callbackData) {
					// check if this callback is still valid
					if (auto desc = callbackData->descriptor.lock()) {
						// call the completion handler
						DEBUG_LOG("callJsCallback calling onCallbackComplete() (key=\"%s\")\n", callbackData->key.c_str());
						desc->onCallbackComplete(callbackData->key);
					} else {
						DEBUG_LOG("callJsCallback completion callback has no descriptor (key=\"%s\")\n", callbackData->key.c_str());
					}
					delete callbackData;
				}

				NAPI_RETURN_UNDEFINED();
			},
			callbackData,
			&completionCallback
		),
		delete callbackData,
		"failed to create completion callback"
	);

	// call the original callback without any arguments
	napi_value global;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_get_global(env, &global),
		delete callbackData,
		"napi_get_global() failed"
	);

	napi_value result;
	DEBUG_LOG("callJsCallback calling js callback (key=\"%s\")\n", callbackData->key.c_str());
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_call_function(env, global, jsCallback, 0, nullptr, &result),
		{
			if (auto desc = callbackData->descriptor.lock()) {
				desc->onCallbackComplete(callbackData->key);
			}
			delete callbackData;
		},
		"napi_call_function() failed"
	);

	// check if the result is a Promise
	napi_value promiseCtor;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_get_named_property(env, global, "Promise", &promiseCtor),
		// not a promise environment, complete immediately
		if (auto desc = callbackData->descriptor.lock()) {
			desc->onCallbackComplete(callbackData->key);
		}
		delete callbackData,
		"failed to get Promise constructor"
	);

	bool isPromise;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_instanceof(env, result, promiseCtor, &isPromise),
		// assume not a promise, complete immediately
		if (auto desc = callbackData->descriptor.lock()) {
			desc->onCallbackComplete(callbackData->key);
		}
		delete callbackData,
		"napi_instanceof() failed"
	);

	if (!isPromise) {
		DEBUG_LOG("callJsCallback result is not a Promise, completing immediately (key=\"%s\")\n", callbackData->key.c_str());

		// If this is a withLock call with a deferred promise, resolve it
		if (callbackData->deferred != nullptr) {
			DEBUG_LOG("callJsCallback resolving deferred promise for synchronous withLock (key=\"%s\")\n", callbackData->key.c_str());
			napi_value undefined;
			napi_get_undefined(env, &undefined);
			napi_resolve_deferred(env, callbackData->deferred, undefined);
		}

		if (auto desc = callbackData->descriptor.lock()) {
			desc->onCallbackComplete(callbackData->key);
		}
		return;
	}

	DEBUG_LOG("callJsCallback result is a Promise, attaching .then() callback (key=\"%s\")\n", callbackData->key.c_str());

	// get the 'then' method from the promise
	napi_value thenMethod;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_get_named_property(env, result, "then", &thenMethod),
		if (auto desc = callbackData->descriptor.lock()) {
			desc->onCallbackComplete(callbackData->key);
		}
		delete callbackData,
		"failed to get .then() method"
	);

	// create resolve and reject callbacks that both complete the lock
	// we need to store the shared_ptr in a way N-API callbacks can access it
	auto* resolveDataPtr = new std::shared_ptr<LockCallbackCompletionData>(callbackDataPtr);

	napi_value resolveCallback;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_create_function(
			env,
			"rocksdb-js.lock.callback.resolve",
			NAPI_AUTO_LENGTH,
			[](napi_env env, napi_callback_info info) -> napi_value {
				napi_value result;
				::napi_get_undefined(env, &result);

				DEBUG_LOG("callJsCallback promise resolve callback\n");

				void* data;
				::napi_get_cb_info(env, info, nullptr, nullptr, nullptr, &data);
				auto* callbackDataPtr = static_cast<std::shared_ptr<LockCallbackCompletionData>*>(data);

				if (callbackDataPtr && *callbackDataPtr) {
					auto& callbackData = **callbackDataPtr;
					auto desc = callbackData.descriptor.lock();
					if (!callbackData.completed.exchange(true) && desc) {
						DEBUG_LOG("callJsCallback promise resolved, calling onCallbackComplete() (key=\"%s\")\n", callbackData.key.c_str());

						// if this is a withLock call with a deferred promise, resolve it
						if (callbackData.deferred != nullptr) {
							DEBUG_LOG("callJsCallback resolving deferred promise for withLock (key=\"%s\")\n", callbackData.key.c_str());
							napi_value undefined;
							napi_get_undefined(env, &undefined);
							napi_resolve_deferred(env, callbackData.deferred, undefined);
						}

						desc->onCallbackComplete(callbackData.key);
					} else {
						DEBUG_LOG("callJsCallback promise resolve callback already completed (key=\"%s\")\n", callbackData.key.c_str());
					}
				}

				// clean up the shared_ptr wrapper
				delete callbackDataPtr;
				return result;
			},
			resolveDataPtr,
			&resolveCallback
		),
		/* cleanup */ {
			if (auto desc = callbackData->descriptor.lock()) {
				desc->onCallbackComplete(callbackData->key);
			}
			delete resolveDataPtr;
		},
		"failed to create resolve callback"
	);

	// create reject callback - shared_ptr handles safe sharing between resolve/reject
	auto* rejectDataPtr = new std::shared_ptr<LockCallbackCompletionData>(callbackDataPtr);

	napi_value rejectCallback;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_create_function(
			env,
			"rocksdb-js.lock.callback.reject",
			NAPI_AUTO_LENGTH,
			[](napi_env env, napi_callback_info info) -> napi_value {
				napi_value result;
				::napi_get_undefined(env, &result);

				void* data;
				::napi_get_cb_info(env, info, nullptr, nullptr, nullptr, &data);
				auto* callbackDataPtr = static_cast<std::shared_ptr<LockCallbackCompletionData>*>(data);

				if (callbackDataPtr && *callbackDataPtr) {
					auto& callbackData = **callbackDataPtr;
					if (auto desc = callbackData.descriptor.lock()) {
						DEBUG_LOG("callJsCallback promise rejected, calling onCallbackComplete() (key=\"%s\")\n", callbackData.key.c_str());

						// if this is a withLock call with a deferred promise, reject it
						if (callbackData.deferred != nullptr) {
							DEBUG_LOG("callJsCallback rejecting deferred promise for withLock (key=\"%s\")\n", callbackData.key.c_str());
							// get the error from the first argument of the reject callback
							size_t argc = 1;
							napi_value argv[1];
							napi_get_cb_info(env, info, &argc, argv, nullptr, nullptr);
							napi_value error = argc > 0 ? argv[0] : nullptr;
							if (error == nullptr) {
								napi_get_undefined(env, &error);
							}
							napi_reject_deferred(env, callbackData.deferred, error);
						}

						desc->onCallbackComplete(callbackData.key);
					}
				}

				// clean up the shared_ptr wrapper
				delete callbackDataPtr;
				return result;
			},
			rejectDataPtr,
			&rejectCallback
		),
		/* cleanup */ {
			if (auto desc = callbackData->descriptor.lock()) {
				desc->onCallbackComplete(callbackData->key);
			}
			delete rejectDataPtr;
			delete resolveDataPtr;
		},
		"failed to create reject callback"
	);

	// call `promise.then(resolveCallback, rejectCallback)` for key "key"
	napi_value thenArgs[] = { resolveCallback, rejectCallback };
	napi_value thenResult;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_call_function(env, result, thenMethod, 2, thenArgs, &thenResult),
		{
			if (auto desc = callbackData->descriptor.lock()) {
				desc->onCallbackComplete(callbackData->key);
			}
			delete resolveDataPtr;
			delete rejectDataPtr;
		},
		"failed to call .then()"
	);
}

/**
 * Finalize callback for when the user shared ArrayBuffer is garbage collected.
 * It removes the corresponding entry from the `userSharedBuffers` map to and
 * calls the finalize function, which removes the event listener, if applicable.
 */
static void userSharedBufferFinalize(napi_env env, void* unusedData, void* hint) {
	auto* finalizeData = static_cast<UserSharedBufferFinalizeData*>(hint);

	if (auto dbHandle = finalizeData->dbHandle.lock()) {
		DEBUG_LOG("userSharedBufferFinalize GC'd dbHandle=%p\n", dbHandle.get());
		if (finalizeData->callbackRef) {
			napi_value callback;
			if (::napi_get_reference_value(env, finalizeData->callbackRef, &callback) == napi_ok) {
				DEBUG_LOG("%p userSharedBufferFinalize removing listener for key:", dbHandle.get());
				DEBUG_LOG_KEY_LN(finalizeData->key);
				if (dbHandle->descriptor) {
					DEBUG_LOG("%p userSharedBufferFinalize descriptor is still alive %p", dbHandle.get(), dbHandle->descriptor.get());
					dbHandle->descriptor->removeListener(env, finalizeData->key, callback);
				} else {
					DEBUG_LOG("%p userSharedBufferFinalize descriptor is not alive %p", dbHandle.get(), dbHandle->descriptor.get());
				}
			}
			finalizeData->callbackRef = nullptr;
		}
	} else {
		DEBUG_LOG("userSharedBufferFinalize GC'd dbHandle was already destroyed for key:");
		DEBUG_LOG_KEY_LN(finalizeData->key);
	}

	if (auto columnDescriptor = finalizeData->columnDescriptor.lock()) {
		if (finalizeData->sharedData) {
			DEBUG_LOG("%p userSharedBufferFinalize releasing user shared buffer (column=%p) for key:", columnDescriptor.get(), columnDescriptor->column.get());
			DEBUG_LOG_KEY(finalizeData->key);
			DEBUG_LOG_MSG(" (use_count: %ld)\n", finalizeData->sharedData.use_count());
			columnDescriptor->releaseUserSharedBuffer(finalizeData->key, finalizeData->sharedData);
		}
	} else {
		DEBUG_LOG("userSharedBufferFinalize columnDescriptor was already destroyed for key:");
		DEBUG_LOG_KEY_LN(finalizeData->key);
	}

	// Destroying finalizeData drops the last strong ref to the shared data
	// when this was the final external ArrayBuffer; the buffer storage is
	// released here rather than in DBDescriptor::close().
	delete finalizeData;
}

napi_value DBDescriptor::getUserSharedBuffer(
	napi_env env,
	std::string& key,
	std::shared_ptr<DBHandle> dbHandle,
	napi_value defaultBuffer,
	napi_ref callbackRef
) {
	bool isArrayBuffer;
	NAPI_STATUS_THROWS(::napi_is_arraybuffer(env, defaultBuffer, &isArrayBuffer));
	if (!isArrayBuffer) {
		::napi_throw_error(env, nullptr, "Default buffer must be an ArrayBuffer");
		return nullptr;
	}

	std::lock_guard<std::mutex> lock(dbHandle->columnDescriptor->userSharedBuffersMutex);

	auto userSharedBufferIter = dbHandle->columnDescriptor->userSharedBuffers.find(key);
	if (userSharedBufferIter == dbHandle->columnDescriptor->userSharedBuffers.end()) {
		// shared buffer does not exist, create it
		void* data;
		size_t size;

		NAPI_STATUS_THROWS(::napi_get_arraybuffer_info(
			env,
			defaultBuffer,
			&data,
			&size
		));

		DEBUG_LOG("%p DBHandle::getUserSharedBuffer Initializing user shared buffer with default buffer size: %zu\n", this, size);
		userSharedBufferIter = dbHandle->columnDescriptor->userSharedBuffers.emplace(key, std::make_shared<UserSharedBufferData>(data, size)).first;
	} else {
		DEBUG_LOG("%p DBHandle::getUserSharedBuffer User shared buffer already initialized for key:", this);
	}

	auto userSharedBuffer = userSharedBufferIter->second;

	DEBUG_LOG("%p DBHandle::getUserSharedBuffer Creating external ArrayBuffer with size %zu for key:", this, userSharedBuffer->size);
	DEBUG_LOG_KEY_LN(key);

	// Hold a strong ref to the user shared buffer data here so the external
	// ArrayBuffer's storage outlives DBDescriptor / ColumnFamilyDescriptor
	// teardown (the map may be cleared on close() while JS still retains the
	// ArrayBuffer). The data is released when this finalize data is destroyed.
	auto* finalizeData = new UserSharedBufferFinalizeData(
		key,
		std::weak_ptr<DBHandle>(dbHandle),
		std::weak_ptr<ColumnFamilyDescriptor>(dbHandle->columnDescriptor),
		userSharedBuffer,
		callbackRef
	);

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_external_arraybuffer(
		env,
		userSharedBuffer->data,   // data
		userSharedBuffer->size,   // size
		userSharedBufferFinalize, // finalize_cb
		finalizeData,             // finalize_hint
		&result                   // [out] result
	));
	return result;
}

/**
 * Adds an event listener to this descriptor's event emitter.
 */
napi_ref DBDescriptor::addListener(
	napi_env env,
	std::string& key,
	napi_value callback,
	std::weak_ptr<DBHandle> owner
) {
	// Convert the typed weak_ptr to a type-erased weak_ptr<void> for the
	// EventEmitter. The pointer value held by the weak_ptr is preserved
	// because DBHandle has no virtual/multiple-inheritance offset for void*.
	auto sp = owner.lock();
	std::weak_ptr<void> erasedOwner;
	if (sp) {
		erasedOwner = std::shared_ptr<void>(sp, sp.get());
	}
	return this->events.addListener(env, key, callback, erasedOwner);
}

bool DBDescriptor::notify(std::string key, ListenerData* data) {
	return this->events.notify(key, data);
}

napi_value DBDescriptor::listeners(napi_env env, std::string& key) {
	return this->events.listeners(env, key);
}

napi_value DBDescriptor::removeListener(napi_env env, std::string& key, napi_value callback) {
	return this->events.removeListener(env, key, callback);
}

void DBDescriptor::removeListenersByOwner(DBHandle* owner) {
	this->events.removeListenersByOwner(static_cast<void*>(owner));
}

void DBDescriptor::removeListenersByEnv(napi_env env) {
	this->events.removeListenersByEnv(env);
}

/**
 * Lists all transaction logs in the database.
 *
 * @param env The environment of the current callback.
 */
napi_value DBDescriptor::listTransactionLogStores(napi_env env) {
	return TransactionLogStoreRegistry::ListStores(env, this->path);
}

/**
 * Purges transaction logs.
 */
napi_value DBDescriptor::purgeTransactionLogs(napi_env env, napi_value options) {
	return TransactionLogStoreRegistry::PurgeStores(env, this->path, options);
}

/**
 * Finds or creates a transaction log store by name.
 *
 * @param name The name of the transaction log store.
 * @returns The transaction log store.
 */
std::shared_ptr<TransactionLogStore> DBDescriptor::resolveTransactionLogStore(const std::string& name) {
	return TransactionLogStoreRegistry::ResolveStore(this->path, name);
}

rocksdb::Status DBDescriptor::flush() {
	if (this->readOnly) {
		DEBUG_LOG("%p DBDescriptor::flush Skipping flush for readonly database\n", this);
		return rocksdb::Status::OK();
	}

	// Snapshot the column family descriptors under the columns mutex. flush()
	// can run on a libuv worker thread while the JS thread drops a column
	// family (which erases from the map); the shared_ptr copies also pin the
	// handles so they cannot be destroyed mid-Flush.
	std::vector<std::shared_ptr<ColumnFamilyDescriptor>> pinnedColumns;
	{
		std::lock_guard<std::mutex> lock(this->columnsMutex);
		pinnedColumns.reserve(this->columns.size());
		for (const auto& [name, columnDescriptor] : this->columns) {
			pinnedColumns.push_back(columnDescriptor);
		}
	}
	std::vector<rocksdb::ColumnFamilyHandle*> columnHandles;
	columnHandles.reserve(pinnedColumns.size());
	for (const auto& columnDescriptor : pinnedColumns) {
		columnHandles.push_back(columnDescriptor->column.get());
	}
	// Perform flush
	rocksdb::FlushOptions flushOptions;
	return this->db->Flush(
		flushOptions,
		columnHandles
	);
}

rocksdb::Status DBDescriptor::compactRange(
	rocksdb::ColumnFamilyHandle* column,
	const rocksdb::Slice* start,
	const rocksdb::Slice* end
) {
	std::lock_guard<std::mutex> lock(this->compactMutex);
	DEBUG_LOG("%p DBDescriptor::compactRange Compacting range\n", this);
	return this->db->CompactRange(
		rocksdb::CompactRangeOptions(),
		column,
		start,
		end
	);
}

} // namespace rocksdb_js
