#include "transaction_log/transaction_log_store.h"
#include "database/db_handle.h"
#include "database/db_descriptor.h"
#include "database/db_registry.h"
#include "database/db_settings.h"
#include "transaction_log/transaction_log_store_registry.h"
#include "core/verification_table.h"

namespace rocksdb_js {

namespace {

bool lookupTxnlogSummaryStat(
	const std::string& statName,
	const TransactionLogStoreStats& total,
	uint64_t logCount,
	double& value
) {
	if (statName == TRANSACTION_LOG_SUMMARY_LOG_COUNT_KEY) {
		value = static_cast<double>(logCount);
		return true;
	}
#define X(key, field) \
	else if (statName == key) { \
		value = static_cast<double>(total.field); \
		return true; \
	}
	TRANSACTION_LOG_SUMMARY_STATS(X)
#undef X
	return false;
}

void addTxnlogStoreStats(TransactionLogStoreStats& total, const TransactionLogStoreStats& s) {
#define X(key, field) total.field += s.field;
	TRANSACTION_LOG_SUMMARY_STATS(X)
#undef X
}

void setTxnlogSummaryStatsOnObject(
	napi_env env,
	napi_value result,
	const TransactionLogStoreStats& total,
	uint64_t logCount
) {
	napi_value jsValue;
	if (::napi_create_double(env, static_cast<double>(logCount), &jsValue) == napi_ok) {
		::napi_set_named_property(env, result, TRANSACTION_LOG_SUMMARY_LOG_COUNT_KEY, jsValue);
	}
#define X(key, field) \
	do { \
		napi_value _txnlogValue; \
		if (::napi_create_double(env, static_cast<double>(total.field), &_txnlogValue) == napi_ok) { \
			::napi_set_named_property(env, result, key, _txnlogValue); \
		} \
	} while (0);
	TRANSACTION_LOG_SUMMARY_STATS(X)
#undef X
}

} // namespace

/**
 * Creates a new DBHandle.
 */
DBHandle::DBHandle(napi_env env, napi_ref exportsRef)
	: descriptor(nullptr), env(env), exportsRef(exportsRef) {}

/**
 * Close the DBHandle and destroy it.
 */
DBHandle::~DBHandle() {
	DEBUG_LOG("%p DBHandle::~DBHandle\n", this);
	this->close();
}

/**
 * Clears all data in the database's column family.
 */
rocksdb::Status DBHandle::clear() {
	if (!this->opened() || this->isCancelled()) {
		DEBUG_LOG("%p Database closed during clear operation\n", this);
		return rocksdb::Status::Aborted("Database closed during clear operation");
	}

	// compact the database to reclaim space
	rocksdb::Status status = this->descriptor->compactRange(
		this->columnDescriptor->column.get(),
		nullptr,
		nullptr
	);
	if (!status.ok()) {
		// A dropped column family is effectively already empty — clear is a no-op.
		// Callers that subsequently write to the same handle will receive
		// kColumnFamilyDropped at write time and can handle recovery there.
		if (status.IsColumnFamilyDropped()) {
			return rocksdb::Status::OK();
		}
		return status;
	}
	// it appears we do not need to call WaitForCompact for this to work
	rocksdb::Status clearStatus = rocksdb::DeleteFilesInRange(
		this->descriptor->db.get(), this->columnDescriptor->column.get(), nullptr, nullptr
	);
	// After data is deleted, advance all non-lock VT slots to fresh
	// settled-empty generations. This prevents stale pre-clear versions from
	// being re-published via a concurrent populate CAS. The sweep is coarse
	// (covers all slots in the process-global table) since slot provenance is
	// not tracked for non-lock slots; the cost is spurious cache misses for
	// other stores, which is acceptable given clear() is already a rare,
	// expensive operation. Lock slots are skipped — the write-intent lifecycle
	// handles them independently. We sweep regardless of clearStatus: if the
	// delete partially succeeded, any keys that were removed must not remain
	// cacheable via stale VT entries.
	if (this->enableVerificationTable) {
		VerificationTable* vt = DBSettings::getInstance().getVerificationTableRaw();
		if (vt) vt->settleAllSlots();
	}
	return clearStatus;
}

/**
 * Closes the DBHandle.
 */
void DBHandle::close() {
	DEBUG_LOG("%p DBHandle::close dbDescriptor=%p (ref count = %ld)\n", this, this->descriptor.get(), this->descriptor.use_count());

	// cancel all active async work before closing
	this->cancelAllAsyncWork();

	// wait for all async work to complete before closing
	this->waitForAsyncWorkCompletion();

	// decrement the reference count on the column and descriptor
	if (this->columnDescriptor) {
		this->columnDescriptor.reset();
	}

	if (this->descriptor) {
		// clean up listeners owned by this handle before releasing locks
		this->descriptor->removeListenersByOwner(this);
		this->descriptor->lockReleaseByOwner(this);

		// release our reference to the descriptor
		this->descriptor.reset();
	}

	// clean up transaction log references
	for (auto& [name, ref] : this->logRefs) {
		DEBUG_LOG("%p DBHandle::close Releasing transaction log JS reference \"%s\"\n", this, name.c_str());
		::napi_delete_reference(this->env, ref);
	}
	this->logRefs.clear();

	DEBUG_LOG("%p DBHandle::close Handle closed\n", this);
}

rocksdb::ColumnFamilyHandle* DBHandle::getColumnFamilyHandle() const {
	return this->columnDescriptor->column.get();
}

std::string DBHandle::getColumnFamilyName() const {
	return this->columnDescriptor->column->GetName();
}

napi_value DBHandle::getStat(napi_env env, const std::string& statName) {
	// transaction log summary stats are computed here (not RocksDB tickers or
	// column-family properties), so resolve them before anything else.
	if (statName.rfind("txnlog.", 0) == 0) {
		TransactionLogStoreStats total;
		uint64_t logCount = 0;
		this->collectTransactionLogSummary(total, logCount);
		double txnlogValue = 0;
		bool found = lookupTxnlogSummaryStat(statName, total, logCount, txnlogValue);
		napi_value jsValue;
		if (found) {
			NAPI_STATUS_THROWS(::napi_create_double(env, txnlogValue, &jsValue));
		} else {
			// unknown txnlog.* key: it is never a RocksDB ticker or column-family
			// property, so return undefined rather than falling through (which
			// would throw when statistics are disabled).
			NAPI_STATUS_THROWS(::napi_get_undefined(env, &jsValue));
		}
		return jsValue;
	}

	// check if this is an internal stat first?
	uint64_t value = 0;
	bool success = this->descriptor->db->GetIntProperty(this->getColumnFamilyHandle(), statName, &value);
	if (success) {
		napi_value jsValue;
		NAPI_STATUS_THROWS(::napi_create_int64(env, value, &jsValue));
		return jsValue;
	}

	// not an internal stat, try getting it from the statistics
	return this->descriptor->getStat(env, statName);
}

#define SET_INTERNAL_STAT(result, name) \
	do { \
		uint64_t value = 0; \
		napi_value jsValue; \
		bool success = this->descriptor->db->GetIntProperty(this->getColumnFamilyHandle(), name, &value); \
		napi_status status; \
		if (success) { \
			status = ::napi_create_int64(env, value, &jsValue); \
		} else { \
			status = ::napi_get_undefined(env, &jsValue); \
		} \
		if (status == napi_ok) { \
			::napi_set_named_property(env, result, name, jsValue); \
		} \
	} while (0)

napi_value DBHandle::getStats(napi_env env, bool all) {
	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_object(env, &result));

	this->descriptor->getStats(env, all, &result);

	// memtable
	SET_INTERNAL_STAT(result, "rocksdb.num-immutable-mem-table");
	SET_INTERNAL_STAT(result, "rocksdb.num-immutable-mem-table-flushed");
	SET_INTERNAL_STAT(result, "rocksdb.mem-table-flush-pending");
	SET_INTERNAL_STAT(result, "rocksdb.cur-size-active-mem-table");
	SET_INTERNAL_STAT(result, "rocksdb.cur-size-all-mem-tables");
	SET_INTERNAL_STAT(result, "rocksdb.size-all-mem-tables");
	SET_INTERNAL_STAT(result, "rocksdb.num-entries-active-mem-table");
	SET_INTERNAL_STAT(result, "rocksdb.num-deletes-active-mem-table");

	// compaction
	SET_INTERNAL_STAT(result, "rocksdb.compaction-pending");
	SET_INTERNAL_STAT(result, "rocksdb.estimate-pending-compaction-bytes");
	SET_INTERNAL_STAT(result, "rocksdb.num-running-compactions");
	SET_INTERNAL_STAT(result, "rocksdb.num-running-flushes");

	// sst
	SET_INTERNAL_STAT(result, "rocksdb.total-sst-files-size");
	SET_INTERNAL_STAT(result, "rocksdb.live-sst-files-size");

	// data size
	SET_INTERNAL_STAT(result, "rocksdb.estimate-live-data-size");
	SET_INTERNAL_STAT(result, "rocksdb.estimate-num-keys");

	// block cache
	SET_INTERNAL_STAT(result, "rocksdb.block-cache-capacity");
	SET_INTERNAL_STAT(result, "rocksdb.block-cache-usage");
	SET_INTERNAL_STAT(result, "rocksdb.block-cache-pinned-usage");

	// snapshots
	SET_INTERNAL_STAT(result, "rocksdb.num-live-versions");
	SET_INTERNAL_STAT(result, "rocksdb.current-super-version-number");
	SET_INTERNAL_STAT(result, "rocksdb.oldest-snapshot-time");

	// blobs
	SET_INTERNAL_STAT(result, "rocksdb.num-blob-files");
	SET_INTERNAL_STAT(result, "rocksdb.total-blob-file-size");
	SET_INTERNAL_STAT(result, "rocksdb.live-blob-file-size");

	// transaction log summary, aggregated across all of this database's logs.
	// This is independent of the RocksDB statistics gate above, so it appears
	// even when statistics are disabled. The same keys are retrievable
	// individually via `db.getStat()`; per-log detail is available via
	// `log.getStats()`. All keys are documented in docs/stats.md.
	{
		TransactionLogStoreStats total;
		uint64_t logCount = 0;
		this->collectTransactionLogSummary(total, logCount);
		setTxnlogSummaryStatsOnObject(env, result, total, logCount);
	}

	return result;
}

void DBHandle::collectTransactionLogSummary(TransactionLogStoreStats& total, uint64_t& logCount) {
	auto stores = TransactionLogStoreRegistry::GetStores(this->descriptor->path);
	logCount = 0;
	for (const auto& store : stores) {
		if (!store) {
			continue;
		}
		TransactionLogStoreStats s;
		store->collectStats(s);
		logCount++;
		addTxnlogStoreStats(total, s);
	}
}

/**
 * Has the DBRegistry open a RocksDB database and then move it's handle properties
 * to this DBHandle.
 *
 * @param path - The filesystem path to the database.
 * @param options - The options for the database.
 */
void DBHandle::open(const std::string& path, const DBOptions& options) {
	// Reset the cancelled state in case this handle was previously closed
	// and is being re-opened
	this->resetCancelled();

	this->path = path;

	auto handleParams = DBRegistry::OpenDB(path, options);
	this->columnDescriptor = std::move(handleParams->columnDescriptor);
	this->descriptor = std::move(handleParams->descriptor);
	this->disableWAL = options.disableWAL;
	this->enableVerificationTable = options.verificationTable;

	// Note: We cannot attach this handle to the descriptor because we don't
	// have the smart pointer to the dbHandle instance, so the caller needs to
	// do it.

	// at this point, the DBDescriptor has at least 2 refs: the registry and this handle
}

/**
 * Checks if the referenced database is opened.
 */
bool DBHandle::opened() const {
	if (this->descriptor && this->descriptor->db) {
		return true;
	}
	return false;
}

/**
 * Unreferences a transaction log instance.
 */
void DBHandle::unrefLog(const std::string& name) {
	auto it = this->logRefs.find(name);
	if (it == this->logRefs.end()) {
		DEBUG_LOG("%p DBHandle::unrefLog Transaction log \"%s\" not found (size=%zu)\n", this, name.c_str(), this->logRefs.size());
		return;
	}

	DEBUG_LOG("%p DBHandle::unrefLog Unreferencing transaction log \"%s\" (size=%zu)\n", this, name.c_str(), this->logRefs.size());
	::napi_delete_reference(this->env, it->second);
	this->logRefs.erase(it);
}

/**
 * Get or create a transaction log instance.
 */
napi_value DBHandle::useLog(napi_env env, napi_value jsDatabase, std::string& name) {
	napi_value instance;

	// check if we already have it cached
	auto existingRef = this->logRefs.find(name);
	if (existingRef != this->logRefs.end()) {
		napi_status status = ::napi_get_reference_value(env, existingRef->second, &instance);

		if (status == napi_ok && instance != nullptr) {
			// DEBUG_LOG("%p DBHandle::useLog Returning existing transaction log \"%s\"\n", this, name.c_str());
			return instance;
		}

		DEBUG_LOG("%p DBHandle::useLog Removing stale reference to transaction log \"%s\"\n", this, name.c_str());
		::napi_delete_reference(env, existingRef->second);
		this->logRefs.erase(name);
	}

	DEBUG_LOG("%p DBHandle::useLog Creating new transaction log \"%s\"\n", this, name.c_str());

	napi_value exports;
	NAPI_STATUS_THROWS(::napi_get_reference_value(env, this->exportsRef, &exports));

	napi_value args[2];
	args[0] = jsDatabase;

	napi_value transactionLogCtor;
	NAPI_STATUS_THROWS(::napi_get_named_property(env, exports, "TransactionLog", &transactionLogCtor));

	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, name.c_str(), name.size(), &args[1]));

	NAPI_STATUS_THROWS(::napi_new_instance(env, transactionLogCtor, 2, args, &instance));

	napi_ref ref;
	NAPI_STATUS_THROWS(::napi_create_reference(env, instance, 0, &ref));
	this->logRefs.emplace(name, ref);

	this->descriptor->notify("new-transaction-log", ListenerData::fromStrings({ name }));

	return instance;
}

} // namespace rocksdb_js
