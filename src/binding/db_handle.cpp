#include <algorithm>
#include "db_handle.h"
#include "db_descriptor.h"
#include "db_registry.h"

namespace rocksdb_js {

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
	rocksdb::Status status = this->descriptor->db->CompactRange(
		rocksdb::CompactRangeOptions(),
		this->columnDescriptor->column.get(),
		nullptr,
		nullptr
	);
	if (!status.ok()) {
		return status;
	}
	// it appears we do not need to call WaitForCompact for this to work
	return rocksdb::DeleteFilesInRange(this->descriptor->db.get(), this->columnDescriptor->column.get(), nullptr, nullptr);
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
	// check if this is an internal stat first?
	uint64_t value = 0; \
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
		if (success) { \
			NAPI_STATUS_THROWS(::napi_create_int64(env, value, &jsValue)); \
			NAPI_STATUS_THROWS(::napi_set_named_property(env, result, name, jsValue)); \
		} else { \
			NAPI_STATUS_THROWS(::napi_get_undefined(env, &jsValue)); \
			NAPI_STATUS_THROWS(::napi_set_named_property(env, result, name, jsValue)); \
		} \
	} while (0)

napi_value DBHandle::getStats(napi_env env, bool all) {
	napi_value result = this->descriptor->getStats(env, all);

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

	return result;
}

/**
 * Has the DBRegistry open a RocksDB database and then move it's handle properties
 * to this DBHandle.
 *
 * @param path - The filesystem path to the database.
 * @param options - The options for the database.
 */
void DBHandle::open(const std::string& path, const DBOptions& options) {
	auto handleParams = DBRegistry::OpenDB(path, options);
	this->columnDescriptor = std::move(handleParams->columnDescriptor);
	this->descriptor = std::move(handleParams->descriptor);
	this->disableWAL = options.disableWAL;
	this->path = path;

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

	return instance;
}

} // namespace rocksdb_js
