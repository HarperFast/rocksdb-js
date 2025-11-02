#include <algorithm>
#include "db_handle.h"
#include "db_descriptor.h"
#include "db_registry.h"
#include "transaction_log.h"

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
	DEBUG_LOG("%p DBHandle::~DBHandle\n", this)
	this->close();
}

/**
 * Clears all data in the database's column family.
 */
rocksdb::Status DBHandle::clear(uint32_t batchSize, uint64_t& deleted) {
	ASSERT_OPENED_AND_NOT_CANCELLED(this, "clear")

	// create a write batch and iterator
	rocksdb::WriteBatch batch;
	std::unique_ptr<rocksdb::Iterator> it = std::unique_ptr<rocksdb::Iterator>(
		this->descriptor->db->NewIterator(
			rocksdb::ReadOptions(),
			this->column.get()
		)
	);

	DEBUG_LOG("%p DBHandle::Clear Starting clear with batch size %u\n", this, batchSize)

	// iterate over the database and add each key to the write batch
	rocksdb::Status status;
	deleted = 0;
	it->SeekToFirst();
	bool valid = it->Valid();
	while (valid) {
		ASSERT_OPENED_AND_NOT_CANCELLED(this, "clear")

		batch.Delete(it->key());
		++deleted;
		it->Next();
		valid = it->Valid();

		// if we've reached the end of the iterator or the batch is full, write the batch
		if (!valid || batch.Count() >= batchSize) {
			ASSERT_OPENED_AND_NOT_CANCELLED(this, "clear")
			DEBUG_LOG("%p DBHandle::Clear Writing batch with %zu keys\n", this, batch.Count())

			rocksdb::WriteOptions writeOptions;
			writeOptions.disableWAL = this->disableWAL;

			status = this->descriptor->db->Write(writeOptions, &batch);
			if (!status.ok()) {
				return status;
			}

			batch.Clear();
		}
	}

	// Check one final time before compaction
	ASSERT_OPENED_AND_NOT_CANCELLED(this, "clear")

	// compact the database to reclaim space
	return this->descriptor->db->CompactRange(
		rocksdb::CompactRangeOptions(),
		this->column.get(),
		nullptr,
		nullptr
	);
}

/**
 * Closes the DBHandle.
 */
void DBHandle::close() {
	DEBUG_LOG("%p DBHandle::close dbDescriptor=%p (ref count = %ld)\n", this, this->descriptor.get(), this->descriptor.use_count())

	// cancel all active async work before closing
	this->cancelAllAsyncWork();

	// wait for all async work to complete before closing
	this->waitForAsyncWorkCompletion();

	// decrement the reference count on the column and descriptor
	if (this->column) {
		this->column.reset();
	}

	if (this->descriptor) {
		// clean up listeners owned by this handle before releasing locks
		this->descriptor->removeListenersByOwner(this);

		this->descriptor->lockReleaseByOwner(this);
		this->descriptor.reset();
	}

	// clean up transaction log references
	for (auto& [name, ref] : this->logRefs) {
		DEBUG_LOG("%p DBHandle::close Releasing transaction log reference %s\n", this, name.c_str())
		::napi_delete_reference(this->env, ref);
	}
	this->logRefs.clear();

	DEBUG_LOG("%p DBHandle::close Handle closed\n", this)
}

/**
 * Adds an listener to the database descriptor.
 *
 * @param env The environment of the current callback.
 * @param key The key.
 * @param callback The callback to call when the event is emitted.
 */
napi_ref DBHandle::addListener(napi_env env, std::string key, napi_value callback) {
	return this->descriptor->addListener(env, key, callback, weak_from_this());
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
	this->column = std::move(handleParams->column);
	this->descriptor = std::move(handleParams->descriptor);
	this->disableWAL = options.disableWAL;
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
		DEBUG_LOG("%p DBHandle::unrefLog Transaction log \"%s\" not found (size=%zu)\n", this, name.c_str(), this->logRefs.size())
		return;
	}

	DEBUG_LOG("%p DBHandle::unrefLog Unreferencing transaction log \"%s\" (size=%zu)\n", this, name.c_str(), this->logRefs.size())
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
			// DEBUG_LOG("%p DBHandle::useLog Returning existing transaction log \"%s\"\n", this, name.c_str())
			return instance;
		}

		DEBUG_LOG("%p DBHandle::useLog Removing stale reference to transaction log \"%s\"\n", this, name.c_str())
		::napi_delete_reference(env, existingRef->second);
		this->logRefs.erase(name);
	}

	DEBUG_LOG("%p DBHandle::useLog Creating new transaction log \"%s\"\n", this, name.c_str())

	napi_value exports;
	NAPI_STATUS_THROWS(::napi_get_reference_value(env, this->exportsRef, &exports))

	napi_value args[2];
	args[0] = jsDatabase;

	napi_value transactionLogCtor;
	NAPI_STATUS_THROWS(::napi_get_named_property(env, exports, "TransactionLog", &transactionLogCtor))

	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, name.c_str(), name.size(), &args[1]))

	NAPI_STATUS_THROWS(::napi_new_instance(env, transactionLogCtor, 2, args, &instance))

	napi_ref ref;
	NAPI_STATUS_THROWS(::napi_create_reference(env, instance, 0, &ref))
	this->logRefs.emplace(name, ref);

	return instance;
}

} // namespace rocksdb_js
