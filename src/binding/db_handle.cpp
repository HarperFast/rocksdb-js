#include "db_handle.h"
#include "db_descriptor.h"
#include "db_registry.h"
#include <algorithm>

namespace rocksdb_js {

/**
 * Creates a new DBHandle.
 */
DBHandle::DBHandle()
	: descriptor(nullptr) {}

/**
 * Creates a new DBHandle from a DBDescriptor.
 */
DBHandle::DBHandle(
	std::shared_ptr<DBDescriptor> descriptor,
	const DBOptions& options
) : descriptor(descriptor),
	disableWAL(options.disableWAL) {}

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
	auto handle = DBRegistry::OpenDB(path, options);
	this->column = std::move(handle->column);
	this->descriptor = std::move(handle->descriptor);
	// at this point, the DBDescriptor has at least 2 refs: the registry and this handle
	DEBUG_LOG("%p DBHandle::open dbhandle %p is no longer needed, moved DBDescriptor %p to this handle (ref count = %ld)\n",
		this, handle.get(), this->descriptor.get(), this->descriptor.use_count())
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
 * Get or create a transaction log.
 */
napi_value DBHandle::useLog(napi_env env, std::string& name) {
	NAPI_RETURN_UNDEFINED()
}

} // namespace rocksdb_js
