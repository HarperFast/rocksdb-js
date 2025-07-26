#include "db_handle.h"
#include "db_registry.h"

namespace rocksdb_js {

/**
 * Creates a new DBHandle.
 */
DBHandle::DBHandle()
	: descriptor(nullptr) {}

/**
 * Creates a new DBHandle from a DBDescriptor.
 */
DBHandle::DBHandle(std::shared_ptr<DBDescriptor> descriptor)
	: descriptor(descriptor) {}

/**
 * Close the DBHandle and destroy it.
 */
DBHandle::~DBHandle() {
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

			status = this->descriptor->db->Write(rocksdb::WriteOptions(), &batch);
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
	DEBUG_LOG("%p DBHandle::close() dbDescriptor=%p\n", this, this->descriptor.get())

	// cancel all active async work before closing
	this->cancelAllAsyncWork();

	// wait for all async work to complete before closing
	this->waitForAsyncWorkCompletion();

	// decrement the reference count on the column and descriptor
	if (this->column) {
		this->column.reset();
	}

	if (this->descriptor) {
		this->descriptor.reset();
	}

	// purge all weak references in the registry
	DBRegistry::Purge();
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
	DEBUG_LOG("%p DBHandle::open dbhandle %p is no longer needed\n", this, handle.get())
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

} // namespace rocksdb_js
