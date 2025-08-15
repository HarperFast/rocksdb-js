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
	DEBUG_LOG("%p DBHandle::~DBHandle\n", this)
	this->close();
}

/**
 * Closes the DBHandle.
 */
void DBHandle::close() {
	DEBUG_LOG("%p DBHandle::close dbDescriptor=%p (ref count = %ld)\n", this, this->descriptor.get(), this->descriptor.use_count())

	if (this->column) {
		this->column.reset();
	}

	if (this->descriptor) {
		this->descriptor->lockReleaseByOwner(this);
		this->descriptor.reset();
	}

	DEBUG_LOG("%p DBHandle::close Handle closed\n", this)
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
	DEBUG_LOG("%p DBHandle::open dbhandle %p is no longer needed, moved DBDescriptor %p to this handle (ref count = %ld)\n", this, handle.get(), this->descriptor.get(), this->descriptor.use_count())
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
