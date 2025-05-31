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
 * Closes the DBHandle.
 */
void DBHandle::close() {
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
