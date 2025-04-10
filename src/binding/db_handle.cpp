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
	this->column.reset();
	this->descriptor.reset();
	DBRegistry::getInstance()->purge();
}

/**
 * Has the DBRegistry open a RocksDB database and then move it's handle properties
 * to this DBHandle.
 * 
 * @param path - The filesystem path to the database.
 * @param options - The options for the database.
 */
void DBHandle::open(const std::string& path, const DBOptions& options) {
	auto handle = DBRegistry::getInstance()->openDB(path, options);
	this->column = std::move(handle->column);
	this->descriptor = std::move(handle->descriptor);
}

/**
 * Checks if the referenced database is opened.
 */
bool DBHandle::opened() const {
	return this->descriptor != nullptr && this->descriptor->db != nullptr;
}

} // namespace rocksdb_js
