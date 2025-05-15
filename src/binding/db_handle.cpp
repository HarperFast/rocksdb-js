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
	// TODO: Do we NEED this?
	// this->close();
}

/**
 * Closes the DBHandle.
 */
void DBHandle::close() {
	fprintf(stderr, "DBHandle::close this=%p\n", this);
	if (this->column) {
		this->column.reset();
	}

	if (this->descriptor) {
		this->descriptor.reset();
	}

	// purge all weak references in the registry
	DBRegistry::getInstance()->purge();
	fprintf(stderr, "DBHandle::close done this=%p\n", this);
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
	return this->descriptor && this->descriptor->db;
}

} // namespace rocksdb_js
