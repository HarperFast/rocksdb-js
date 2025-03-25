#include "db_handle.h"
#include "db_registry.h"

namespace rocksdb_js {

/**
 * Open a RocksDB database with column family, caches it in the registry, and
 * return a handle to it.
 * 
 * @param path - The filesystem path to the database.
 * @param options - The options for the database.
 */
void DBHandle::open(const std::string& path, const DBOptions& options) {
	auto handle = DBRegistry::getInstance()->openDB(path, options);
	this->db = std::move(handle->db);
	this->column = std::move(handle->column);
	this->mode = std::move(handle->mode);
	// note: handle is now invalid
}

} // namespace rocksdb_js
