#include "dbi.h"

namespace rocksdb_js {

void DBI::close() {
	this->db = nullptr;
	this->column = nullptr;
}

void DBI::open(const std::string& path, const DBOptions& options) {
	auto handle = Registry::openRocksDB(path, options);
	this->db = handle->db;
	this->column = handle->column;
}

} // namespace rocksdb_js
