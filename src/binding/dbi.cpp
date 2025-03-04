#include "dbi.h"

namespace rocksdb_js {

DBI::~DBI() {
	fprintf(stderr, "destroying dbi\n");
	close();
}

/**
 * Close the database and column family.
 */
void DBI::close() {
	fprintf(stderr, "closing dbi\n");
	this->column.reset();  // drop the column family first
	this->db.reset();      // then close the database
	fprintf(stderr, "closed dbi\n");
}

/**
 * Open the database and column family using the Registry.
 */
void DBI::open(const std::string& path, const DBOptions& options) {
	auto handle = Registry::openRocksDB(path, options);
	this->db = handle->db;
	this->column = handle->column;
}

} // namespace rocksdb_js
