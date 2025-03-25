#ifndef __DB_HANDLE_H__
#define __DB_HANDLE_H__

#include <memory>
#include <mutex>
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "db_options.h"
#include <node_api.h>

namespace rocksdb_js {

/**
 * Handle for a RocksDB database and the selected column family. This handle is
 * returned by the Registry and is used by the `Database` class.
 *
 * This handle is for convenience since passing around a shared pointer is a
 * pain.
 */
struct DBHandle final {
	DBHandle(): mode(DBMode::Default) {}
	DBHandle(std::shared_ptr<rocksdb::DB> db, DBMode mode) : db(db), mode(mode) {}

	~DBHandle() {
		this->close();
	}

	void close() {
		this->column.reset();
		this->db.reset();
	}
	void open(const std::string& path, const DBOptions& options);
	bool opened() const { return this->db != nullptr; }

	std::shared_ptr<rocksdb::DB> db;
	DBMode mode;
	std::shared_ptr<rocksdb::ColumnFamilyHandle> column;
};

} // namespace rocksdb_js

#endif
