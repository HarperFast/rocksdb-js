#ifndef __DBI_H__
#define __DBI_H__

#include "registry.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"

namespace rocksdb_js {

struct DBI {
	DBI() : db(nullptr), column(nullptr) {}

	void close();
	void open(const std::string& path, const DBOptions& options);
	bool opened() const { return this->db != nullptr; }

	std::shared_ptr<rocksdb::TransactionDB> db;
	std::shared_ptr<rocksdb::ColumnFamilyHandle> column;
};

} // namespace rocksdb_js

#endif