#ifndef __DBI_H__
#define __DBI_H__

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include <node_api.h>

namespace rocksdb_js {

namespace dbi_wrap {
	napi_value close(napi_env, napi_callback_info);
	napi_value get(napi_env, napi_callback_info);
	napi_value isOpen(napi_env, napi_callback_info);
	napi_value open(napi_env, napi_callback_info);
	napi_value put(napi_env, napi_callback_info);
}

class RocksDBHandle final {
	friend napi_value dbi_wrap::close(napi_env, napi_callback_info);
	friend napi_value dbi_wrap::get(napi_env, napi_callback_info);
	friend napi_value dbi_wrap::isOpen(napi_env, napi_callback_info);
	friend napi_value dbi_wrap::open(napi_env, napi_callback_info);
	friend napi_value dbi_wrap::put(napi_env, napi_callback_info);

private:
	std::string path;
	std::map<std::string, rocksdb::ColumnFamilyHandle*> columns;

public:
	rocksdb::TransactionDB* db;

	RocksDBHandle(const std::string& path);
	std::shared_ptr<rocksdb::ColumnFamilyHandle> openColumnFamily(const std::string& name);
};

class DBI {
	friend napi_value dbi_wrap::close(napi_env, napi_callback_info);
	friend napi_value dbi_wrap::get(napi_env, napi_callback_info);
	friend napi_value dbi_wrap::isOpen(napi_env, napi_callback_info);
	friend napi_value dbi_wrap::open(napi_env, napi_callback_info);
	friend napi_value dbi_wrap::put(napi_env, napi_callback_info);

public:
	DBI(std::shared_ptr<RocksDBHandle> db, std::shared_ptr<rocksdb::ColumnFamilyHandle> columnFamily)
		: db(std::move(db)), columnFamily(std::move(columnFamily)) {}

	DBI(const DBI& other)
		: db(other.db), columnFamily(other.columnFamily) {}

	~DBI();

	inline rocksdb::TransactionDB* getDB() const { return this->db->db; }

private:
	std::shared_ptr<RocksDBHandle> db;
	std::shared_ptr<rocksdb::ColumnFamilyHandle> columnFamily;
};

} // namespace rocksdb_js

#endif