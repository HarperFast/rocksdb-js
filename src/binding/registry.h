#ifndef __REGISTRY_H__
#define __REGISTRY_H__

#include <mutex>
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"

namespace rocksdb_js {

/**
 * Options for opening a RocksDB database.
 */
struct DBOptions final {
	std::string name;
	int parallelism;
};

/**
 * Descriptor for a RocksDB database and its column families. This is used by
 * the Registry.
 */
struct RocksDBDescriptor final {
	RocksDBDescriptor(rocksdb::TransactionDB* db) : db(db) {}

	std::shared_ptr<rocksdb::TransactionDB> db;
	std::map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> columns;
};

/**
 * Handle for a RocksDB database and the selected column family. This handle is
 * returned by the Registry and is used by the DBI.
 */
struct RocksDBHandle final {
	RocksDBHandle(std::shared_ptr<rocksdb::TransactionDB> db, std::shared_ptr<rocksdb::ColumnFamilyHandle> column) : db(db), column(column) {}

	std::shared_ptr<rocksdb::TransactionDB> db;
	std::shared_ptr<rocksdb::ColumnFamilyHandle> column;
};

/**
 * Tracks all RocksDB databases instances.
 */
class Registry final {
private:
	Registry() = default;

	std::map<std::string, std::unique_ptr<RocksDBDescriptor>> databases;

	static std::unique_ptr<Registry> instance;
	static std::mutex mutex;

public:
	~Registry() = default;

	static Registry* getInstance() {
		if (!instance) {
			instance = std::unique_ptr<Registry>(new Registry());
		}
		return instance.get();
	}

	static void cleanup() {
		instance.reset();
	}

	static std::shared_ptr<RocksDBHandle> openRocksDB(const std::string& path, const DBOptions& options);
};

} // namespace rocksdb_js

#endif