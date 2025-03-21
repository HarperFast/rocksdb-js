#ifndef __DB_REGISTRY_H__
#define __DB_REGISTRY_H__

#include <memory>
#include <mutex>
#include "db_handle.h"

namespace rocksdb_js {

/**
 * Descriptor for a RocksDB database and its column families. This is used by
 * the Registry.
 */
struct DBDescriptor final {
	DBDescriptor(
		std::string path,
		std::shared_ptr<rocksdb::TransactionDB> db,
		std::map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> columns
	):
		path(path),
		db(db)
	{
		for (auto& column : columns) {
			this->columns[column.first] = column.second;
		}
	}

	std::string path;
	std::weak_ptr<rocksdb::TransactionDB> db;
	std::map<std::string, std::weak_ptr<rocksdb::ColumnFamilyHandle>> columns;
};

/**
 * Tracks all RocksDB databases instances using a RocksDBDescriptor that
 * contains a weak reference to the database and column families.
 */
class DBRegistry final {
private:
	// private constructor
	DBRegistry() = default;

	std::map<std::string, std::unique_ptr<DBDescriptor>> databases;

	static std::unique_ptr<DBRegistry> instance;
	std::mutex mutex;

public:
	static DBRegistry* getInstance() {
		if (!instance) {
			instance = std::unique_ptr<DBRegistry>(new DBRegistry());
		}
		return instance.get();
	}

	static void cleanup() {
		// delete the registry instance
		instance.reset();
	}

	std::unique_ptr<DBHandle> openDB(const std::string& path, const DBOptions& options);

	/**
	 * Get the number of databases in the registry.
	 */
	size_t size() {
		std::lock_guard<std::mutex> lock(mutex);
		return this->databases.size();
	}
};

} // namespace rocksdb_js

#endif
