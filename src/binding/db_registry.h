#ifndef __DB_REGISTRY_H__
#define __DB_REGISTRY_H__

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
	RocksDBDescriptor(
		std::string path,
		std::shared_ptr<rocksdb::TransactionDB> db,
		std::map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> columns
	)
		: path(path), db(db) {
		for (auto& column : columns) {
			this->columns[column.first] = column.second;
		}
	}

	std::string path;
	std::weak_ptr<rocksdb::TransactionDB> db;
	std::map<std::string, std::weak_ptr<rocksdb::ColumnFamilyHandle>> columns;
};

/**
 * Handle for a RocksDB database and the selected column family. This handle is
 * returned by the Registry and is used by the DBI.
 */
struct RocksDBHandle final {
	RocksDBHandle() = default;
	RocksDBHandle(std::shared_ptr<rocksdb::TransactionDB> db) : db(db), column(nullptr) {}

	~RocksDBHandle() {
		this->close();
	}

	void close() {
		this->column.reset();
		this->db.reset();
	}
	void open(const std::string& path, const DBOptions& options);
	bool opened() const { return this->db != nullptr; }

	std::shared_ptr<rocksdb::TransactionDB> db;
	std::shared_ptr<rocksdb::ColumnFamilyHandle> column;
};

/**
 * Tracks all RocksDB databases instances using a RocksDBDescriptor that
 * contains a weak reference to the database and column families.
 */
class DBRegistry final {
private:
	// private constructor
	DBRegistry() = default;

	std::map<std::string, std::unique_ptr<RocksDBDescriptor>> databases;

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

	std::unique_ptr<RocksDBHandle> openRocksDB(const std::string& path, const DBOptions& options);

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
