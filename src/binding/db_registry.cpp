#include "db_registry.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

// Initialize the static instance
std::unique_ptr<DBRegistry> DBRegistry::instance;

void RocksDBHandle::open(const std::string& path, const DBOptions& options) {
	auto handle = DBRegistry::getInstance()->openRocksDB(path, options);
	this->db = std::move(handle->db);
	this->column = std::move(handle->column);
	// note: handle is now invalid
}

/**
 * Open a RocksDB database with column family, caches it in the registry, and
 * return a handle to it.
 * 
 * @param path - The filesystem path to the database.
 * @param options - The options for the database.
 * @return A handle to the RocksDB database including the transaction db and
 * column family handle.
 */
std::unique_ptr<RocksDBHandle> DBRegistry::openRocksDB(const std::string& path, const DBOptions& options) {
	bool doCreate = true;
	std::shared_ptr<rocksdb::TransactionDB> db;
	std::map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> columns;
	std::lock_guard<std::mutex> lock(mutex);

	// check if database already exists
	auto dbIterator = this->databases.find(path);
	if (dbIterator != this->databases.end()) {
		std::shared_ptr<rocksdb::TransactionDB> existingDb = dbIterator->second->db.lock();
		if (existingDb) {
			db = existingDb;
			for (auto& column : dbIterator->second->columns) {
				std::shared_ptr<rocksdb::ColumnFamilyHandle> existingColumn = column.second.lock();
				if (existingColumn) {
					columns[column.first] = existingColumn;
				}
			}
			doCreate = false;
		}
	}
	
	if (doCreate) {
		// database doesn't exist, create it
		rocksdb::Options dbOptions;
		dbOptions.comparator = rocksdb::BytewiseComparator();
		dbOptions.create_if_missing = true;
		dbOptions.create_missing_column_families = true;
		dbOptions.enable_blob_files = true;
		dbOptions.enable_blob_garbage_collection = true;
		dbOptions.min_blob_size = 1024;
		dbOptions.persist_user_defined_timestamps = true;
		dbOptions.IncreaseParallelism(options.parallelism);

		rocksdb::TransactionDBOptions txndbOptions;

		std::vector<rocksdb::ColumnFamilyDescriptor> cfDescriptors = {
			rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions())
		};
		std::vector<rocksdb::ColumnFamilyHandle*> cfHandles;

		rocksdb::TransactionDB* rdb;
		rocksdb::Status status = rocksdb::TransactionDB::Open(dbOptions, txndbOptions, path, cfDescriptors, &cfHandles, &rdb);
		if (!status.ok()) {
			throw std::runtime_error(status.ToString().c_str());
		}

		db = std::shared_ptr<rocksdb::TransactionDB>(rdb, [this, path](rocksdb::TransactionDB* ptr) {
			// this is called when the last reference to the db is released
			std::lock_guard<std::mutex> lock(mutex);
			this->databases.erase(path);
			delete ptr;
		});

		for (size_t n = 0; n < cfHandles.size(); ++n) {
			columns[cfDescriptors[n].name] = std::shared_ptr<rocksdb::ColumnFamilyHandle>(cfHandles[n]);
		}

		this->databases[path] = std::make_unique<RocksDBDescriptor>(path, db, columns);
	}

	std::unique_ptr<RocksDBHandle> handle = std::make_unique<RocksDBHandle>(db);
	
	// handle the column family
	std::string name = options.name.empty() ? "default" : options.name;
	auto colIterator = columns.find(name);
	if (colIterator != columns.end()) {
		// column family already exists
		handle->column = colIterator->second;
	} else if (name != "default") {
		// column family doesn't exist, create it
		rocksdb::ColumnFamilyHandle* cfHandle;
		rocksdb::Status status = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), name, &cfHandle);
		if (!status.ok()) {
			throw std::runtime_error(status.ToString().c_str());
		}
		handle->column = std::shared_ptr<rocksdb::ColumnFamilyHandle>(cfHandle);
	} else {
		// use the default column family
		handle->column = columns[rocksdb::kDefaultColumnFamilyName];
	}

	return handle;
}

} // namespace rocksdb_js
