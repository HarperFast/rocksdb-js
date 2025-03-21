#include "db_registry.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

// Initialize the static instance
std::unique_ptr<DBRegistry> DBRegistry::instance;

/**
 * Helper function to create a column family.
 * 
 * @param db - The RocksDB database instance.
 * @param name - The name of the column family.
 */
std::shared_ptr<rocksdb::ColumnFamilyHandle> createColumn(const std::shared_ptr<rocksdb::TransactionDB> db, const std::string& name) {
	rocksdb::ColumnFamilyHandle* cfHandle;
	rocksdb::Status status = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), name, &cfHandle);
	if (!status.ok()) {
		throw std::runtime_error(status.ToString().c_str());
	}
	return std::shared_ptr<rocksdb::ColumnFamilyHandle>(cfHandle);
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
std::unique_ptr<DBHandle> DBRegistry::openDB(const std::string& path, const DBOptions& options) {
	bool dbExists = false;
	std::shared_ptr<rocksdb::TransactionDB> db;
	std::map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> columns;
	std::string name = options.name.empty() ? "default" : options.name;
	std::lock_guard<std::mutex> lock(mutex);

	// check if database already exists
	auto dbIterator = this->databases.find(path);
	if (dbIterator != this->databases.end()) {
		std::shared_ptr<rocksdb::TransactionDB> existingDb = dbIterator->second->db.lock();
		if (existingDb) {
			db = existingDb;
			dbExists = true;

			// manually copy the columns because we don't know which ones are valid
			bool columnExists = false;
			for (auto& column : dbIterator->second->columns) {
				std::shared_ptr<rocksdb::ColumnFamilyHandle> existingColumn = column.second.lock();
				if (existingColumn) {
					columns[column.first] = existingColumn;
					if (column.first == name) {
						columnExists = true;
					}
				}
			}
			if (!columnExists) {
				columns[name] = createColumn(db, name);
			}
		}
	}
	
	if (!dbExists) {
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
		txndbOptions.default_lock_timeout = 1000;
		txndbOptions.transaction_lock_timeout = 1000;

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

		bool columnExists = false;
		for (size_t n = 0; n < cfHandles.size(); ++n) {
			columns[cfDescriptors[n].name] = std::shared_ptr<rocksdb::ColumnFamilyHandle>(cfHandles[n]);
			if (cfDescriptors[n].name == name) {
				columnExists = true;
			}
		}
		if (!columnExists) {
			columns[name] = createColumn(db, name);
		}

		this->databases[path] = std::make_unique<DBDescriptor>(path, db, columns);
	}

	std::unique_ptr<DBHandle> handle = std::make_unique<DBHandle>(db);
	
	// handle the column family
	auto colIterator = columns.find(name);
	if (colIterator != columns.end()) {
		// column family already exists
		handle->column = colIterator->second;
	} else {
		// use the default column family
		handle->column = columns[rocksdb::kDefaultColumnFamilyName];
	}

	return handle;
}

} // namespace rocksdb_js
