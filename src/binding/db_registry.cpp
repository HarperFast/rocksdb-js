#include "db_registry.h"
#include "db_settings.h"
#include "macros.h"
#include "util.h"
#include "rocksdb/table.h"

namespace rocksdb_js {

// Initialize the static instance
std::unique_ptr<DBRegistry> DBRegistry::instance;

/**
 * Helper function to create a column family.
 *
 * @param db - The RocksDB database instance.
 * @param name - The name of the column family.
 */
std::shared_ptr<rocksdb::ColumnFamilyHandle> createColumn(const std::shared_ptr<rocksdb::DB> db, const std::string& name) {
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
	std::unordered_map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> columns;
	std::string name = options.name.empty() ? "default" : options.name;
	std::shared_ptr<DBDescriptor> descriptor;
	std::lock_guard<std::mutex> lock(mutex);

	// check if database already exists
	auto dbIterator = this->databases.find(path);
	if (dbIterator != this->databases.end()) {
		descriptor = dbIterator->second.lock();
		if (descriptor) {
			// check if the database is already open with a different mode
			if (options.mode != descriptor->mode) {
				throw std::runtime_error(
					"Database already open in '" +
					(descriptor->mode == DBMode::Optimistic ? std::string("optimistic") : std::string("pessimistic")) +
					"' mode"
				);
			}

			dbExists = true;

			// manually copy the columns because we don't know which ones are valid
			bool columnExists = false;
			for (auto& column : descriptor->columns) {
				std::shared_ptr<rocksdb::ColumnFamilyHandle> existingColumn = column.second;
				if (existingColumn) {
					columns[column.first] = existingColumn;
					if (column.first == name) {
						columnExists = true;
					}
				}
			}
			if (!columnExists) {
				columns[name] = createColumn(descriptor->db, name);
			}
		}
	}

	if (!dbExists) {
		// database doesn't exist, create it

		// set or disable the block cache
		rocksdb::BlockBasedTableOptions tableOptions;
		if (options.noBlockCache) {
			tableOptions.no_block_cache = true;
		} else {
			DBSettings& settings = DBSettings::getInstance();
			tableOptions.block_cache = settings.getBlockCache();
		}

		// set the database options
		rocksdb::Options dbOptions;
		dbOptions.comparator = rocksdb::BytewiseComparator();
		dbOptions.create_if_missing = true;
		dbOptions.create_missing_column_families = true;
		dbOptions.enable_blob_files = true;
		dbOptions.enable_blob_garbage_collection = true;
		dbOptions.min_blob_size = 1024;
		dbOptions.persist_user_defined_timestamps = true;
		dbOptions.IncreaseParallelism(options.parallelismThreads);
		dbOptions.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOptions));

		// prepare the column family stuff
		std::vector<rocksdb::ColumnFamilyDescriptor> cfDescriptors = {
			rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions())
		};
		std::vector<rocksdb::ColumnFamilyHandle*> cfHandles;

		std::shared_ptr<rocksdb::DB> db;

		if (options.mode == DBMode::Pessimistic) {
			rocksdb::TransactionDBOptions txndbOptions;
			txndbOptions.default_lock_timeout = 10000;
			txndbOptions.transaction_lock_timeout = 10000;

			rocksdb::TransactionDB* rdb;
			rocksdb::Status status = rocksdb::TransactionDB::Open(dbOptions, txndbOptions, path, cfDescriptors, &cfHandles, &rdb);
			if (!status.ok()) {
				throw std::runtime_error(status.ToString().c_str());
			}
			db = std::shared_ptr<rocksdb::DB>(rdb);
		} else {
			rocksdb::OptimisticTransactionDB* rdb;
			rocksdb::Status status = rocksdb::OptimisticTransactionDB::Open(dbOptions, path, cfDescriptors, &cfHandles, &rdb);
			if (!status.ok()) {
				throw std::runtime_error(status.ToString().c_str());
			}
			db = std::shared_ptr<rocksdb::DB>(rdb);
		}

		// figure out if desired column family exists and if not create it
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

		// create the descriptor and add it to the registry
		descriptor = std::make_shared<DBDescriptor>(path, options.mode, db, columns);
		this->databases[path] = descriptor;
	}

	std::unique_ptr<DBHandle> handle = std::make_unique<DBHandle>(descriptor);

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

/**
 * Purge expired database descriptors from the registry.
 */
void DBRegistry::purge() {
	std::lock_guard<std::mutex> lock(this->mutex);
	fprintf(stderr, "DBRegistry::purge start size=%zu\n", this->databases.size());
	for (auto it = this->databases.begin(); it != this->databases.end();) {
        if (it->second.expired()) {
            it = this->databases.erase(it);
	    } else {
            ++it;
        }
    }
	fprintf(stderr, "DBRegistry::purge done size=%zu\n", this->databases.size());
}

} // namespace rocksdb_js
