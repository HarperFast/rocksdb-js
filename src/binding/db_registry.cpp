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
std::unique_ptr<DBHandle> DBRegistry::OpenDB(const std::string& path, const DBOptions& options) {
	if (!instance) {
		instance = std::unique_ptr<DBRegistry>(new DBRegistry());
		DEBUG_LOG("%p DBRegistry::OpenDB Initializing registry\n", instance.get())
	} else {
		DEBUG_LOG("%p DBRegistry::OpenDB Registry already initialized\n", instance.get())
	}

	bool dbExists = false;
	std::unordered_map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> columns;
	std::string name = options.name.empty() ? "default" : options.name;
	std::shared_ptr<DBDescriptor> descriptor;
	std::lock_guard<std::mutex> lock(instance->mutex);

	// check if database already exists
	auto dbIterator = instance->databases.find(path);
	if (dbIterator != instance->databases.end()) {
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

			DEBUG_LOG("%p DBRegistry::OpenDB Database %s already open\n", instance.get(), path.c_str())
			DEBUG_LOG("%p DBRegistry::OpenDB Checking for column family %s\n", instance.get(), name.c_str())

			dbExists = true;

			// manually copy the columns because we don't know which ones are valid
			bool columnExists = false;
			for (auto& column : descriptor->columns) {
				columns[column.first] = column.second;
				if (column.first == name) {
					DEBUG_LOG("%p DBRegistry::OpenDB Column family %s already exists\n", instance.get(), name.c_str())
					columnExists = true;
				}
			}
			if (!columnExists) {
				DEBUG_LOG("DBRegistry::OpenDB Creating column family \"%s\"\n", name.c_str())
				columns[name] = createColumn(descriptor->db, name);
				descriptor->columns[name] = columns[name];
			}
		}
	}

	if (!dbExists) {
		DEBUG_LOG("%p DBRegistry::OpenDB Opening %s\n", instance.get(), path.c_str())

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

		// prepare the column family stuff - first check if database exists
		std::vector<rocksdb::ColumnFamilyDescriptor> cfDescriptors;
		std::vector<std::string> columnFamilyNames;

		// try to list existing column families
		rocksdb::Status listStatus = rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(), path, &columnFamilyNames);
		if (listStatus.ok() && !columnFamilyNames.empty()) {
			// Database exists, use existing column families
			for (const auto& cfName : columnFamilyNames) {
				cfDescriptors.emplace_back(cfName, rocksdb::ColumnFamilyOptions());
			}
		} else {
			// database doesn't exist or no column families found, use default
			cfDescriptors = {
				rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions())
			};
		}
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
			db = std::shared_ptr<rocksdb::DB>(rdb, DBDeleter{});
		} else {
			rocksdb::OptimisticTransactionDB* rdb;
			rocksdb::Status status = rocksdb::OptimisticTransactionDB::Open(dbOptions, path, cfDescriptors, &cfHandles, &rdb);
			if (!status.ok()) {
				throw std::runtime_error(status.ToString().c_str());
			}
			db = std::shared_ptr<rocksdb::DB>(rdb, DBDeleter{});
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
		instance->databases[path] = descriptor;
	}

	std::unique_ptr<DBHandle> handle = std::make_unique<DBHandle>(descriptor);
	DEBUG_LOG("%p DBRegistry::OpenDB Created DBHandle %p\n", instance.get(), handle.get())

	// handle the column family
	auto colIterator = columns.find(name);
	if (colIterator != columns.end()) {
		// column family already exists
		DEBUG_LOG("%p DBRegistry::OpenDB Column family %s found\n", instance.get(), name.c_str())
		handle->column = colIterator->second;
	} else {
		// use the default column family
		DEBUG_LOG("%p DBRegistry::OpenDB Column family %s not found, using default\n", instance.get(), name.c_str())
		handle->column = columns[rocksdb::kDefaultColumnFamilyName];
	}

	return handle;
}

/**
 * Purge expired database descriptors from the registry.
 */
void DBRegistry::Purge(bool all) {
	if (instance) {
		std::lock_guard<std::mutex> lock(instance->mutex);
#ifdef DEBUG
		size_t initialSize = instance->databases.size();
#endif
		for (auto it = instance->databases.begin(); it != instance->databases.end();) {
			if (all || it->second.expired()) {
				it = instance->databases.erase(it);
			} else {
				++it;
			}
		}
#ifdef DEBUG
		size_t currentSize = instance->databases.size();
		DEBUG_LOG(
			"%p DBRegistry::purge Purged %zu unused descriptors (size=%zu)\n",
			instance.get(),
			initialSize - currentSize,
			currentSize
		);
#endif
	}
}

/**
 * Get the number of databases in the registry.
 */
size_t DBRegistry::Size() {
	if (instance) {
		std::lock_guard<std::mutex> lock(instance->mutex);
		return instance->databases.size();
	}
	return 0;
}

} // namespace rocksdb_js
