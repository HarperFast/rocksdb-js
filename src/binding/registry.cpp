#include "registry.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

// Initialize the static instance
std::unique_ptr<Registry> Registry::instance;
std::mutex Registry::mutex;

std::shared_ptr<RocksDBHandle> Registry::openRocksDB(const std::string& path, const DBOptions& options) {
	Registry* registry = Registry::getInstance();
	std::unique_ptr<RocksDBDescriptor> desc = nullptr;

	{
		// check if database already exists
		std::lock_guard<std::mutex> lock(mutex);
		auto it = registry->databases.find(path);
		if (it != registry->databases.end()) {
			desc = std::move(it->second);
		}
	}

	if (desc == nullptr) {
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

		rocksdb::TransactionDB* db;
		rocksdb::Status status = rocksdb::TransactionDB::Open(dbOptions, txndbOptions, path, cfDescriptors, &cfHandles, &db);
		if (!status.ok()) {
			throw std::runtime_error(status.ToString().c_str());
		}

		desc = std::make_unique<RocksDBDescriptor>(db);

		for (size_t n = 0; n < cfHandles.size(); ++n) {
			auto db_ptr = desc->db.get();  // Get raw pointer
			desc->columns[cfDescriptors[n].name] = std::shared_ptr<rocksdb::ColumnFamilyHandle>(cfHandles[n], 
				[db_ptr](rocksdb::ColumnFamilyHandle* handle) {
					db_ptr->DropColumnFamily(handle);
				});
		}

		std::lock_guard<std::mutex> lock(mutex);
		registry->databases[path] = std::move(desc);
	}

	// get the column family
	std::string name = options.name.empty() ? "default" : options.name;
	std::shared_ptr<rocksdb::ColumnFamilyHandle> column = nullptr;
	auto it = desc->columns.find(name);
	if (it != desc->columns.end()) {
		// column family already exists
		column = it->second;
	} else if (name != "default") {
		// column family doesn't exist, create it
		rocksdb::ColumnFamilyHandle* cfHandle;
		rocksdb::Status status = desc->db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), name, &cfHandle);
		if (!status.ok()) {
			throw std::runtime_error(status.ToString().c_str());
		}
		auto db_ptr = desc->db.get();
		column = std::shared_ptr<rocksdb::ColumnFamilyHandle>(cfHandle, 
			[db_ptr](rocksdb::ColumnFamilyHandle* handle) {
				db_ptr->DropColumnFamily(handle);
			});
	} else {
		column = desc->columns[rocksdb::kDefaultColumnFamilyName];
	}

	return std::make_shared<RocksDBHandle>(desc->db, column);
}

} // namespace rocksdb_js
