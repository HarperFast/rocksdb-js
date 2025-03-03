#include "dbi.h"

namespace rocksdb_js {

RocksDBHandle::RocksDBHandle(const std::string& path) {
	rocksdb::Options dbOptions;
	dbOptions.comparator = rocksdb::BytewiseComparator();
	dbOptions.create_if_missing = true;
	dbOptions.create_missing_column_families = true;
	dbOptions.enable_blob_files = true;
	dbOptions.enable_blob_garbage_collection = true;
	dbOptions.min_blob_size = 1024;
	dbOptions.persist_user_defined_timestamps = true;

	rocksdb::TransactionDBOptions txndbOptions;

	std::vector<rocksdb::ColumnFamilyDescriptor> cfDescriptors = {
		rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions())
	};
	std::vector<rocksdb::ColumnFamilyHandle*> cfHandles;

	rocksdb::Status status = rocksdb::TransactionDB::Open(dbOptions, txndbOptions, path, cfDescriptors, &cfHandles, &this->db);
	if (!status.ok()) {
		throw std::runtime_error(status.ToString().c_str());
	}

	for (size_t n = 0; n < cfHandles.size(); ++n) {
		this->columns[cfDescriptors[n].name] = cfHandles[n];
	}
}

std::shared_ptr<rocksdb::ColumnFamilyHandle> RocksDBHandle::openColumnFamily(const std::string& name_) {
	std::string name = name_.empty() ? "default" : name_;

	// check if the column family already exists
	auto it = this->columns.find(name);
	if (it != this->columns.end()) {
		return std::shared_ptr<rocksdb::ColumnFamilyHandle>(it->second, [this](rocksdb::ColumnFamilyHandle* handle) {
			this->db->DropColumnFamily(handle);
		});
	}

	if (name == "default") {
		throw std::runtime_error("Default column family cannot be created");
	}

	// column family doesn't exist, create it
	rocksdb::ColumnFamilyHandle* cfHandle;
	rocksdb::Status status = this->db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), name, &cfHandle);
	if (!status.ok()) {
		throw std::runtime_error(status.ToString());
	}
	this->columns[name] = cfHandle;
	return std::shared_ptr<rocksdb::ColumnFamilyHandle>(cfHandle, [this](rocksdb::ColumnFamilyHandle* handle) {
		this->db->DropColumnFamily(handle);
	});
}

} // namespace rocksdb_js
