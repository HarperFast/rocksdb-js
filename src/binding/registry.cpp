#include "registry.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

// Initialize the static instance
std::unique_ptr<Registry> Registry::instance;
std::mutex Registry::mutex;

// Update the databases map type in the Registry class
std::map<std::string, std::shared_ptr<DBI>> databases;

DBI* Registry::open(const std::string& path, const std::string& name_) {
	std::string name = name_.empty() ? "default" : name_;
	Registry* registry = Registry::getInstance();

	std::lock_guard<std::mutex> lock(mutex);

	std::shared_ptr<RocksDBHandle> db;
	auto key = std::make_pair(path, name);

	// check if database already exists
	auto it = registry->databases.find(path);
	if (it != registry->databases.end()) {
		db = it->second;
	} else {
		// database doesn't exist, create it
		db = std::make_shared<RocksDBHandle>(path);
		registry->databases[path] = db;
	}

	std::shared_ptr<rocksdb::ColumnFamilyHandle> column = db->openColumnFamily(name);

	return new DBI(db, column);
}

} // namespace rocksdb_js
