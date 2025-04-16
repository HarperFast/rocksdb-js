#ifndef __DB_REGISTRY_H__
#define __DB_REGISTRY_H__

#include <memory>
#include <mutex>
#include "db_descriptor.h"
#include "db_handle.h"
#include "transaction.h"

namespace rocksdb_js {

/**
 * Tracks all RocksDB databases instances using a RocksDBDescriptor that
 * contains a weak reference to the database and column families.
 */
class DBRegistry final {
private:
	// private constructor
	DBRegistry() = default;

	// map of database path to descriptor
	// this needs to be a weak_ptr because the DBHandles own the descriptor
	std::unordered_map<std::string, std::weak_ptr<DBDescriptor>> databases;

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

	void purge();

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
