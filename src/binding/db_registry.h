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
	/**
	 * Private constructor.
	 */
	DBRegistry() = default;

	/**
	 * Map of database path to descriptor. There can only be one RocksDB
	 * database per path open at a time.
	 */
	std::unordered_map<std::string, std::shared_ptr<DBDescriptor>> databases;

	/**
	 * Mutex to protect the databases map.
	 */
	std::mutex databasesMutex;

	/**
	 * The singleton instance of the registry.
	 */
	static std::unique_ptr<DBRegistry> instance;

public:
	static std::unique_ptr<DBHandle> OpenDB(const std::string& path, const DBOptions& options);
	static void CloseDB(const std::shared_ptr<DBHandle> handle);
	static void PurgeAll();
	static size_t Size();
};

} // namespace rocksdb_js

#endif
