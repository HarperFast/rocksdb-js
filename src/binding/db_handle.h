#ifndef __DB_HANDLE_H__
#define __DB_HANDLE_H__

#include <memory>
#include <node_api.h>
#include "rocksdb/db.h"
#include "db_descriptor.h"
#include "util.h"

namespace rocksdb_js {

// forward declare DBDescriptor because of circular dependency
struct DBDescriptor;

/**
 * Handle for a RocksDB database and the selected column family. This handle is
 * returned by the Registry and is used by the `Database` class.
 *
 * This handle is for convenience since passing around a shared pointer is a
 * pain.
 */
struct DBHandle final : Closable, AsyncWorkHandle {
	DBHandle();
	DBHandle(std::shared_ptr<DBDescriptor> descriptor);
	~DBHandle();

	rocksdb::Status clear(uint32_t batchSize, uint64_t& deleted);
	void close();
	napi_value get(
		napi_env env,
		rocksdb::Slice& key,
		napi_value resolve,
		napi_value reject,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr
	);
	void open(const std::string& path, const DBOptions& options);
	bool opened() const;

	std::shared_ptr<DBDescriptor> descriptor;
	std::shared_ptr<rocksdb::ColumnFamilyHandle> column;
};

} // namespace rocksdb_js

#endif
