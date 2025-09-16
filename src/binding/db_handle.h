#ifndef __DB_HANDLE_H__
#define __DB_HANDLE_H__

#include <memory>
#include <vector>
#include <utility>
#include <string>
#include <node_api.h>
#include "rocksdb/db.h"
#include "db_options.h"
#include "util.h"

namespace rocksdb_js {

// forward declarations
struct DBDescriptor;

/**
 * Handle for a RocksDB database and the selected column family. This handle is
 * returned by the Registry and is used by the `Database` class.
 *
 * This handle is for convenience since passing around a shared pointer is a
 * pain.
 */
struct DBHandle final : Closable, AsyncWorkHandle, public std::enable_shared_from_this<DBHandle> {
	/**
	 * The RocksDB database descriptor
	 */
	std::shared_ptr<DBDescriptor> descriptor;

	/**
	 * The RocksDB column family handle.
	 */
	std::shared_ptr<rocksdb::ColumnFamilyHandle> column;

	/**
	 * Whether to disable WAL.
	 */
	bool disableWAL;

	DBHandle();
	DBHandle(std::shared_ptr<DBDescriptor> descriptor, const DBOptions& options);
	~DBHandle();

	napi_ref addListener(napi_env env, std::string key, napi_value callback);

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
};

} // namespace rocksdb_js

#endif
