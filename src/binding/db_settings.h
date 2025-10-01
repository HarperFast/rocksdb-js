#ifndef __DB_SETTINGS_H__
#define __DB_SETTINGS_H__

#include <memory>
#include <node_api.h>
#include "rocksdb/cache.h"

namespace rocksdb_js {

/**
 * Stores the global settings for RocksDB databases as well as various global
 * state.
 */
class DBSettings final {
private:
	DBSettings(); // private constructor

	static std::unique_ptr<DBSettings> instance;

	uint32_t blockCacheSize;
	std::shared_ptr<rocksdb::Cache> blockCache;

public:
	static DBSettings& getInstance() {
		if (!instance) {
			instance = std::unique_ptr<DBSettings>(new DBSettings());
		}
		return *instance;
	}

	uint32_t getBlockCacheSize() const {
		return blockCacheSize;
	}

	std::shared_ptr<rocksdb::Cache> getBlockCache();

	static napi_value Config(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);
};

} // namespace rocksdb_js

#endif