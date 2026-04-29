#ifndef __DB_SETTINGS_H__
#define __DB_SETTINGS_H__

#include <memory>
#include <mutex>
#include <node_api.h>
#include "rocksdb/cache.h"
#include "verification_table.h"

namespace rocksdb_js {

/**
 * Stores the global settings for RocksDB databases as well as various global
 * state.
 */
class DBSettings final {
private:
	DBSettings(); // private constructor

	static std::unique_ptr<DBSettings> instance;

	size_t blockCacheSize;
	std::shared_ptr<rocksdb::Cache> blockCache;

	// Number of slots requested for the verification table. Default 128K
	// (1 MB at 8 bytes per slot). 0 disables the table. Configurable via
	// RocksDatabase.config({ verificationTableEntries }) only before the
	// table is first materialized; after that, attempts to change it throw.
	size_t verificationTableEntries;

	// Random hash seed mixed into verification-table slot indices.
	uint64_t verificationTableSeed;

	std::unique_ptr<VerificationTable> verificationTable;
	std::mutex verificationTableMutex;

public:
	static DBSettings& getInstance() {
		if (!instance) {
			instance = std::unique_ptr<DBSettings>(new DBSettings());
		}
		return *instance;
	}

	size_t getBlockCacheSize() const {
		return blockCacheSize;
	}

	std::shared_ptr<rocksdb::Cache> getBlockCache();

	/**
	 * Returns the global verification table, materializing it on first call.
	 * After the first call, the table size is fixed for the process lifetime.
	 * Returns null when the table is disabled (entries == 0).
	 */
	VerificationTable* getVerificationTable();

	/**
	 * Returns the verification table if it has already been materialized,
	 * without creating it. Safe to call from any thread. Returns null when
	 * the table has not yet been created or is disabled.
	 *
	 * Use this in hot paths (e.g. transaction commit) where materializing
	 * the table would trigger the config-freeze check unexpectedly.
	 */
	VerificationTable* getVerificationTableRaw();

	static napi_value Config(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);
};

} // namespace rocksdb_js

#endif