#ifndef __DB_SETTINGS_H__
#define __DB_SETTINGS_H__

#include <memory>
#include <mutex>
#include <node_api.h>
#include "rocksdb/cache.h"
#include "rocksdb/write_buffer_manager.h"

namespace rocksdb_js {

/**
 * Stores the global settings for RocksDB databases as well as various global
 * state.
 */
class DBSettings final {
private:
	DBSettings(); // private constructor

	size_t blockCacheSize;
	std::shared_ptr<rocksdb::Cache> blockCache;

	// Total memory limit (bytes) shared across all databases for active and
	// immutable memtables. 0 disables the manager (each database uses its own
	// unbounded memtable budget).
	//
	// Atomic so JS-thread writes in Config() are safely visible to libuv
	// worker threads that read it from getWriteBufferManager() during async
	// database open. Not protected by writeBufferManagerMutex because the
	// "should I attach a WBM at all?" check needs to be lock-free fast path.
	std::atomic<size_t> writeBufferManagerSize;

	// When true, memtable memory is "charged" against the shared block cache.
	// Active memtables and the block cache then draw from a single pool — the
	// cache shrinks during write bursts and reclaims room as memtables flush.
	// Has no effect when the block cache is disabled (size 0).
	//
	// Atomic for the same reason as writeBufferManagerSize — concurrent
	// reads from worker threads vs writes from the JS thread.
	std::atomic<bool> writeBufferManagerCostToCache;

	// When true, writes stall once the manager's buffer_size is exceeded
	// instead of allowing memtables to grow past the limit. Off by default to
	// favor write throughput over hard memory bounding.
	std::atomic<bool> writeBufferManagerAllowStall;

	std::shared_ptr<rocksdb::WriteBufferManager> writeBufferManager;
	std::mutex writeBufferManagerMutex;

	bool compactOnClose;

public:
	/**
	 * Returns the process-wide DBSettings singleton.
	 *
	 * Uses C++11 magic-static initialization so concurrent first-callers
	 * from libuv worker threads don't race during construction.
	 */
	static DBSettings& getInstance() {
		static DBSettings instance;
		return instance;
	}

	size_t getBlockCacheSize() const {
		return blockCacheSize;
	}

	std::shared_ptr<rocksdb::Cache> getBlockCache();

	size_t getWriteBufferManagerSize() const {
		return writeBufferManagerSize.load(std::memory_order_relaxed);
	}

	bool getWriteBufferManagerCostToCache() const {
		return writeBufferManagerCostToCache.load(std::memory_order_relaxed);
	}

	std::shared_ptr<rocksdb::WriteBufferManager> getWriteBufferManager();

	inline bool getCompactOnClose() const {
		return compactOnClose;
	}

	static napi_value Config(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);
};

} // namespace rocksdb_js

#endif