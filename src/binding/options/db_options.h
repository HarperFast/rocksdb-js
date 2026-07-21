#ifndef __DB_OPTIONS_H__
#define __DB_OPTIONS_H__

#include <cstdint>
#include <string>
#include <thread>

namespace rocksdb_js {

/**
 * The RocksDB database mode.
 */
enum class DBMode {
	Optimistic,
	Pessimistic,
};

/**
 * Options for opening a RocksDB database. It holds the processed napi argument
 * values passed in from public `open()` method.
 */
struct DBOptions final {
	// Global memtable size trigger across all column families. When the sum of
	// all memtables reaches this size, the largest memtable is flushed. With
	// `atomic_flush = true`, this triggers flushes across every CF. 0 disables
	// the global trigger so per-CF `writeBufferSize` drives flushing.
	uint64_t dbWriteBufferSize = 0;
	bool disableWAL = false;
	bool enableStats = false;
	// Maximum number of memtables that can be queued per column family before
	// writes stall. Higher values absorb write bursts while flushes catch up,
	// at the cost of memory (roughly `maxWriteBufferNumber * writeBufferSize`
	// per CF).
	int32_t maxWriteBufferNumber = 16;
	// Bytes of recent memtable history to retain in memory for transaction
	// conflict checking. -1 derives the value from
	// `maxWriteBufferNumber * writeBufferSize` (the RocksDB-recommended default
	// for OptimisticTransactionDB).
	int64_t maxWriteBufferSizeToMaintain = -1;
	// Maximum number of table files RocksDB keeps open (`max_open_files`).
	// 0 = auto: derive a budget from the effective per-process open-file limit
	// (see `deriveMaxOpenFiles`); -1 = unlimited (every SST held open — can
	// exhaust the process fd limit under compaction lag); >0 = explicit cap.
	int32_t maxOpenFiles = 0;
	DBMode mode = DBMode::Optimistic;
	std::string name;
	bool noBlockCache = false;
	bool readOnly = false;
	uint32_t parallelismThreads = std::max<uint32_t>(1, std::thread::hardware_concurrency() / 2);
	uint8_t statsLevel = rocksdb::StatsLevel::kExceptDetailedTimers;
	float transactionLogMaxAgeThreshold = 0.75f;
	uint32_t transactionLogMaxSize = 16 * 1024 * 1024; // 16MB
	uint32_t transactionLogRetentionMs = 3 * 24 * 60 * 60 * 1000; // 3 days
	std::string transactionLogsPath;
	// Per-CF memtable size at which the memtable is sealed and flushed. Smaller
	// values produce more frequent, faster flushes; larger values batch more
	// writes per SST file.
	uint64_t writeBufferSize = 16ULL * 1024 * 1024; // 16MB
	// Opt-in per-CF flag enabling Verification Table slot locking/tracking for
	// this column family's writes (see core/verification_table.h).
	bool verificationTable = false;
};

} // namespace rocksdb_js

#endif
