#ifndef __DB_OPTIONS_H__
#define __DB_OPTIONS_H__

#include <cstdint>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include "rocksdb/compression_type.h"
#include "rocksdb/statistics.h"

namespace rocksdb_js {

/**
 * The RocksDB database mode.
 */
enum class DBMode {
	Optimistic,
	Pessimistic,
};

// Maps to `rocksdb::DbPath`.
struct StoragePath final {
	std::string path;
	uint64_t targetSize = 0;
};

/**
 * Blob-file (large value) settings. Values at or above `minSize` are stored in
 * separate blob files rather than inline in SST files, which keeps compaction
 * from rewriting them at every level.
 *
 * These are PER-COLUMN-FAMILY options, and RocksDB does not restore them on
 * open, so — exactly like compression — every field is `std::optional` and an
 * omitted field means "leave this column family's persisted value alone". Plain
 * defaults would restamp every other family in the database with the settings of
 * whichever family happened to open it. The default named in each comment
 * applies only to a column family being CREATED.
 */
struct BlobOptions final {
	// enable_blob_files (default true).
	std::optional<bool> enabled;
	// min_blob_size (default 2048).
	std::optional<uint64_t> minSize;
	// blob_dir: the directory blob files live in. Empty = alongside the SST
	// files (`cf_paths.front()`), which is RocksDB's stock behavior. Requires a
	// RocksDB built with the downstream blob_dir patch — see
	// ROCKSDB_HAS_CF_BLOB_DIR. Changing it on an existing database orphans the
	// blob files already written, so a mismatch on reopen is rejected unless
	// `allowDirChange` says the files have been moved.
	//
	// Deliberately NOT optional: unlike the other fields, "omitted" here has to
	// mean "alongside the SST files" rather than "inherit", so that dropping the
	// option from a config is rejected instead of silently continuing to read
	// blob files from a directory the configuration no longer mentions.
	std::string dir;
	// Acknowledges that the existing blob files have been relocated to `dir`,
	// permitting an open that would otherwise be rejected as a mismatch against
	// the directory recorded in the database's OPTIONS file. Nothing is moved on
	// the caller's behalf; this only records where they went.
	//
	// DATABASE-WIDE, unlike every other field here: a relocation moves the whole
	// closed database's blob files at once (a flat backup restore does the same),
	// so `dir` is applied to every column family rather than the one this open
	// names. Scoped to the target, the rest would keep pointing at the old
	// location — after a restore, at the SOURCE database's live blob directory.
	bool allowDirChange = false;
	// enable_blob_garbage_collection (default true).
	std::optional<bool> garbageCollection;
	// blob_garbage_collection_age_cutoff (0..1, default 0.25).
	std::optional<double> garbageCollectionAgeCutoff;
	// blob_garbage_collection_force_threshold (0..1, default 1.0 = never forced).
	std::optional<double> garbageCollectionForceThreshold;
	// prepopulate_blob_cache (default false). Inert without a blob cache
	// (`RocksDatabase.config({ blobCacheSize })`), but still recorded: RocksDB
	// rewrites the OPTIONS file on every open, so a cache-less process that
	// dropped it would persist "off" over the serving process's request.
	std::optional<bool> prepopulateCache;
};

// Defaults applied to a column family that is being CREATED. An EXISTING family
// keeps whatever it persisted for any field the caller omitted.
inline constexpr bool kDefaultBlobEnabled = true;
inline constexpr uint64_t kDefaultBlobMinSize = 2048;
inline constexpr bool kDefaultBlobGarbageCollection = true;
inline constexpr double kDefaultBlobGarbageCollectionAgeCutoff = 0.25;
inline constexpr double kDefaultBlobGarbageCollectionForceThreshold = 1.0;
inline constexpr bool kDefaultBlobPrepopulateCache = false;

/**
 * Options for opening a RocksDB database. It holds the processed napi argument
 * values passed in from public `open()` method.
 */
struct DBOptions final {
	// Global memtable size trigger across all column families. When the sum of
	// all memtables reaches this size, the largest memtable is flushed. With
	// `atomic_flush = true`, this triggers flushes across every CF. 0 (the
	// default) disables the global trigger so per-CF `writeBufferSize` drives
	// flushing.
	//
	// Off by default because this is ONE budget divided among every column
	// family, so its safe value depends on a family count not knowable here.
	// Oversubscribe it and RocksDB thrashes on "flush the largest memtable",
	// costing two orders of magnitude of write throughput; any fixed value has
	// that cliff at some family count, and 0 has none. A WriteBufferManager is
	// the way to bound total memtable memory: it bounds across databases instead
	// of dividing a fixed budget among families.
	//
	// Applied by the FIRST open of a path — the descriptor is process-global, so
	// a later open with a different value is silently ignored (as with every
	// other database-wide option here).
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
	// for OptimisticTransactionDB) — EXCEPT when a stalling WriteBufferManager
	// is configured, where the derived value becomes 0 because retained history
	// the manager's budget cannot hold stalls writes permanently (see
	// resolveMaxWriteBufferSizeToMaintain in db_descriptor.cpp). An explicit
	// value is always honored as given.
	int64_t maxWriteBufferSizeToMaintain = -1;
	// Maximum number of table files RocksDB keeps open (`max_open_files`).
	// 0 = auto: derive a budget from the effective per-process open-file limit
	// (see `deriveMaxOpenFiles`); -1 = unlimited (every SST held open — can
	// exhaust the process fd limit under compaction lag); >0 = explicit cap.
	int32_t maxOpenFiles = 0;
	// Per-file size cap for informational log files (`LOG` / `LOG.old.*`,
	// `max_log_file_size`). RocksDB's own default is 0 (unbounded — a file only
	// rotates on reopen), which combined with `keep_log_file_num` retaining
	// several files let purely informational logging grow without bound in
	// production (HarperFast/rocksdb-js#729). 16MB matches this codebase's other
	// 16MB size defaults (`writeBufferSize`, `transactionLogMaxSize`); with
	// `keep_log_file_num = 5` (set alongside this in `DBDescriptor::open`), the
	// total informational-log footprint is bounded at `5 * maxLogFileSize` = 80MB.
	uint64_t maxLogFileSize = 16ULL * 1024 * 1024; // 16MB
	// Whether `maxLogFileSize` came from an explicit caller request (vs the 16MB
	// default). `max_log_file_size` is a DB-wide `DBOptions` value fixed at first
	// open, so `DBRegistry::OpenDB` rejects a second in-process open of an
	// already-open path that explicitly asks for a *different* size, while
	// letting a plain reopen (non-explicit default) inherit the live value —
	// same discipline as `compressionExplicit` (see db_registry.cpp).
	bool maxLogFileSizeExplicit = false;
	// Verbosity of informational logging (`info_log_level`). `std::nullopt`
	// leaves RocksDB's own default (`Logger::kDefaultLogLevel`: `INFO_LEVEL` in
	// release builds, `DEBUG_LEVEL` in debug builds of the linked RocksDB
	// library) untouched.
	std::optional<uint8_t> infoLogLevel;
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
	// Block/blob compression algorithm for this column family. `Database::Open`
	// fills this in: the caller's explicit choice, or the LZ4 default when the
	// build supports it (else `std::nullopt` = RocksDB's own default, Snappy when
	// linked else none). Applied to both `ColumnFamilyOptions::compression` (SST
	// blocks) and `blob_compression_type` (large values stored as blobs), which
	// otherwise defaults to no compression. Compression is a dynamically-
	// changeable option: on reopen the new algorithm governs subsequently written
	// SST/blob files; existing files keep their original compression until
	// rewritten by compaction.
	std::optional<rocksdb::CompressionType> compression;
	// Whether `compression` came from an explicit caller request (vs the LZ4
	// default). Used by `DBRegistry::OpenDB` to reject a second in-process open of
	// an already-open column family that explicitly asks for a *different*
	// algorithm, while letting a plain reopen inherit the live setting.
	bool compressionExplicit = false;
	// Compression level passed to `compression_opts.level`. `std::nullopt` keeps
	// RocksDB's per-algorithm default level. Meaning is algorithm-specific (see
	// CompressionOptions::level).
	std::optional<int> compressionLevel;
	// When true, apply `compression` to EVERY column family the underlying
	// `DB::Open` opens, not just the one named by `name`. RocksDB opens all of a
	// database's column families in that one call, and the default behavior gives
	// each of the others its persisted algorithm — correct when codecs are chosen
	// per table, but it leaves a caller that wants one codec for the whole
	// database unable to express it: the families it did not name are already open
	// at their old algorithm before it can ask. Only meaningful together with an
	// explicit `compression`, and only on the open that actually creates the
	// database handle (later opens reuse it).
	bool compressionForAllColumnFamilies = false;
	// Volumes SST files may be placed on, mapped to `DBOptions::db_paths`. When
	// empty (the default) everything lives under the database directory.
	//
	// RocksDB fills these in order — newer/smaller levels go to earlier entries,
	// and a level spills to the next entry once the running total of estimated
	// level sizes exceeds an entry's `targetSize`. Placement is a static function
	// of the target sizes and the level-size options; it does not react to actual
	// disk usage. Two caveats worth knowing:
	//
	//  - A path's index is what gets recorded per SST file in the MANIFEST, so
	//    paths may only be APPENDED across reopens. Reordering or removing an
	//    entry makes RocksDB look for existing files in the wrong directory.
	//  - More than one entry disables `level_compaction_dynamic_level_bytes`
	//    (RocksDB's SanitizeCfOptions logs a warning and turns it off), so level
	//    sizing becomes static.
	//
	// Blob files do NOT follow these paths — see `blobs.dir`.
	std::vector<StoragePath> paths;
	BlobOptions blobs;
};

} // namespace rocksdb_js

#endif
