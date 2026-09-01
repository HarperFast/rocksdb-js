#include "core/background_error.h"
#include "core/blob_relocation.h"
#include "core/options_file.h"
#include "core/platform.h"
#include "database/db_descriptor.h"
#include "database/db_registry.h"
#include "database/db_settings.h"
#include "napi/helpers.h"
#include "transaction/transaction_handle.h"
#include "transaction_log/transaction_log_store_registry.h"
#include "rocksdb/convenience.h"
#include "rocksdb/listener.h"
#include "rocksdb/utilities/options_util.h"
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <system_error>
#include <unordered_map>

namespace rocksdb_js {

// forward declarations
static void callJsCallback(napi_env env, napi_value jsCallback, void* context, void* data);

// Process-global monotonic source for DBDescriptor::vtEpoch. Starts at 1 so a
// valid epoch is never 0. 64-bit: never wraps in practice, so every descriptor
// open across the process lifetime gets a distinct VerificationTable identity.
static std::atomic<uint64_t> vtEpochCounter{1};

static uint64_t nextVtEpoch() {
	return vtEpochCounter.fetch_add(1, std::memory_order_relaxed);
}

// A column family's persisted per-CF options, recovered from the on-disk OPTIONS
// file so a cold open of one CF does not clobber the others. Compression and the
// blob settings are both per-CF, and RocksDB restores neither on open.
struct PersistedCFOptions {
	rocksdb::CompressionType compression;
	rocksdb::CompressionType blobCompression;
	rocksdb::CompressionOptions compressionOpts;
	bool enableBlobFiles;
	uint64_t minBlobSize;
	bool enableBlobGarbageCollection;
	double blobGarbageCollectionAgeCutoff;
	double blobGarbageCollectionForceThreshold;
	rocksdb::PrepopulateBlobCache prepopulateBlobCache;
	// Where this column family's blob files were last written. Compared against
	// the requested `blobs.dir` so a reopen cannot strand them. Always empty when
	// built against a RocksDB without the blob_dir patch.
	std::string blobDir;
};

// Puts the blob settings of a column family being created back to the documented
// creation defaults. The base options carry the opening handle's request, which
// belongs to the family it named — the `default` family a fresh database creates
// on the way to a named one must not persist it (invariant 19).
static void applyBlobCreationDefaults(rocksdb::ColumnFamilyOptions& cfOptions) {
	cfOptions.enable_blob_files = kDefaultBlobEnabled;
	cfOptions.min_blob_size = kDefaultBlobMinSize;
	cfOptions.enable_blob_garbage_collection = kDefaultBlobGarbageCollection;
	cfOptions.blob_garbage_collection_age_cutoff = kDefaultBlobGarbageCollectionAgeCutoff;
	cfOptions.blob_garbage_collection_force_threshold =
		kDefaultBlobGarbageCollectionForceThreshold;
	cfOptions.prepopulate_blob_cache = rocksdb::PrepopulateBlobCache::kDisable;
#ifdef ROCKSDB_HAS_CF_BLOB_DIR
	cfOptions.blob_dir.clear();
#endif
}

// Restores a column family's persisted blob settings over the base options. Used
// for every column family on a cold open, before the caller's request is applied
// to the target one: blob settings are per-CF and RocksDB does not restore them,
// so without this, opening one family restamps every other family in the
// database with the opener's settings — and in Harper, where every table is a
// named column family, whichever table opened first would decide
// `min_blob_size`/`enable_blob_files` for all of them.
static void restorePersistedBlobOptions(
	rocksdb::ColumnFamilyOptions& cfOptions,
	const PersistedCFOptions& persisted
) {
	cfOptions.enable_blob_files = persisted.enableBlobFiles;
	cfOptions.min_blob_size = persisted.minBlobSize;
	cfOptions.enable_blob_garbage_collection = persisted.enableBlobGarbageCollection;
	cfOptions.blob_garbage_collection_age_cutoff = persisted.blobGarbageCollectionAgeCutoff;
	cfOptions.blob_garbage_collection_force_threshold = persisted.blobGarbageCollectionForceThreshold;
	// Restored whether or not THIS process has a blob cache. The setting is inert
	// without one, but RocksDB rewrites the OPTIONS file on open, so dropping it
	// here would let any cache-less opener (a CLI tool, a `noBlockCache`
	// maintenance script) persist kDisable over the serving process's request and
	// silently turn prepopulation off at its next restart.
	cfOptions.prepopulate_blob_cache = persisted.prepopulateBlobCache;
#ifdef ROCKSDB_HAS_CF_BLOB_DIR
	cfOptions.blob_dir = persisted.blobDir;
#endif
}

// Applies only the blob settings the caller actually supplied. Fields left out
// keep whatever is already on `cfOptions` — the family's persisted value on a
// reopen — which is the same "a plain reopen inherits the live value" rule
// compression follows.
static void applyExplicitBlobOptions(
	rocksdb::ColumnFamilyOptions& cfOptions,
	const BlobOptions& blobs
) {
	if (blobs.enabled) {
		cfOptions.enable_blob_files = *blobs.enabled;
	}
	if (blobs.minSize) {
		cfOptions.min_blob_size = *blobs.minSize;
	}
	if (blobs.garbageCollection) {
		cfOptions.enable_blob_garbage_collection = *blobs.garbageCollection;
	}
	if (blobs.garbageCollectionAgeCutoff) {
		cfOptions.blob_garbage_collection_age_cutoff = *blobs.garbageCollectionAgeCutoff;
	}
	if (blobs.garbageCollectionForceThreshold) {
		cfOptions.blob_garbage_collection_force_threshold = *blobs.garbageCollectionForceThreshold;
	}
	if (blobs.prepopulateCache) {
		cfOptions.prepopulate_blob_cache = *blobs.prepopulateCache
			? rocksdb::PrepopulateBlobCache::kFlushOnly
			: rocksdb::PrepopulateBlobCache::kDisable;
	}
#ifdef ROCKSDB_HAS_CF_BLOB_DIR
	// Unlike the fields above, `dir` is not optional: an omitted directory means
	// "alongside the SST files", so it is always applied.
	cfOptions.blob_dir = blobs.dir;
#endif
}

// Applies a compression algorithm (and optional level) to both the SST block
// compression and the blob-file compression of the given column family options.
static void applyCompression(
	rocksdb::ColumnFamilyOptions& cfOptions,
	rocksdb::CompressionType compression,
	const std::optional<int>& level
) {
	cfOptions.compression = compression;
	// Large values are stored in blob files (enable_blob_files), which have their
	// own compression that defaults to none; apply the same algorithm so the
	// whole dataset is compressed, not just the inline (< min_blob_size) portion.
	cfOptions.blob_compression_type = compression;
	// An explicit request is "algorithm + optional level"; omitting the level
	// means the algorithm's default. When applied over a CF that inherited a
	// persisted level (e.g. cold-reopening a zstd-level-19 CF as zlib), that
	// inherited level must NOT carry over — reset it to the default sentinel so
	// the effective request matches what the API documents (and what the registry
	// warm-reopen check compares against).
	cfOptions.compression_opts.level =
		level ? *level : rocksdb::CompressionOptions::kDefaultCompressionLevel;
}

/**
 * Resolves `max_write_buffer_size_to_maintain` for a column family.
 *
 * Retained memtable history is a floor, not a cap — RocksDB trims down to this target and never
 * below — and that memory is charged to the process-wide WriteBufferManager. A target the budget
 * cannot hold is therefore a deadlock rather than backpressure: the budget fills with history that
 * is never released, and a manager built with `allowStall` stalls every write to the database
 * permanently.
 *
 * The derived default (`-1` → `maxWriteBufferNumber * writeBufferSize`) is dropped to 0 under any
 * stalling manager. Deliberately coarse: the safe per-family bound is the budget divided by the
 * live column-family count, which is not knowable here, so a comfortably-sized budget loses its
 * history window too. The cost is that conflict checking reports "cannot determine"
 * (`kTryAgain`) more often, which callers retry; the cost of the alternative is a hang.
 *
 * That cost was measured rather than assumed, which is why the coarse form is kept instead of a
 * budget-aware clamp: it is ~zero whenever flushing is organic (driven by memtables filling), and
 * only appears once flushes are frequent relative to transaction lifetime — at a forced 20ms
 * cadence against 20ms+ transactions it roughly doubles attempts per commit, as history is what
 * would otherwise resolve a non-conflicting transaction whose snapshot has already been flushed
 * away. A clamp would only recover that regime.
 *
 * An explicit caller value is honored untouched — sizing it against the budget and the
 * column-family count is then the caller's job.
 */
static int64_t resolveMaxWriteBufferSizeToMaintain(const DBOptions& options) {
	if (options.maxWriteBufferSizeToMaintain >= 0) {
		return options.maxWriteBufferSizeToMaintain;
	}
	DBSettings& settings = DBSettings::getInstance();
	if (settings.getWriteBufferManagerSize() > 0 && settings.getWriteBufferManagerAllowStall()) {
		return 0;
	}
	return options.maxWriteBufferSizeToMaintain;
}

rocksdb::ColumnFamilyOptions buildColumnFamilyOptions(
	const DBOptions& options,
	rocksdb::ColumnFamilyOptions cfOptions
) {
	rocksdb::BlockBasedTableOptions tableOptions;
	if (options.noBlockCache) {
		tableOptions.no_block_cache = true;
	} else {
		tableOptions.block_cache = DBSettings::getInstance().getBlockCache();
	}

	// RocksDB does not put blob values in the block cache, so without this every
	// blob read is real I/O — which dominates once blob files are on slower
	// storage than the SSTs.
	// `noBlockCache` means "this database does not use the process-wide caches",
	// so it opts out of the blob cache too — otherwise a scratch/bulk-load
	// database would evict the serving database's cached blob values.
	// Both directions for the same reason as prepopulate_blob_cache below:
	// leaving it untouched attaches the shared blob cache to a family created
	// with `noBlockCache`, which is the eviction that opt-out exists to prevent.
	cfOptions.blob_cache =
		options.noBlockCache ? nullptr : DBSettings::getInstance().getBlobCache();
	// The blob defaults belong to a column family being CREATED. A cold open of
	// an EXISTING family overwrites these from its OPTIONS file
	// (restorePersistedBlobOptions) and then re-applies only what the caller
	// explicitly asked for, to the target family alone.
	cfOptions.enable_blob_files = options.blobs.enabled.value_or(kDefaultBlobEnabled);
	cfOptions.min_blob_size = options.blobs.minSize.value_or(kDefaultBlobMinSize);
	cfOptions.enable_blob_garbage_collection =
		options.blobs.garbageCollection.value_or(kDefaultBlobGarbageCollection);
	cfOptions.blob_garbage_collection_age_cutoff =
		options.blobs.garbageCollectionAgeCutoff.value_or(kDefaultBlobGarbageCollectionAgeCutoff);
	cfOptions.blob_garbage_collection_force_threshold =
		options.blobs.garbageCollectionForceThreshold.value_or(
			kDefaultBlobGarbageCollectionForceThreshold);
	// Assigned in both directions, like every other field here: the warm path
	// builds on the descriptor's own `cfOptions`, so a one-sided assignment lets
	// the database's FIRST opener's setting ride along into every family created
	// later, persisted.
	cfOptions.prepopulate_blob_cache =
		options.blobs.prepopulateCache.value_or(kDefaultBlobPrepopulateCache)
			? rocksdb::PrepopulateBlobCache::kFlushOnly
			: rocksdb::PrepopulateBlobCache::kDisable;
#ifdef ROCKSDB_HAS_CF_BLOB_DIR
	cfOptions.blob_dir = options.blobs.dir;
#endif
	cfOptions.write_buffer_size = static_cast<size_t>(options.writeBufferSize);
	cfOptions.max_write_buffer_number = options.maxWriteBufferNumber;
	cfOptions.max_write_buffer_size_to_maintain = resolveMaxWriteBufferSizeToMaintain(options);
	cfOptions.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOptions));
	return cfOptions;
}

void ensureBlobDirExists(rocksdb::Env* env, const std::string& blobDir) {
#ifdef ROCKSDB_HAS_CF_BLOB_DIR
	if (blobDir.empty()) {
		return;
	}
	rocksdb::Status status = env->CreateDirIfMissing(blobDir);
	if (!status.ok()) {
		throw rocksdb_js::DBException(
			"Cannot use blobs.dir \"" + blobDir + "\": " + status.ToString()
		);
	}
#else
	(void)env;
	(void)blobDir;
#endif
}

// Reads each existing column family's persisted per-CF options (compression and
// blob settings) from the database's latest OPTIONS file into `result`,
// returning the RocksDB status. The OPTIONS file is the ONLY authoritative
// source for these (RocksDB does not restore per-CF options on open), so callers
// must treat a non-OK status as fatal for an existing DB rather than falling
// back to defaults — doing so would open the non-target CFs with the base
// defaults and silently restamp them on the next OPTIONS write.
static rocksdb::Status loadPersistedCFOptions(
	const std::string& path,
	std::unordered_map<std::string, PersistedCFOptions>& result
) {
	rocksdb::ConfigOptions configOptions;
	// Be permissive: we only read a handful of known fields, so
	// unknown/unsupported options in the persisted file must not fail the load.
	configOptions.ignore_unknown_options = true;
	configOptions.ignore_unsupported_options = true;
	rocksdb::DBOptions loadedDbOptions;
	std::vector<rocksdb::ColumnFamilyDescriptor> loadedCfDescriptors;
	rocksdb::Status status =
		rocksdb::LoadLatestOptions(configOptions, path, &loadedDbOptions, &loadedCfDescriptors);
	if (status.ok()) {
		for (const auto& descriptor : loadedCfDescriptors) {
			result[descriptor.name] = PersistedCFOptions{
				descriptor.options.compression,
				descriptor.options.blob_compression_type,
				descriptor.options.compression_opts,
				descriptor.options.enable_blob_files,
				descriptor.options.min_blob_size,
				descriptor.options.enable_blob_garbage_collection,
				descriptor.options.blob_garbage_collection_age_cutoff,
				descriptor.options.blob_garbage_collection_force_threshold,
				descriptor.options.prepopulate_blob_cache,
#ifdef ROCKSDB_HAS_CF_BLOB_DIR
				descriptor.options.blob_dir,
#else
				std::string(),
#endif
			};
		}
	}
	return status;
}

struct JobTracker final {
	int columnFamilyCount = 0;
	rocksdb::SequenceNumber flushedSequence = 0;
};

/**
 * Shared state between the RocksDB `EventListener` and the `DBDescriptor`,
 * created BEFORE `DB::Open` so background callbacks fired during open have a
 * valid, race-free target even though the descriptor does not exist yet
 * (HarperFast/rocksdb-js#754). The descriptor pointer is published under
 * `mutex_` once construction succeeds, and every read takes the same lock — so
 * there is no data race on the `weak_ptr`. (The previous design bound a shared
 * `weak_ptr` object after open while background threads called `lock()` on that
 * same object concurrently, which is undefined behavior.)
 *
 * A background error that latches before the descriptor is attached is stashed
 * as `pendingError_` (the same JSON form `setLastError` stores) and transferred
 * to the descriptor on publish, so an error during open still reaches
 * `getLastError()` / the `'error'` event instead of being silently dropped.
 */
struct DBEventListenerState final {
	// Flush callbacks: the attached descriptor, or null before attach / after close.
	std::shared_ptr<DBDescriptor> lockDescriptor() {
		std::lock_guard<std::mutex> lock(this->mutex_);
		return this->descriptor_.lock();
	}

	// OnBackgroundError: route the serialized error to the descriptor when it is
	// attached, else stash it for transfer on publish. Touches no N-API and never
	// blocks, so it is safe on a RocksDB background thread.
	void recordBackgroundError(std::string json) {
		std::shared_ptr<DBDescriptor> desc;
		{
			std::lock_guard<std::mutex> lock(this->mutex_);
			desc = this->descriptor_.lock();
			if (!desc) {
				this->pendingError_ = std::move(json);
				return;
			}
		}
		// setLastError stores + emits; call it outside our lock.
		desc->setLastError(std::move(json));
	}

	// Publish the descriptor once open succeeds and flush any error captured
	// during open. A concurrent recordBackgroundError therefore either stashes
	// (observed before publish, drained here) or routes straight to the
	// descriptor (after) — never lost.
	void publishDescriptor(std::shared_ptr<DBDescriptor> descriptor) {
		std::string pending;
		{
			std::lock_guard<std::mutex> lock(this->mutex_);
			this->descriptor_ = descriptor;
			pending.swap(this->pendingError_);
		}
		if (!pending.empty()) {
			descriptor->setLastError(std::move(pending));
		}
	}

private:
	std::mutex mutex_;
	std::weak_ptr<DBDescriptor> descriptor_;
	std::string pendingError_;
};

/**
 * Custom event listener that handles flush completion events and notifies
 * transaction log stores to track what has been flushed to the database.
 */
class TransactionLogEventListener : public rocksdb::EventListener {
public:
	TransactionLogEventListener(std::shared_ptr<DBEventListenerState> state)
		: state(std::move(state)) {}

	void OnFlushBegin(rocksdb::DB* db, const rocksdb::FlushJobInfo& flush_info) override {
		auto desc = this->state->lockDescriptor();
		if (!desc) {
			return;
		}
		// RocksDB can run flushes concurrently across background threads, so guard
		// the shared jobTrackers map — concurrent std::unordered_map access is a
		// data race.
		std::lock_guard<std::mutex> jobLock(this->jobTrackersMutex);
		// Track flush job by job_id, so we can determine when all the flushes have completed for
		// With atomic flushes, there will be multiple flush events for each column family in the database
		// We we want to flush at the beginning of the flush job (for first time job_id appears)
		// And then we want to track the job so that we can determine when all the flushes have completed for
		// the database job.
		auto it = this->jobTrackers.find(flush_info.job_id);
		if (it == this->jobTrackers.end()) {
			// Create new entry
			JobTracker tracker;
			tracker.columnFamilyCount = 1;
			rocksdb::SequenceNumber flushedSequence = flush_info.largest_seqno;
			tracker.flushedSequence = flushedSequence;
			this->jobTrackers[flush_info.job_id] = tracker;
			DEBUG_LOG("%p TransactionLogEventListener::OnFlushBegin flushedSequence=%llu\n",
				desc.get(), (unsigned long long)flushedSequence);

			// Get stores from the registry
			auto stores = TransactionLogStoreRegistry::GetStores(desc->path);
			for (auto& store : stores) {
				store->databaseFlushBegin(flushedSequence);
			}
		} else {
			// Increment existing entry so we know how many column families are being flushed
			it->second.columnFamilyCount++;
		}
	}

	void OnFlushCompleted(rocksdb::DB* db, const rocksdb::FlushJobInfo& flush_info) override {
		auto desc = this->state->lockDescriptor();
		if (!desc) {
			return;
		}

		rocksdb::SequenceNumber flushedSequence = flush_info.largest_seqno;
		DEBUG_LOG("%p TransactionLogEventListener::OnFlushCompleted cf name=%s job id=%u flushedSequence=%llu\n",
			desc.get(), flush_info.cf_name.c_str(), flush_info.job_id, (unsigned long long)flushedSequence);

		// Guard the shared jobTrackers map — see OnFlushBegin.
		std::lock_guard<std::mutex> jobLock(this->jobTrackersMutex);
		// Track flush job by job_id
		auto it = this->jobTrackers.find(flush_info.job_id);
		if (it == this->jobTrackers.end()) {
			DEBUG_LOG("%p TransactionLogEventListener::OnFlushCompleted unable to find job id=%d\n",
				desc.get(), flush_info.job_id);
		} else {
			// we find the highest sequence number; this represents the overall sequence
			// number for the flush job
			if (flushedSequence > it->second.flushedSequence) {
				it->second.flushedSequence = flushedSequence;
			}
			// Decrement existing entry until we have completed all the flush actions for the job
			if (--it->second.columnFamilyCount == 0) {
				// The last CF flush has completed for the job, now signal that the database flush is done
				DEBUG_LOG("%p TransactionLogEventListener::OnFlushCompleted job completed name=%s job id=%d flushedSequence=%llu\n",
					desc.get(), flush_info.cf_name.c_str(), flush_info.job_id, (unsigned long long)it->second.flushedSequence);

				// Get stores from the registry
				auto stores = TransactionLogStoreRegistry::GetStores(desc->path);
				for (auto& store : stores) {
					store->databaseFlushed(it->second.flushedSequence);
				}
				this->jobTrackers.erase(it); // cleanup
			}
		}
	}

	// Surfaces a RocksDB background error to JS (HarperFast/rocksdb-js#730).
	// Serializes it to a JSON string and hands it to `setLastError`, which stores
	// it (readable on demand via `db.getLastError()`) and emits the `'error'`
	// event — both reconstruct the same `BackgroundError` from this string on the
	// JS thread, so nothing N-API/env-bound is touched here. We do NOT suppress
	// the error (leaving *bgError untouched) — the point is to surface it, not
	// hide it. Runs on flush/compaction/write threads; storing a string and the
	// thread-safe, asynchronous emit keep this cheap and non-blocking.
	void OnBackgroundError(rocksdb::BackgroundErrorReason reason, rocksdb::Status* bgError) override {
		if (bgError == nullptr) {
			return;
		}
		// Route through the shared state (NOT the descriptor directly): an error
		// latched during DB::Open, before the descriptor is attached, is stashed
		// and transferred on publish rather than dropped (#754).
		int severity = static_cast<int>(bgError->severity());
		int reasonInt = static_cast<int>(reason);
		this->state->recordBackgroundError(backgroundErrorToJson(
			bgError->ToString(),
			severity,
			backgroundErrorSeverityName(severity),
			backgroundErrorDisablesWrites(severity),
			reasonInt,
			backgroundErrorReasonName(reasonInt)
		));
	}

	// Surface RocksDB write-stall transitions to JS. Fires when a column family
	// crosses between kNormal/kDelayed/kStopped — the earliest push signal that
	// writes are being throttled (kDelayed) or blocked (kStopped), e.g. the
	// dbWriteBufferSize-oversubscription thrash. Runs on a RocksDB background
	// thread; emitWriteStall touches no napi (async emit) and debounces, so this
	// is cheap and non-blocking, same discipline as OnBackgroundError.
	void OnStallConditionsChanged(const rocksdb::WriteStallInfo& info) override {
		auto desc = this->state->lockDescriptor();
		if (!desc) {
			return;
		}
		// Runs on a RocksDB background thread; an exception unwinding into RocksDB
		// is undefined behavior. A dropped stall notification is acceptable, so
		// contain anything the emit path throws (e.g. a bad_alloc from the payload
		// allocation) rather than let it escape.
		try {
			desc->emitWriteStall(info.cf_name, info.condition.prev, info.condition.cur);
		} catch (...) {
		}
	}

private:
	std::shared_ptr<DBEventListenerState> state;
	std::mutex jobTrackersMutex;
	std::unordered_map<int, JobTracker> jobTrackers;
};

// Defined below; forward-declared so the constructor can resolve the debounce
// window on the JS thread (see writeStallDebounceWindowMs).
static uint64_t writeStallDebounceMs();

/**
 * Creates a new database descriptor. This constructor is private. To create a
 * new DBDescriptor, use `DBDescriptor::open()`.
 */
DBDescriptor::DBDescriptor(
	const std::string& path,
	const DBOptions& options,
	const rocksdb::ColumnFamilyOptions& cfOptions,
	std::shared_ptr<rocksdb::DB> db,
	std::unordered_map<std::string, std::shared_ptr<ColumnFamilyDescriptor>>&& columns,
	std::shared_ptr<rocksdb::Statistics> statistics
):
	path(path),
	vtEpoch(nextVtEpoch()),
	mode(options.mode),
	readOnly(options.readOnly),
	cfOptions(cfOptions),
	db(db),
	columns(std::move(columns)),
	statistics(statistics)
{
	// Resolve the debounce window here (JS thread, open path) so the emit path on
	// a RocksDB background thread reads a plain field instead of calling ::getenv.
	this->writeStallDebounceWindowMs = writeStallDebounceMs();
}

/**
 * Destroy the database descriptor and any resources associated to it
 * (transactions, iterators, etc).
 */
DBDescriptor::~DBDescriptor() {
	DEBUG_LOG("%p DBDescriptor::~DBDescriptor Closing \"%s\"\n", this, this->path.c_str());
	this->close();
	// Idempotent safety net, matching commitWorker/logWorker's own
	// destructor shutdown.
	this->parkTimeouts->shutdown();
}

/**
 * Close the database descriptor and any resources associated with it
 * (transactions, iterators, etc).
 */
void DBDescriptor::close() {
	// check if already closing
	if (!this->beginClose()) {
		DEBUG_LOG("%p DBDescriptor::close Already closing \"%s\"\n", this, this->path.c_str());
		return;
	}

	this->finishClose();
}

void DBDescriptor::finishClose() {
	DEBUG_LOG("%p DBDescriptor::close Closing \"%s\" (mode=%s read-only=%s closables=%zu columns=%zu transactions=%zu)\n",
		this, this->path.c_str(), this->mode == DBMode::Optimistic ? "optimistic" : "pessimistic", this->readOnly ? "true" : "false", this->closables.size(), this->columns.size(), this->transactions.size());

	// Wait for all in-flight operations to complete before cleanup.
	// The closing flag is already set, so new operations will fail with "Database is closing".
	// Existing operations will decrement operationsInFlight and notify us when done.
	DEBUG_LOG("%p DBDescriptor::close Waiting for %u in-flight operations \"%s\"\n", this, this->operationsInFlight.load(), this->path.c_str());
	uint32_t current;
	while ((current = this->operationsInFlight.load()) != 0) {
		this->operationsInFlight.wait(current);
	}
	DEBUG_LOG("%p DBDescriptor::close All operations complete \"%s\"\n", this, this->path.c_str());

	// Drain the commit pipeline before flushing so its data is included in
	// the flush. The log lane feeds the commit lane, so it must drain first;
	// its final tasks enqueue onto the still-running commit lane (or run
	// inline once that lane stops).
	this->logWorker.shutdown();
	this->commitWorker.shutdown();

	// Release any remaining per-env commit-completion tsfns. An in-flight
	// commit pins this descriptor (state -> txnHandle -> dbHandle -> descriptor),
	// so reaching here means no commit is in flight; only idle (unref'd) tsfns
	// for still-living envs can remain, and those envs will issue no further
	// commits to this descriptor. Queued completions already handed to a tsfn
	// are still delivered (napi_tsfn_release, not abort).
	{
		std::lock_guard<std::mutex> lock(this->commitMutex);
		for (auto& [env, completion] : this->commitCompletions) {
			if (completion.tsfn) {
				::napi_release_threadsafe_function(completion.tsfn, napi_tsfn_release);
			}
		}
		this->commitCompletions.clear();
		// Block any later registerCommitCompletion (a commit racing this close
		// from another env) from re-creating a tsfn that would never be
		// released; such commits fall back to the legacy libuv path.
		this->commitCompletionsClosed = true;
	}

	// We want to ensure that all in-memory data is written to disk. Keeps the waiting default on
	// purpose: flushing immediately here races transaction-log-store teardown (AGENTS invariant 15).
	this->flush();

	// Trigger manual compaction on all column families to reclaim space from
	// tombstones before closing
	if (!this->readOnly && DBSettings::getInstance().getCompactOnClose()) {
		// Snapshot under the columns mutex; a concurrent drop can erase from
		// the map while we compact.
		std::vector<std::shared_ptr<ColumnFamilyDescriptor>> pinnedColumns;
		{
			std::lock_guard<std::mutex> columnsLock(this->columnsMutex);
			pinnedColumns.reserve(this->columns.size());
			for (const auto& [name, columnDesc] : this->columns) {
				pinnedColumns.push_back(columnDesc);
			}
		}
		for (const auto& columnDesc : pinnedColumns) {
			if (columnDesc && columnDesc->column) {
				this->compactRange(columnDesc->column.get(), nullptr, nullptr);
			}
		}
	}

	// Wait for any outstanding (background threads) operations to complete.
	// Note that this is not setting the RocksDB `close_db` flag since active
	// references to the databases may still exist. Also, contrary to the
	// suggestions of the documentation, this method alone does not seem to
	// trigger a flush
	rocksdb::WaitForCompactOptions options;
	this->db->WaitForCompact(options);

	std::unique_lock<std::mutex> txnsLock(this->txnsMutex);

	// Close all handles that still exist and reset their descriptor references
	for (auto it = this->closables.begin(); it != this->closables.end(); ) {
		if (auto closable = it->second.lock()) {
			// Remove from map before closing to avoid re-entrant detach() calls
			it = this->closables.erase(it);

			// Release mutex during close to avoid deadlocks
			txnsLock.unlock();
			closable->close();
			txnsLock.lock();
		} else {
			// Handle was already GC'd, just remove the expired weak_ptr
			it = this->closables.erase(it);
		}
	}

	// Safety-net: cancel any VT locks still held by this DB after all
	// TransactionHandles have been closed. Under normal operation the
	// closable->close() calls above already call releaseIntent() + wake()
	// for every transaction; this is a defensive final pass.
	{
		auto* vt = DBSettings::getInstance().getVerificationTableRaw();
		if (vt) {
			vt->cancelForDB(this->vtEpoch);
		}
	}

	// A park can be registered on a foreign-dbId tracker (colliding VT slot;
	// see the ParkTimeout header comment), so cancelForDB() above cannot be
	// relied on to have woken everything this descriptor is waiting on.
	// ParkTimeoutRegistry::shutdown() resolves whatever is left regardless.
	this->parkTimeouts->shutdown();

	// Unregister from transaction log store registry - this will clean up stores
	// when the last descriptor for this path is closed
	TransactionLogStoreRegistry::Unregister(this->path);

	this->transactions.clear();
	{
		std::lock_guard<std::mutex> columnsLock(this->columnsMutex);
		this->columns.clear();
	}

	this->events.releaseAll();

	this->db.reset();
}

napi_status DBDescriptor::registerCommitCompletion(napi_env env, napi_threadsafe_function_call_js callJs, bool& closed) {
	std::lock_guard<std::mutex> lock(this->commitMutex);
	closed = this->commitCompletionsClosed;
	if (closed) {
		return napi_ok;
	}
	CommitCompletion& completion = this->commitCompletions[env];
	if (completion.tsfn == nullptr) {
		napi_value resourceName;
		napi_status status = ::napi_create_string_utf8(env, "rocksdb.commit", NAPI_AUTO_LENGTH, &resourceName);
		if (status != napi_ok) {
			return status;
		}
		// Created ref'd (thread count 1 for the commit thread), which is what we
		// want with a commit about to be dispatched.
		status = ::napi_create_threadsafe_function(
			env,
			nullptr,   // func: callJs does all the work
			nullptr,   // async_resource
			resourceName,
			0,         // unlimited queue
			1,         // initial thread count: the commit thread
			nullptr,   // finalize data
			nullptr,   // finalize cb
			nullptr,   // context
			callJs,
			&completion.tsfn
		);
		if (status != napi_ok) {
			this->commitCompletions.erase(env);
			return status;
		}
	} else if (completion.pending == 0) {
		// Waking from idle: keep the event loop alive until completion.
		napi_status status = ::napi_ref_threadsafe_function(env, completion.tsfn);
		if (status != napi_ok) {
			return status;
		}
	}
	completion.pending++;
	return napi_ok;
}

bool DBDescriptor::dispatchCommitCompletion(napi_env env, void* state) {
	std::lock_guard<std::mutex> lock(this->commitMutex);
	auto it = this->commitCompletions.find(env);
	if (it == this->commitCompletions.end() || it->second.tsfn == nullptr) {
		// env was torn down / released; the caller drops the state.
		return false;
	}
	napi_status status = ::napi_call_threadsafe_function(it->second.tsfn, state, napi_tsfn_nonblocking);
	return status == napi_ok;
}

void DBDescriptor::finishCommitCompletion(napi_env env) {
	std::lock_guard<std::mutex> lock(this->commitMutex);
	auto it = this->commitCompletions.find(env);
	if (it != this->commitCompletions.end() && --it->second.pending == 0 && it->second.tsfn != nullptr) {
		// Idle: allow the event loop to exit.
		::napi_unref_threadsafe_function(env, it->second.tsfn);
	}
}

void DBDescriptor::releaseCommitCompletionsByEnv(napi_env env) {
	std::lock_guard<std::mutex> lock(this->commitMutex);
	auto it = this->commitCompletions.find(env);
	if (it != this->commitCompletions.end()) {
		if (it->second.tsfn != nullptr) {
			// Queued completions are still delivered before the tsfn finalizes.
			::napi_release_threadsafe_function(it->second.tsfn, napi_tsfn_release);
		}
		this->commitCompletions.erase(it);
	}
}

uint64_t ParkTimeoutRegistry::schedule(
	napi_env env,
	unsigned timeoutMs,
	napi_threadsafe_function tsfn,
	std::shared_ptr<std::atomic<bool>> fired
) {
	std::lock_guard<std::mutex> lock(this->mutex);
	if (this->stopped) {
		// Descriptor already closing: the caller must resolve inline without
		// registering with the LockTracker at all (see the header comment).
		return 0;
	}
	if (!this->threadStarted) {
		try {
			this->thread = std::thread([this]() { this->runLoop(); });
		} catch (...) {
			// Thread creation failed (e.g. thread/resource exhaustion): leave
			// the flag false so the next park retries, and tell the caller to
			// resolve inline now rather than register a park nothing will
			// ever time out.
			return 0;
		}
		this->threadStarted = true;
	}
	auto entry = std::make_unique<ParkTimeout>();
	entry->id = this->nextId++;
	entry->env = env;
	entry->tsfn = tsfn;
	entry->fired = std::move(fired);
	uint64_t id = entry->id;
	auto deadlineIt = this->deadlines.emplace(
		std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs),
		id
	);
	entry->deadlineIt = deadlineIt;
	this->parks.emplace(id, std::move(entry));
	if (deadlineIt == this->deadlines.begin()) {
		// Only the new earliest deadline needs the loop re-armed (this also
		// covers waking it out of the indefinite wait when `deadlines` was
		// empty); any later one already fires within a wait it will take.
		this->cv.notify_all();
	}
	return id;
}

std::unique_ptr<ParkTimeoutRegistry::ParkTimeout> ParkTimeoutRegistry::take(uint64_t id) {
	auto it = this->parks.find(id);
	if (it == this->parks.end()) {
		return nullptr;
	}
	std::unique_ptr<ParkTimeout> owned = std::move(it->second);
	this->deadlines.erase(owned->deadlineIt);
	this->parks.erase(it);
	return owned;
}

void ParkTimeoutRegistry::resolve(ParkTimeout& park) {
	bool expected = false;
	if (park.fired->compare_exchange_strong(expected, true)) {
		// A closing tsfn (env teardown racing this resolve) must not be
		// touched again -- napi_closing means Node may already be freeing it.
		napi_status status = ::napi_call_threadsafe_function(park.tsfn, nullptr, napi_tsfn_nonblocking);
		if (status == napi_ok) {
			::napi_release_threadsafe_function(park.tsfn, napi_tsfn_release);
		}
	}
}

void ParkTimeoutRegistry::runLoop() {
	setThreadName("rocksdb-park-timeout");
	std::unique_lock<std::mutex> lock(this->mutex);
	for (;;) {
		if (this->stopped) {
			return;
		}
		if (this->deadlines.empty()) {
			this->cv.wait(lock);
			continue;
		}
		auto now = std::chrono::steady_clock::now();
		// Copy the deadline: wait_until releases the lock while parked, during
		// which this entry can be erased (a real wake racing the timeout) and
		// the map node freed -- a bound reference into it would be a read of
		// freed memory once the wait re-checks time.
		std::chrono::steady_clock::time_point earliest = this->deadlines.begin()->first;
		if (earliest > now) {
			this->cv.wait_until(lock, earliest);
			continue;
		}
		// Fire while still holding the mutex, like dispatchCommitCompletion.
		while (!this->deadlines.empty() && this->deadlines.begin()->first <= now) {
			auto deadlineIt = this->deadlines.begin();
			auto parkIt = this->parks.find(deadlineIt->second);
			this->deadlines.erase(deadlineIt);
			if (parkIt == this->parks.end()) {
				continue;
			}
			std::unique_ptr<ParkTimeout> due = std::move(parkIt->second);
			this->parks.erase(parkIt);
			ParkTimeoutRegistry::resolve(*due);
		}
	}
}

void ParkTimeoutRegistry::fire(uint64_t id) {
	std::lock_guard<std::mutex> lock(this->mutex);
	std::unique_ptr<ParkTimeout> owned = this->take(id);
	if (!owned) {
		// Already claimed by the timeout thread, releaseByEnv, or shutdown.
		return;
	}
	ParkTimeoutRegistry::resolve(*owned);
}

void ParkTimeoutRegistry::releaseByEnv(napi_env env) {
	std::lock_guard<std::mutex> lock(this->mutex);
	for (auto it = this->parks.begin(); it != this->parks.end();) {
		if (it->second->env != env) {
			++it;
			continue;
		}
		// Mark fired first so neither the timeout thread nor a later real
		// wake ever calls into the tsfn we're about to release -- the
		// promise's env is gone, nothing is listening for the resolve.
		bool expected = false;
		it->second->fired->compare_exchange_strong(expected, true);
		if (!expected) {
			::napi_release_threadsafe_function(it->second->tsfn, napi_tsfn_release);
		}
		this->deadlines.erase(it->second->deadlineIt);
		it = this->parks.erase(it);
	}
}

void ParkTimeoutRegistry::shutdown() {
	std::thread toJoin;
	{
		std::lock_guard<std::mutex> lock(this->mutex);
		if (this->stopped && !this->threadStarted) {
			// Already fully shut down (e.g. finishClose() already ran; this is
			// the destructor's belt-and-suspenders call) -- nothing left to do.
			return;
		}
		this->stopped = true;
		if (this->threadStarted) {
			toJoin = std::move(this->thread);
			this->threadStarted = false;
		}
		// Resolve every park still pending, under the same mutex the other
		// three methods serialize their tsfn calls on -- draining outside the
		// lock would let a concurrent releaseByEnv for a dying env observe
		// "nothing to cancel" while this is mid-call on that same env's tsfn,
		// racing Node freeing it.
		for (auto& entry : this->parks) {
			ParkTimeoutRegistry::resolve(*entry.second);
		}
		this->parks.clear();
		this->deadlines.clear();
	}
	// Notify + join outside the lock: the loop's cv.wait_until needs to
	// re-acquire the mutex to observe `stopped` and return, so joining while
	// still holding it would deadlock.
	this->cv.notify_all();
	if (toJoin.joinable()) {
		toJoin.join();
	}
}

ParkTimeoutRegistry::~ParkTimeoutRegistry() {
	this->shutdown();
}

/**
 * Registers a database resource to be closed when the descriptor is closed.
 *
 * Important: The closable must be same smart_ptr that is napi-wrapped and
 * bound to the JavaScript class counterpart.
 */
void DBDescriptor::attach(std::shared_ptr<Closable> closable) {
	std::lock_guard<std::mutex> lock(this->txnsMutex);
	this->closables[closable.get()] = std::weak_ptr<Closable>(closable);
}

/**
 * Unregisters a database resource from being closed when the descriptor is
 * closed.
 */
void DBDescriptor::detach(std::shared_ptr<Closable> closable) {
	std::lock_guard<std::mutex> lock(this->txnsMutex);
	this->closables.erase(closable.get());
}

#define SET_DOUBLE_PROP(obj, name, value) \
	do { \
		napi_value jsValue; \
		NAPI_STATUS_THROWS(::napi_create_double(env, value, &jsValue)); \
		NAPI_STATUS_THROWS(::napi_set_named_property(env, obj, name, jsValue)); \
	} while (0)

#define SET_INT64_PROP(obj, name, value) \
	do { \
		napi_value jsValue; \
		NAPI_STATUS_THROWS(::napi_create_int64(env, value, &jsValue)); \
		NAPI_STATUS_THROWS(::napi_set_named_property(env, obj, name, jsValue)); \
	} while (0)

#define SET_HISTOGRAM_DATA_PROP(obj, name, histogram) \
	do { \
		rocksdb::HistogramData hist; \
		this->statistics->histogramData(histogram, &hist); \
		napi_value jsValue = buildHistogramDataObject(env, hist); \
		NAPI_STATUS_THROWS(::napi_set_named_property(env, obj, name, jsValue)); \
	} while (0)

napi_value buildHistogramDataObject(napi_env env, const rocksdb::HistogramData& hist) {
	napi_value obj;
	NAPI_STATUS_THROWS(::napi_create_object(env, &obj));

	SET_DOUBLE_PROP(obj, "average", hist.average);
	SET_INT64_PROP(obj, "count", hist.count);
	SET_DOUBLE_PROP(obj, "max", hist.max);
	SET_DOUBLE_PROP(obj, "median", hist.median);
	SET_DOUBLE_PROP(obj, "min", hist.min);
	SET_DOUBLE_PROP(obj, "percentile95", hist.percentile95);
	SET_DOUBLE_PROP(obj, "percentile99", hist.percentile99);
	SET_DOUBLE_PROP(obj, "standardDeviation", hist.standard_deviation);
	SET_INT64_PROP(obj, "sum", hist.sum);

	return obj;
}

napi_value DBDescriptor::getStat(napi_env env, const std::string& statName) {
	if (!this->statistics) {
		::napi_throw_error(env, nullptr, "Statistics are not enabled");
		NAPI_RETURN_UNDEFINED();
	}

	for (const auto& [ticker, name] : rocksdb::TickersNameMap) {
		if (name == statName) {
			uint64_t value = this->statistics->getTickerCount(ticker);
			napi_value result;
			NAPI_STATUS_THROWS(::napi_create_int64(env, value, &result));
			return result;
		}
	}

	for (const auto& [histogram, name] : rocksdb::HistogramsNameMap) {
		if (name == statName) {
			rocksdb::HistogramData hist;
			this->statistics->histogramData(histogram, &hist);
			return buildHistogramDataObject(env, hist);
		}
	}

	NAPI_RETURN_UNDEFINED();
}

bool DBDescriptor::getStats(napi_env env, bool all, napi_value* result) {
	if (!this->statistics) {
		return false;
	}

#undef NAPI_STATUS_THROWS
#define NAPI_STATUS_THROWS(call) NAPI_STATUS_THROWS_RVAL(call, false)

	NAPI_STATUS_THROWS(::napi_create_object(env, result));

	if (all) {
		// get all stats
		for (const auto& [ticker, name] : rocksdb::TickersNameMap) {
			napi_value value;
			NAPI_STATUS_THROWS(::napi_create_int64(env, this->statistics->getTickerCount(ticker), &value));
			napi_value key;
			NAPI_STATUS_THROWS(::napi_create_string_utf8(env, name.c_str(), name.size(), &key));
			NAPI_STATUS_THROWS(::napi_set_property(env, *result, key, value));
		}

		for (const auto& [histogram, name] : rocksdb::HistogramsNameMap) {
			rocksdb::HistogramData hist;
			this->statistics->histogramData(histogram, &hist);
			napi_value key;
			NAPI_STATUS_THROWS(::napi_create_string_utf8(env, name.c_str(), name.size(), &key));
			napi_value value = buildHistogramDataObject(env, hist);
			NAPI_STATUS_THROWS(::napi_set_property(env, *result, key, value));
		}
	} else {
		// get essential stats

		// block cache
		SET_INT64_PROP(*result, "rocksdb.block.cache.hit", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_HIT));
		SET_INT64_PROP(*result, "rocksdb.block.cache.miss", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_MISS));
		SET_INT64_PROP(*result, "rocksdb.block.cache.data.hit", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_DATA_HIT));
		SET_INT64_PROP(*result, "rocksdb.block.cache.data.miss", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_DATA_MISS));
		SET_INT64_PROP(*result, "rocksdb.block.cache.index.hit", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_INDEX_HIT));
		SET_INT64_PROP(*result, "rocksdb.block.cache.index.miss", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_INDEX_MISS));
		SET_INT64_PROP(*result, "rocksdb.block.cache.filter.hit", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_FILTER_HIT));
		SET_INT64_PROP(*result, "rocksdb.block.cache.filter.miss", this->statistics->getTickerCount(rocksdb::Tickers::BLOCK_CACHE_FILTER_MISS));

		// bloom filter
		SET_INT64_PROP(*result, "rocksdb.bloom.filter.useful", this->statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL));
		SET_INT64_PROP(*result, "rocksdb.bloom.filter.full.positive", this->statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_FULL_POSITIVE));
		SET_INT64_PROP(*result, "rocksdb.bloom.filter.full.true.positive", this->statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_FULL_TRUE_POSITIVE));

		// iterators
		SET_INT64_PROP(*result, "rocksdb.db.iter.bytes.read", this->statistics->getTickerCount(rocksdb::Tickers::ITER_BYTES_READ));
		SET_INT64_PROP(*result, "rocksdb.number.reseeks.iteration", this->statistics->getTickerCount(rocksdb::Tickers::NUMBER_OF_RESEEKS_IN_ITERATION));

		// keys
		SET_INT64_PROP(*result, "rocksdb.number.keys.read", this->statistics->getTickerCount(rocksdb::Tickers::NUMBER_KEYS_READ));
		SET_INT64_PROP(*result, "rocksdb.number.keys.written", this->statistics->getTickerCount(rocksdb::Tickers::NUMBER_KEYS_WRITTEN));

		// values
		SET_INT64_PROP(*result, "rocksdb.bytes.read", this->statistics->getTickerCount(rocksdb::Tickers::BYTES_READ));
		SET_INT64_PROP(*result, "rocksdb.bytes.written", this->statistics->getTickerCount(rocksdb::Tickers::BYTES_WRITTEN));

		// memtable
		SET_INT64_PROP(*result, "rocksdb.memtable.hit", this->statistics->getTickerCount(rocksdb::Tickers::MEMTABLE_HIT));
		SET_INT64_PROP(*result, "rocksdb.memtable.miss", this->statistics->getTickerCount(rocksdb::Tickers::MEMTABLE_MISS));

		// transactions
		SET_INT64_PROP(*result, "rocksdb.txn.overhead.mutex.prepare", this->statistics->getTickerCount(rocksdb::Tickers::TXN_PREPARE_MUTEX_OVERHEAD));
		SET_INT64_PROP(*result, "rocksdb.txn.overhead.mutex.old.commit.map", this->statistics->getTickerCount(rocksdb::Tickers::TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD));
		SET_INT64_PROP(*result, "rocksdb.txn.overhead.mutex.snapshot", this->statistics->getTickerCount(rocksdb::Tickers::TXN_SNAPSHOT_MUTEX_OVERHEAD));

		// compaction
		SET_INT64_PROP(*result, "rocksdb.compact.read.bytes", this->statistics->getTickerCount(rocksdb::Tickers::COMPACT_READ_BYTES));
		SET_INT64_PROP(*result, "rocksdb.compact.write.bytes", this->statistics->getTickerCount(rocksdb::Tickers::COMPACT_WRITE_BYTES));
		SET_INT64_PROP(*result, "rocksdb.compaction.cancelled", this->statistics->getTickerCount(rocksdb::Tickers::COMPACTION_CANCELLED));
		SET_INT64_PROP(*result, "rocksdb.stall.micros", this->statistics->getTickerCount(rocksdb::Tickers::STALL_MICROS));

		// errors & i/o
		SET_INT64_PROP(*result, "rocksdb.no.file.errors", this->statistics->getTickerCount(rocksdb::Tickers::NO_FILE_ERRORS));
		SET_INT64_PROP(*result, "rocksdb.read.amp.estimate.useful.bytes", this->statistics->getTickerCount(rocksdb::Tickers::READ_AMP_ESTIMATE_USEFUL_BYTES));
		SET_INT64_PROP(*result, "rocksdb.read.amp.total.read.bytes", this->statistics->getTickerCount(rocksdb::Tickers::READ_AMP_TOTAL_READ_BYTES));

		// histogram data
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.db.get.micros", rocksdb::Histograms::DB_GET);
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.db.write.micros", rocksdb::Histograms::DB_WRITE);
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.db.seek.micros", rocksdb::Histograms::DB_SEEK);
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.db.flush.micros", rocksdb::Histograms::FLUSH_TIME);
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.db.write.stall", rocksdb::Histograms::WRITE_STALL);
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.blobdb.value.size", rocksdb::Histograms::BLOB_DB_VALUE_SIZE);
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.sst.read.micros", rocksdb::Histograms::SST_READ_MICROS);
		SET_HISTOGRAM_DATA_PROP(*result, "rocksdb.compaction.times.micros", rocksdb::Histograms::COMPACTION_TIME);
	}

#undef NAPI_STATUS_THROWS
#define NAPI_STATUS_THROWS(call) NAPI_STATUS_THROWS_RVAL(call, nullptr)

	return true;
}

/**
 * Adds the callback to a queue to be executed mutually exclusive and if the
 * lock is available, executes it immediately followed by any newly queued
 * callbacks. Called by `db.withLock()`.
 */
void DBDescriptor::lockCall(
	napi_env env,
	std::string& key,
	napi_value callback,
	napi_deferred deferred,
	std::shared_ptr<DBHandle> owner
) {
	bool isNewLock = false;
	this->lockEnqueueCallback(
		env,       // env
		key,       // key
		callback,  // callback
		owner,     // owner
		false,     // skipEnqueueIfNewLock
		deferred,  // deferred
		&isNewLock // [out] isNewLock
	);

	if (!isNewLock) {
		DEBUG_LOG("%p DBDescriptor::lockCall callback queued for key:", this);
		DEBUG_LOG_KEY_LN(key);
		return;
	}

	// lock found
	std::unique_lock<std::mutex> locksMutex(this->locksMutex);
	auto lockHandle = this->locks.find(key);

	if (lockHandle == this->locks.end()) {
		DEBUG_LOG("%p DBDescriptor::lockCall no lock found for key:", this);
		DEBUG_LOG_KEY_LN(key);
		return;
	}

	auto& handle = lockHandle->second;

	// try to acquire the "lock" atomically
	bool expected = false;
	if (!handle->isRunning.compare_exchange_strong(expected, true)) {
		// another callback is already running
		DEBUG_LOG("%p DBDescriptor::lockCall another callback is already running for key:", this);
		DEBUG_LOG_KEY_LN(key);
		return;
	}

	// we now "own" the execution for this key
	if (handle->threadsafeCallbacks.empty()) {
		handle->isRunning.store(false);
		DEBUG_LOG("%p DBDescriptor::lockCall no callbacks left, removing lock for key:", this);
		DEBUG_LOG_KEY_LN(key);
		// remove the empty lock handle from the map
		this->locks.erase(key);
		return;
	}

	LockCallback lockCallback = handle->threadsafeCallbacks.front();
	handle->threadsafeCallbacks.pop();
	napi_threadsafe_function threadsafeCallback = lockCallback.callback;

	// release the mutex before calling the callback to avoid holding locks
	// during callback execution
	locksMutex.unlock();

	if (!threadsafeCallback) {
		DEBUG_LOG("%p DBDescriptor::lockCall threadsafe lock callback is null for key:", this);
		DEBUG_LOG_KEY_LN(key);
		return;
	}

	DEBUG_LOG("%p DBDescriptor::lockCall calling callback for key:", this);
	DEBUG_LOG_KEY_LN(key);

	// create callback data that includes the key for completion and deferred promise
	auto* callbackData = new LockCallbackCompletionData(key, weak_from_this(), lockCallback.deferred);

	// use threadsafe function instead of direct call
	napi_status status = ::napi_call_threadsafe_function(threadsafeCallback, callbackData, napi_tsfn_blocking);
	if (status != napi_ok && status != napi_closing) {
		DEBUG_LOG("%p DBDescriptor::lockCall failed to call threadsafe function\n", this);
		delete callbackData;
		this->onCallbackComplete(key);
	}

	// release the threadsafe function
	::napi_release_threadsafe_function(threadsafeCallback, napi_tsfn_release);
}

/**
 * Enqueues a callback to be called when a lock is acquired. Called by
 * `db.tryLock()` and `DBDescriptor::lockCall()`.
 */
void DBDescriptor::lockEnqueueCallback(
	napi_env env,
	std::string& key,
	napi_value callback,
	std::shared_ptr<DBHandle> owner,
	bool skipEnqueueIfNewLock,
	napi_deferred deferred,
	bool* isNewLock
) {
	std::lock_guard<std::mutex> lock(this->locksMutex);
	std::shared_ptr<LockHandle> lockHandle;
	auto lockHandleIterator = this->locks.find(key);

	if (lockHandleIterator == this->locks.end()) {
		// no lock found
		DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback no lock found for key:", this);
		DEBUG_LOG_KEY_LN(key);
		lockHandle = std::make_shared<LockHandle>(owner, env);
		this->locks.emplace(key, lockHandle);
		if (isNewLock != nullptr) {
			*isNewLock = true;
		}
		if (skipEnqueueIfNewLock) {
			DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback skipping enqueue because lock already exists\n", this);
			return;
		}
	} else {
		DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback lock found for key %s\n", this, key.c_str());
		lockHandle = lockHandleIterator->second;
	}

	// lock found
	napi_valuetype type;
	NAPI_STATUS_THROWS_VOID(::napi_typeof(env, callback, &type));
	if (type == napi_function) {
		napi_value resourceName;
		NAPI_STATUS_THROWS_VOID(::napi_create_string_latin1(
			env,
			"rocksdb-js.lock",
			NAPI_AUTO_LENGTH,
			&resourceName
		));

		napi_threadsafe_function threadsafeCallback;
		NAPI_STATUS_THROWS_VOID(::napi_create_threadsafe_function(
			env,                // env
			callback,           // func
			nullptr,            // async_resource
			resourceName,       // async_resource_name
			0,                  // max_queue_size
			1,                  // initial_thread_count
			nullptr,            // thread_finalize_data
			nullptr,            // thread_finalize_callback
			nullptr,            // context
			callJsCallback,     // call_js_cb
			&threadsafeCallback // [out] callback
		));

		DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback enqueuing callback %p\n", this, threadsafeCallback);
		NAPI_STATUS_THROWS_VOID(::napi_unref_threadsafe_function(env, threadsafeCallback));

		// Create LockCallback and add to queue
		lockHandle->threadsafeCallbacks.push(LockCallback(threadsafeCallback, deferred));
	}
}

/**
 * Checks if a lock exists for the given key. Called by `db.hasLock()`.
 */
bool DBDescriptor::lockExistsByKey(std::string& key) {
	std::lock_guard<std::mutex> lock(this->locksMutex);
	auto lockHandle = this->locks.find(key);
	bool exists = lockHandle != this->locks.end();
	DEBUG_LOG("%p DBDescriptor::hasLock %s lock for key \"%s\"\n", this, exists ? "found" : "not found", key.c_str());
	return exists;
}

/**
 * Releases a lock by key. Called by `db.unlock()`.
 */
bool DBDescriptor::lockReleaseByKey(std::string& key) {
	std::queue<LockCallback> threadsafeCallbacks;

	{
		std::lock_guard<std::mutex> lock(this->locksMutex);
		auto lockHandle = this->locks.find(key);

		if (lockHandle == this->locks.end()) {
			// no lock found
			DEBUG_LOG("%p DBDescriptor::lockReleaseByKey no lock found\n", this);
			return false;
		}

		// lock found, remove it
		threadsafeCallbacks = std::move(lockHandle->second->threadsafeCallbacks);
		DEBUG_LOG("%p DBDescriptor::lockReleaseByKey removing lock\n", this);
		this->locks.erase(key);
	}

	DEBUG_LOG("%p DBDescriptor::lockReleaseByKey calling %zu unlock callbacks\n", this, threadsafeCallbacks.size());

	// call the callbacks in order, but stop if any callback fails
	while (!threadsafeCallbacks.empty()) {
		auto lockCallback = threadsafeCallbacks.front();
		threadsafeCallbacks.pop();
		DEBUG_LOG("%p DBDescriptor::lockReleaseByKey calling callback %p\n", this, lockCallback.callback);
		napi_status status = ::napi_call_threadsafe_function(lockCallback.callback, nullptr, napi_tsfn_blocking);
		if (status == napi_closing) {
			continue;
		}
		::napi_release_threadsafe_function(lockCallback.callback, napi_tsfn_release);
	}

	return true;
}

/**
 * Releases all locks owned by the given handle. Called by `db.close()`.
 */
void DBDescriptor::lockReleaseByOwner(DBHandle* owner) {
	std::set<napi_threadsafe_function> threadsafeCallbacks;

	{
		std::lock_guard<std::mutex> lock(this->locksMutex);
			DEBUG_LOG("%p DBDescriptor::lockReleaseByOwner checking %zu locks if they are owned handle %p\n", this, this->locks.size(), owner);
		for (auto it = this->locks.begin(); it != this->locks.end();) {
			auto lockOwner = it->second->owner.lock();
			if (!lockOwner || lockOwner.get() == owner) {
				DEBUG_LOG("%p DBDescriptor::lockReleaseByOwner found lock %p with %zu callbacks\n", this, it->second.get(), it->second->threadsafeCallbacks.size());
				// move all callbacks from the queue
				while (!it->second->threadsafeCallbacks.empty()) {
					threadsafeCallbacks.insert(it->second->threadsafeCallbacks.front().callback);
					it->second->threadsafeCallbacks.pop();
				}
				it = this->locks.erase(it);
			} else {
				++it;
			}
		}
	}

	DEBUG_LOG("%p DBDescriptor::lockReleaseByOwner calling %zu unlock callbacks\n", this, threadsafeCallbacks.size());

	// call the callbacks in order, but stop if any callback fails
	for (auto& callback : threadsafeCallbacks) {
		DEBUG_LOG("%p DBDescriptor::lockReleaseByOwner calling callback %p\n", this, callback);
		napi_status status = ::napi_call_threadsafe_function(callback, nullptr, napi_tsfn_blocking);
		if (status == napi_closing) {
			continue;
		}
		::napi_release_threadsafe_function(callback, napi_tsfn_release);
	}
}

/**
 * Rejects the one `paths` change that turns a healthy database into an
 * unopenable one.
 *
 * With an empty `db_paths` RocksDB sanitizes it to `[{dbname, ...}]`, so every
 * SST file an untiered database ever wrote carries path index 0 meaning THE
 * DATABASE DIRECTORY. Supplying `paths` for the first time redefines index 0 as
 * `paths[0]`, RocksDB looks for those files on the wrong volume, and the open
 * fails with `Corruption: ... MANIFEST-NNNNNN may be corrupted` — which sends an
 * operator to backup restore rather than to the one config line they changed.
 * That zero-to-one transition is the migration people actually attempt, and the
 * supported form of it is to list the database directory itself as `paths[0]`.
 *
 * Decided by file existence, not by comparing directory strings: if the database
 * directory holds SST files and they are not reachable under `paths[0]`, this
 * open would break. An already-tiered database has no SST files in its own
 * directory, and one that lists its own directory as `paths[0]` finds them
 * there, so both pass — including when the two spellings differ (trailing
 * separator, symlink). Identical spellings are the exception, and only as a
 * shortcut: they cannot fail, and the supported form of the migration produces
 * them on a database whose own directory is where all the SST files are.
 *
 * This does NOT catch reordering or removing an entry of an existing list; the
 * files are elsewhere and nothing on disk records which index they came from.
 * That stays a documented append-only rule (see `DBOptions::paths`).
 */
static void assertStoragePathsUsable(
	rocksdb::Env* env,
	const std::string& path,
	const std::vector<StoragePath>& paths
) {
	if (paths.empty() || paths[0].path == path) {
		// The supported form of the migration, and then every file listed below
		// would be stat'ed at the directory it was just listed from — one syscall
		// per SST file of an arbitrarily large database, on every open.
		return;
	}

	std::vector<std::string> children;
	// Listing through rocksdb::Env rather than std::filesystem: Env does its own
	// UTF-8 conversion on Windows, where std::filesystem::path would re-encode
	// through the active code page.
	if (!env->GetChildren(path, &children).ok()) {
		return;
	}

	for (const auto& child : children) {
		if (child.size() < 4 || child.compare(child.size() - 4, 4, ".sst") != 0) {
			continue;
		}
		// Every one of them, not just the first: a colliding file number under a
		// shared paths[0], or a half-finished manual copy, would otherwise let one
		// reachable file vouch for a set that is not.
		if (env->FileExists(paths[0].path + "/" + child).ok()) {
			continue;
		}
		throw rocksdb_js::DBException(
			"Cannot open \"" + path + "\" with paths[0] \"" + paths[0].path +
			"\": the database already has SST files in its own directory, which RocksDB records "
			"as path index 0. Adding paths now redefines index 0 as \"" + paths[0].path +
			"\", so those files would be looked for on the wrong volume. Add the database "
			"directory itself as the first entry instead: paths: [{ path: \"" + path +
			"\", targetSize: ... }, { path: \"" + paths[0].path + "\", ... }]."
		);
	}
}

/**
 * Names `paths` as a suspect on the open failure a `paths` change produces.
 *
 * `assertStoragePathsUsable` above catches the zero-to-one transition up front
 * because it is decidable from the files. Removing an entry, or reordering one,
 * is not: the MANIFEST records a path INDEX, the list it indexes into is
 * supplied at open and stored nowhere, and the files are exactly where they were
 * left. RocksDB looks on the wrong volume and reports
 * `Corruption: ... MANIFEST-NNNNNN may be corrupted`, which reads as data loss —
 * so an operator goes to backup restore instead of to the config line they
 * changed. It is recoverable by putting the list back, and this is what says so.
 *
 * The note is appended rather than substituted, and phrased conditionally,
 * because a genuinely corrupt database produces the same status: this cannot
 * tell them apart, only make sure the cheap explanation is not missed.
 */
static std::string explainOpenFailure(const rocksdb::Status& status) {
	std::string message = status.ToString();
	if (!status.IsCorruption() || message.find(".sst") == std::string::npos) {
		return message;
	}
	return message +
		"\n\nIf this database has ever been opened with `paths`, that same list must be "
		"supplied again: RocksDB stores each SST file's location as an index into the list "
		"given at open and does not record the list itself, so removing or reordering an "
		"entry points it at the wrong volume for files that are still where they were left. "
		"Entries may only be appended. Otherwise the files named above are genuinely missing.";
}

#ifndef ROCKSDB_HAS_CF_BLOB_DIR
/**
 * Refuses to open a database whose blob files this build cannot find.
 *
 * `blob_dir` does not exist as a field here, and `loadPersistedCFOptions` is
 * deliberately permissive about unknown options, so a database written by a
 * patched build with its blob files on another volume would otherwise open
 * cleanly and look for them beside the SST files — every value at or above
 * `min_blob_size` failing with "No such file or directory" on read, with nothing
 * at open to say why. The parsed options cannot show it, so the OPTIONS file is
 * scanned as text.
 */
static void assertNoPersistedBlobDir(rocksdb::Env* env, const std::string& path) {
	std::string optionsFileName;
	if (!rocksdb::GetLatestOptionsFileName(path, env, &optionsFileName).ok()) {
		return;
	}

	std::string contents;
	if (!rocksdb::ReadFileToString(env, path + "/" + optionsFileName, &contents).ok()) {
		return;
	}

	// The scan itself is Node-free and lives in core/ so a GoogleTest can cover
	// it: this whole function is compiled only into an UNPATCHED build, where no
	// test can produce a database carrying a blob_dir to trip it.
	if (auto blobDir = rocksdb_js::findPersistedBlobDir(contents)) {
		throw rocksdb_js::DBException(
			"Cannot open \"" + path + "\": its blob files were written to \"" + *blobDir +
			"\", but this build of rocksdb-js is linked against a RocksDB without the blob_dir "
			"patch and would look for them beside the SST files. Use a build with the patch "
			"(ROCKSDB_HAS_CF_BLOB_DIR)."
		);
	}
}
#endif

/**
 * Creates a new DBDescriptor.
 */
std::shared_ptr<DBDescriptor> DBDescriptor::open(const std::string& path, const DBOptions& options) {
	std::string name = options.name.empty() ? "default" : options.name;
	DEBUG_LOG("DBDescriptor::open Opening \"%s\" (column family: \"%s\", read-only: %s)\n", path.c_str(), name.c_str(), options.readOnly ? "true" : "false");

	DBSettings& settings = DBSettings::getInstance();

	// set the database options
	rocksdb::Options dbOptions;
	// we could also consider some testing around using atomic_flush
	dbOptions.atomic_flush = true; // this is necessary in order to ensure that we can track full flush jobs back to the corresponding sequence numbers
	dbOptions.comparator = rocksdb::BytewiseComparator();
	dbOptions.create_if_missing = !options.readOnly;
	dbOptions.create_missing_column_families = !options.readOnly;
	dbOptions.db_write_buffer_size = options.dbWriteBufferSize;
	// Attach the process-wide WriteBufferManager (if configured) so memtable
	// memory is bounded across all DBs in this process. With cost_to_cache,
	// active memtables share the block cache pool — the cache shrinks during
	// write bursts and reclaims room as memtables flush.
	if (auto wbm = settings.getWriteBufferManager()) {
		dbOptions.write_buffer_manager = wbm;
	}
	dbOptions.IncreaseParallelism(options.parallelismThreads);
	// Bound how many table files RocksDB holds open: with the RocksDB default
	// (-1, every SST open forever) compaction lag under sustained ingest can
	// run the process out of fds (HarperFast/harper#1785 environment).
	dbOptions.max_open_files = options.maxOpenFiles == 0
		? deriveMaxOpenFiles(getEffectiveOpenFileLimit())
		: options.maxOpenFiles;
	dbOptions.keep_log_file_num = 5; // these are informational log files that clutter up the database directory

	// Spread SST files across volumes. Left on `db_paths` rather than `cf_paths`
	// so one tiering policy covers every column family in the database (RocksDB
	// falls back to db_paths when cf_paths is empty). A file's path index is
	// recorded in the MANIFEST, so entries may only ever be appended — see the
	// note on DBOptions::paths.
	for (const auto& storagePath : options.paths) {
		dbOptions.db_paths.emplace_back(storagePath.path, storagePath.targetSize);
	}
	// Explicit narrowing: RocksDB's field is size_t; the value is validated
	// <= MAX_SAFE_INTEGER at parse time, and a >4GB info-log cap (only reachable
	// on a 32-bit build) is nonsensical, so the cast is safe and silences
	// -Wshorten-64-to-32. Bounds each retained log file's size (see db_options.h).
	dbOptions.max_log_file_size = static_cast<size_t>(options.maxLogFileSize);
	if (options.infoLogLevel.has_value()) {
		dbOptions.info_log_level = static_cast<rocksdb::InfoLogLevel>(*options.infoLogLevel);
	}
	dbOptions.persist_user_defined_timestamps = true;
	if (options.enableStats) {
		dbOptions.statistics = rocksdb::CreateDBStatistics();
		dbOptions.statistics->set_stats_level(static_cast<rocksdb::StatsLevel>(options.statsLevel));
	} else {
		dbOptions.statistics = nullptr;
	}

	// Base options shared by every column family. Compression is applied per CF
	// below so opening one family cannot restamp another's algorithm.
	auto cfOptions = buildColumnFamilyOptions(options);

	// Shared listener state, created BEFORE DB::Open so a background error fired
	// during open has a valid, race-free target (the descriptor does not exist
	// yet). The descriptor is published into it once constructed, transferring any
	// error captured mid-open (HarperFast/rocksdb-js#754).
	auto listenerState = std::make_shared<DBEventListenerState>();
	auto eventListener = std::make_shared<TransactionLogEventListener>(listenerState);
	dbOptions.listeners.push_back(eventListener);

	// prepare the column family stuff - first check if database exists
	std::vector<rocksdb::ColumnFamilyDescriptor> cfDescriptors;
	std::vector<std::string> columnFamilyNames;

	// try to list existing column families
	DEBUG_LOG("DBDescriptor::open Listing column families for \"%s\"\n", path.c_str());
	rocksdb::Status listStatus = rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(), path, &columnFamilyNames);
#ifdef ROCKSDB_HAS_CF_BLOB_DIR
	struct AcceptedBlobRelocation {
		std::string cfName;
		std::string from;
		std::string to;
	};
	std::vector<AcceptedBlobRelocation> acceptedBlobRelocations;
	bool createRequestedBlobDirBeforeOpen = false;
	bool createRequestedBlobDirForNewColumn = false;
#endif
	if (listStatus.ok() && !columnFamilyNames.empty()) {
		assertStoragePathsUsable(dbOptions.env, path, options.paths);
#ifndef ROCKSDB_HAS_CF_BLOB_DIR
		assertNoPersistedBlobDir(dbOptions.env, path);
#endif

		// Database exists. Compression is per-CF: opening one column family must
		// not change another's algorithm, and RocksDB requires opening every CF
		// at once with the options we supply (it does not restore persisted
		// per-CF options on its own). So preserve each CF's persisted compression,
		// and apply the caller's request ONLY to the target CF (options.name), and
		// only when it was explicitly requested — the LZ4 default must never
		// override an existing CF's stored algorithm (a plain reopen inherits it).
		//
		// `compressionForAllColumnFamilies` opts out of that per-CF preservation:
		// the explicit request is applied to every family instead, which is how a
		// caller expresses "this database uses one codec" for families it never
		// names (see db_options.h).
		//
		// The blob settings (`blobs.*`) are per-CF for exactly the same reason and
		// get exactly the same treatment, with no all-families opt-out: each
		// family's persisted values are restored and only the fields the caller
		// explicitly supplied are applied, to the target family alone.
		std::unordered_map<std::string, PersistedCFOptions> persisted;
		rocksdb::Status persistedStatus = loadPersistedCFOptions(path, persisted);
		if (!persistedStatus.ok()) {
			// The DB exists (we just listed its column families), so a missing or
			// unparseable OPTIONS file is NOT the fresh-DB case — we cannot recover
			// each CF's persisted compression, and opening them with the base
			// defaults would silently restamp the non-target CFs. Fail loudly.
			throw rocksdb_js::DBException(
				"Failed to load persisted column family options for \"" + path +
				"\": " + persistedStatus.ToString()
			);
		}
#ifdef ROCKSDB_HAS_CF_BLOB_DIR
		auto scanBlobDir = rocksdb_js::makeBlobDirScanner(dbOptions.env);
		// Where the open's target family kept its blob files before this open.
		// Disengaged when the target is not on disk yet, in which case nothing
		// moved out from under it and no other family can be said to have moved
		// with it.
		std::optional<std::string> targetPersistedBlobDir;
		if (auto targetIt = persisted.find(name); targetIt != persisted.end()) {
			targetPersistedBlobDir = targetIt->second.blobDir;
		}
		auto blobDirExists = [&dbOptions](const std::string& dir) {
			return dbOptions.env->FileExists(dir).ok();
		};
#endif
		for (const auto& cfName : columnFamilyNames) {
			DEBUG_LOG("DBDescriptor::open Opening column family \"%s\"\n", cfName.c_str());
			rocksdb::ColumnFamilyOptions cfo = cfOptions;
			auto it = persisted.find(cfName);
			if (it != persisted.end()) {
				cfo.compression = it->second.compression;
				cfo.blob_compression_type = it->second.blobCompression;
				cfo.compression_opts = it->second.compressionOpts;
				restorePersistedBlobOptions(cfo, it->second);
			}
			const bool isTarget = cfName == name;
			if ((isTarget || options.compressionForAllColumnFamilies) && options.compression &&
				options.compressionExplicit) {
				applyCompression(cfo, *options.compression, options.compressionLevel);
			}
			if (isTarget) {
				applyExplicitBlobOptions(cfo, options.blobs);
			}
#ifdef ROCKSDB_HAS_CF_BLOB_DIR
			rocksdb_js::BlobRelocationInput relocation;
			relocation.dbPath = path;
			relocation.defaultBlobDir =
				dbOptions.db_paths.empty() ? path : dbOptions.db_paths.front().path;
			relocation.cfName = cfName;
			relocation.isTarget = isTarget;
			if (it != persisted.end()) {
				relocation.persistedBlobDir = it->second.blobDir;
			}
			relocation.targetPersistedBlobDir = targetPersistedBlobDir;
			relocation.requestedDir = options.blobs.dir;
			relocation.allowDirChange = options.blobs.allowDirChange;
			relocation.currentBlobDir = cfo.blob_dir;

			rocksdb_js::BlobRelocationDecision decision =
				rocksdb_js::decideBlobRelocation(relocation, scanBlobDir, blobDirExists);
			if (!decision.error.empty()) {
				throw rocksdb_js::DBException(decision.error);
			}
			createRequestedBlobDirBeforeOpen =
				createRequestedBlobDirBeforeOpen || decision.mayCreateDestination;
			if (relocation.allowDirChange && relocation.persistedBlobDir) {
				const std::string& defaultBlobDir =
					relocation.defaultBlobDir.empty() ? relocation.dbPath : relocation.defaultBlobDir;
				const std::string& from =
					relocation.persistedBlobDir->empty() ? defaultBlobDir : *relocation.persistedBlobDir;
				const std::string& to = decision.blobDir.empty() ? defaultBlobDir : decision.blobDir;
				if (from != to) {
					acceptedBlobRelocations.push_back({ cfName, from, to });
				}
			}
			cfo.blob_dir = decision.blobDir;
#endif
			cfDescriptors.emplace_back(cfName, cfo);
		}
#ifdef ROCKSDB_HAS_CF_BLOB_DIR
		if (options.readOnly &&
			(createRequestedBlobDirBeforeOpen || !acceptedBlobRelocations.empty())
		) {
			throw rocksdb_js::DBException(
				"Creating or relocating blobs.dir requires a writable open so the directory is persisted"
			);
		}
		if (!options.blobs.dir.empty()) {
			createRequestedBlobDirForNewColumn = !options.readOnly &&
				std::find(columnFamilyNames.begin(), columnFamilyNames.end(), name) ==
				columnFamilyNames.end();
		}
#endif
	} else {
		// Database doesn't exist or no column families found. Create the default
		// column family; apply the requested compression to it only when it is the
		// target (a freshly-created CF gets the request — default or explicit).
		DEBUG_LOG("DBDescriptor::open Database doesn't exist or no column families found, using default\n");
		rocksdb::ColumnFamilyOptions cfo = cfOptions;
		if (name == rocksdb::kDefaultColumnFamilyName) {
			if (options.compression) {
				applyCompression(cfo, *options.compression, options.compressionLevel);
			}
		} else {
			// `default` is created on the way to the named family the caller asked
			// for, so it must not inherit that family's blob settings — a database
			// created by opening table t1 with a blobs.dir would otherwise persist
			// that directory on `default` too, and a later plain
			// `RocksDatabase.open(path)` could not open it at all.
			applyBlobCreationDefaults(cfo);
		}
		cfDescriptors = { rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, cfo) };
#ifdef ROCKSDB_HAS_CF_BLOB_DIR
		if (!options.blobs.dir.empty() && !options.readOnly) {
			createRequestedBlobDirBeforeOpen = name == rocksdb::kDefaultColumnFamilyName;
			createRequestedBlobDirForNewColumn = !createRequestedBlobDirBeforeOpen;
		}
#endif
	}

#ifdef ROCKSDB_HAS_CF_BLOB_DIR
	if (createRequestedBlobDirBeforeOpen) {
		ensureBlobDirExists(dbOptions.env, options.blobs.dir);
	}
#endif

	std::vector<rocksdb::ColumnFamilyHandle*> cfHandles;
	std::shared_ptr<rocksdb::DB> db;
	std::unordered_map<std::string, std::shared_ptr<ColumnFamilyDescriptor>> columns;

	if (options.readOnly) {
		std::unique_ptr<rocksdb::DB> rdb;
		DEBUG_LOG("DBDescriptor::open Opening readonly db for \"%s\"\n", path.c_str());
		rocksdb::Status status = rocksdb::DB::OpenForReadOnly(dbOptions, path, cfDescriptors, &cfHandles, &rdb);
		if (!status.ok()) {
			DEBUG_LOG("DBDescriptor::open Failed to open readonly db for \"%s\": %s\n", path.c_str(), status.ToString().c_str());
			if (status.IsIOError()) {
				DEBUG_LOG("DBDescriptor::open IOError: %s\n", status.ToString().c_str());
				throw rocksdb_js::DBException("Database does not exist");
			}
			throw rocksdb_js::DBException(explainOpenFailure(status));
		}
		DEBUG_LOG("DBDescriptor::open Opened readonly db for \"%s\"\n", path.c_str());
		db = std::shared_ptr<rocksdb::DB>(rdb.release(), DBDeleter{});
	} else if (options.mode == DBMode::Pessimistic) {
		rocksdb::TransactionDBOptions txndbOptions;
		txndbOptions.default_lock_timeout = 10000;
		txndbOptions.transaction_lock_timeout = 10000;

		rocksdb::TransactionDB* rdb;
		DEBUG_LOG("DBDescriptor::open Opening pessimistic transaction db for \"%s\"\n", path.c_str());
		rocksdb::Status status = rocksdb::TransactionDB::Open(dbOptions, txndbOptions, path, cfDescriptors, &cfHandles, &rdb);
		if (!status.ok()) {
			DEBUG_LOG("DBDescriptor::open Failed to open pessimistic transaction db for \"%s\": %s\n", path.c_str(), status.ToString().c_str());
			throw rocksdb_js::DBException(explainOpenFailure(status));
		}
		DEBUG_LOG("DBDescriptor::open Opened pessimistic transaction db for \"%s\"\n", path.c_str());
		db = std::shared_ptr<rocksdb::DB>(rdb, DBDeleter{});
	} else {
		rocksdb::OptimisticTransactionDB* rdb;
		DEBUG_LOG("DBDescriptor::open Opening optimistic transaction db for \"%s\"\n", path.c_str());
		rocksdb::Status status = rocksdb::OptimisticTransactionDB::Open(dbOptions, path, cfDescriptors, &cfHandles, &rdb);
		if (!status.ok()) {
			DEBUG_LOG("DBDescriptor::open Failed to open optimistic transaction db for \"%s\": %s\n", path.c_str(), status.ToString().c_str());
			throw rocksdb_js::DBException(explainOpenFailure(status));
		}
		DEBUG_LOG("DBDescriptor::open Opened optimistic transaction db for \"%s\"\n", path.c_str());
		db = std::shared_ptr<rocksdb::DB>(rdb, DBDeleter{});
	}

#ifdef ROCKSDB_HAS_CF_BLOB_DIR
	if (!acceptedBlobRelocations.empty()) {
		auto infoLog = db->GetDBOptions().info_log;
		for (const auto& relocation : acceptedBlobRelocations) {
			if (infoLog) {
				rocksdb::Log(
					rocksdb::InfoLogLevel::INFO_LEVEL,
					infoLog,
					"rocksdb-js accepted blob-directory relocation for column family %s: %s -> %s",
					relocation.cfName.c_str(),
					relocation.from.c_str(),
					relocation.to.c_str()
				);
			} else {
				DEBUG_LOG(
					"Accepted blob-directory relocation for column family %s: %s -> %s (info LOG unavailable)\n",
					relocation.cfName.c_str(),
					relocation.from.c_str(),
					relocation.to.c_str()
				);
			}
		}
	}
#endif

	// figure out if desired column family exists and if not create it
	bool columnExists = false;
	for (size_t n = 0; n < cfHandles.size(); ++n) {
		auto column = std::shared_ptr<rocksdb::ColumnFamilyHandle>(cfHandles[n]);
		auto columnDescriptor = std::make_shared<ColumnFamilyDescriptor>(column);
		columns[cfDescriptors[n].name] = columnDescriptor;
		if (cfDescriptors[n].name == options.name) {
			columnExists = true;
		}
	}
	// Where every column family's blob files went, for `destroy()` — captured
	// from the options each family was actually opened/created with rather than
	// read back off the live DB later, which races a concurrent close.
	std::unordered_map<std::string, std::string> layoutBlobDirs;
#ifdef ROCKSDB_HAS_CF_BLOB_DIR
	for (const auto& cfDescriptor : cfDescriptors) {
		layoutBlobDirs[cfDescriptor.name] = cfDescriptor.options.blob_dir;
	}
#endif

	if (!columnExists) {
		auto cfo = cfOptions;
		if (options.compression) {
			applyCompression(cfo, *options.compression, options.compressionLevel);
		}
#ifdef ROCKSDB_HAS_CF_BLOB_DIR
		if (createRequestedBlobDirForNewColumn) {
			ensureBlobDirExists(db->GetEnv(), options.blobs.dir);
		}
#endif
		auto column = rocksdb_js::createRocksDBColumnFamily(db, options.name, cfo);
		auto columnDescriptor = std::make_shared<ColumnFamilyDescriptor>(column);
		columns[options.name] = columnDescriptor;
#ifdef ROCKSDB_HAS_CF_BLOB_DIR
		layoutBlobDirs[options.name] = cfo.blob_dir;
#endif
	}

	DEBUG_LOG("DBDescriptor::open Creating DBDescriptor for \"%s\"\n", path.c_str());
	auto descriptor = std::shared_ptr<DBDescriptor>(new DBDescriptor(path, options, cfOptions, db, std::move(columns), dbOptions.statistics));
	descriptor->layoutDbPaths = dbOptions.db_paths;
	descriptor->layoutBlobDirs = std::move(layoutBlobDirs);
	DBRegistry::RecordLayout(path, descriptor->captureLayout());

	// Publish the descriptor into the shared listener state (guarded), so flush
	// callbacks can reach it and any background error captured during open is
	// transferred to it now.
	listenerState->publishDescriptor(descriptor);

	// Register with the transaction log store registry
	TransactionLogStoreConfig logConfig;
	logConfig.transactionLogsPath = options.transactionLogsPath;
	logConfig.transactionLogMaxAgeThreshold = options.transactionLogMaxAgeThreshold;
	logConfig.transactionLogMaxSize = options.transactionLogMaxSize;
	logConfig.transactionLogRetentionMs = std::chrono::milliseconds(options.transactionLogRetentionMs);
	TransactionLogStoreRegistry::Register(path, logConfig);
	TransactionLogStoreRegistry::DiscoverStores(path);

	return descriptor;
}

/**
 * Adds a transaction to the registry.
 */
void DBDescriptor::transactionAdd(std::shared_ptr<TransactionHandle> txnHandle) {
	auto id = txnHandle->id;
	std::lock_guard<std::mutex> lock(this->txnsMutex);
	this->transactions.emplace(id, txnHandle);
	this->closables[txnHandle.get()] = std::weak_ptr<Closable>(txnHandle);
}

/**
 * Retrieves a transaction from the registry.
 */
std::shared_ptr<TransactionHandle> DBDescriptor::transactionGet(uint32_t id) {
	std::lock_guard<std::mutex> lock(this->txnsMutex);
	auto it = this->transactions.find(id);
	if (it != this->transactions.end()) {
		auto txnHandle = it->second;
		if (txnHandle && txnHandle->txn) {
			return txnHandle;
		}
	}
	return nullptr;
}

/**
 * Removes a transaction from the registry.
 */
void DBDescriptor::transactionRemove(std::shared_ptr<TransactionHandle> txnHandle) {
	std::lock_guard<std::mutex> lock(this->txnsMutex);
	this->closables.erase(txnHandle.get());

	auto it = this->transactions.find(txnHandle->id);
	if (it != this->transactions.end()) {
		if (it->second != txnHandle) {
			DEBUG_LOG("%p DBDescriptor::transactionRemove txnId %u mismatch! expected %p, got %p\n", this, txnHandle->id, it->second.get(), txnHandle.get());
		}
		this->transactions.erase(it);
	}
}

/**
 * Closes every registered transaction owned by a handle created on `env`.
 * See the header for why this is env-scoped rather than part of
 * DBHandle::close().
 */
void DBDescriptor::closeTransactionsByEnv(napi_env env) {
	// Collect matches under the mutex, close outside it: close() calls
	// transactionRemove(), which re-takes txnsMutex, and may block in
	// waitForAsyncWorkCompletion() draining an execute still running on the
	// commit thread.
	std::vector<std::shared_ptr<TransactionHandle>> toClose;
	{
		std::lock_guard<std::mutex> lock(this->txnsMutex);
		for (auto& [id, txnHandle] : this->transactions) {
			if (txnHandle && txnHandle->dbHandle && txnHandle->dbHandle->env == env) {
				toClose.push_back(txnHandle);
			}
		}
	}

	for (auto& txnHandle : toClose) {
		DEBUG_LOG("%p DBDescriptor::closeTransactionsByEnv closing transaction %u (env=%p)\n", this, txnHandle->id, env);
		txnHandle->close();
		// close() can only self-remove while it can still reach this descriptor
		// through its DBHandle, and a handle closed earlier by the user has
		// already reset that pointer — so for exactly the case this reap exists
		// to catch, the registry entry (a strong ref to the handle, and through
		// it the DBHandle) would otherwise outlive the env for the life of the
		// process. Removing here is idempotent when close() already did it.
		this->transactionRemove(txnHandle);
	}
}

/**
 * Generates the next unique transaction ID for this database.
 */
uint32_t DBDescriptor::transactionGetNextId() {
	return ++this->nextTransactionId;
}

/**
 * Removes a dropped column family from the columns map so a later open-by-name
 * creates a fresh column family instead of reusing the dangling dropped
 * handle. DBHandles still holding the descriptor keep it alive via their
 * shared_ptr and can continue reading until they close; only the by-name
 * lookup is removed. The family is removed from the destroy layout first so a
 * concurrent open cannot register a fresh same-name family before that cleanup.
 */
void DBDescriptor::unregisterColumnFamily(const std::string& columnName) {
	DBRegistry::RemoveColumnFamilyLayout(this->path, columnName);
	std::lock_guard<std::mutex> lock(this->columnsMutex);
	// Retire debounce state so the map stays bounded and a recreated CF of the
	// same name starts fresh rather than inheriting a stale reported-stalled bit.
	this->writeStallDebounce.forget(columnName);
	if (this->columns.erase(columnName)) {
		DEBUG_LOG("%p DBDescriptor::unregisterColumnFamily unregistered column \"%s\"\n",
			this, columnName.c_str());
	} else {
		DEBUG_LOG("%p DBDescriptor::unregisterColumnFamily column \"%s\" not found\n",
			this, columnName.c_str());
	}
}

/**
 * Called when a lock callback completes (async or sync) to clean up the lock
 * handle and fire the next callback in the queue.
 */
void DBDescriptor::onCallbackComplete(const std::string& key) {
	// try to mark the current callback as complete and fire the next one
	// use a try-catch to handle the case where mutexes might be invalid
	try {
		std::lock_guard<std::mutex> lock(this->locksMutex);
		auto lockHandle = this->locks.find(key);
		if (lockHandle != this->locks.end()) {
			lockHandle->second->isRunning.store(false);
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete marking as complete (key=\"%s\")\n", this, key.c_str());
		} else {
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete lock already removed (key=\"%s\")\n", this, key.c_str());
			return; // lock was already cleaned up, nothing to do
		}
	} catch (const std::exception& e) {
		// the Visual C++ compiler complains that `e` is unused in release
		// builds, so we need to use the `maybe_unused` attribute and this will
		// be optimized out in release builds
		[[maybe_unused]] auto msg = e.what();
		DEBUG_LOG("%p DBDescriptor::onCallbackComplete failed to acquire lock (key=\"%s\"): %s\n", this, key.c_str(), msg);
		return; // mutex is invalid, descriptor is likely being destroyed
	}

	// fire the next callback in the queue
	DEBUG_LOG("%p DBDescriptor::onCallbackComplete firing next callback (key=\"%s\")\n", this, key.c_str());
	try {
		std::unique_lock<std::mutex> lock(this->locksMutex);
		auto lockHandle = this->locks.find(key);

		if (lockHandle == this->locks.end()) {
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete no lock found (key=\"%s\")\n", this, key.c_str());
			return;
		}

		auto& handle = lockHandle->second;

		// try to acquire the "lock" atomically
		bool expected = false;
		if (!handle->isRunning.compare_exchange_strong(expected, true)) {
			// another callback is already running
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete another callback is already running (key=\"%s\")\n", this, key.c_str());
			return;
		}

		// we now "own" the execution for this key
		if (handle->threadsafeCallbacks.empty()) {
			handle->isRunning.store(false);
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete no callbacks left (key=\"%s\"), removing lock\n", this, key.c_str());
			// remove the empty lock handle from the map
			this->locks.erase(key);
			return;
		}

		LockCallback lockCallback = handle->threadsafeCallbacks.front();
		handle->threadsafeCallbacks.pop();
		auto callback = lockCallback.callback;

		// release the mutex before calling the callback to avoid holding locks during callback execution
		lock.unlock();

		DEBUG_LOG("%p DBDescriptor::onCallbackComplete calling callback %p (key=\"%s\")\n", this, callback, key.c_str());

		// create callback data that includes the key for completion and deferred promise
		auto* callbackData = new LockCallbackCompletionData(key, weak_from_this(), lockCallback.deferred);

		// use threadsafe function instead of direct call
		napi_status status = ::napi_call_threadsafe_function(callback, callbackData, napi_tsfn_blocking);
		if (status != napi_ok && status != napi_closing) {
			DEBUG_LOG("%p DBDescriptor::onCallbackComplete failed to call threadsafe function (key=\"%s\")\n", this, key.c_str());
			delete callbackData;
			this->onCallbackComplete(key);
		}

		// release the threadsafe function
		::napi_release_threadsafe_function(callback, napi_tsfn_release);
	} catch (const std::exception& e) {
		// the Visual C++ compiler complains that `e` is unused in release
		// builds, so we need to use the `maybe_unused` attribute and this will
		// be optimized out in release builds
		[[maybe_unused]] auto msg = e.what();
		DEBUG_LOG("%p DBDescriptor::onCallbackComplete failed to fire next callback (key=\"%s\"): %s\n", this, key.c_str(), msg);
	}
}

/**
 * `callJsCallback()` helper macros.
 */
#ifdef DEBUG
	#define CALL_JS_CB_DEBUG_LOG(msg, ...) \
		do { \
			std::string errorStr = rocksdb_js::getNapiExtendedError(env, status); \
			rocksdb_js::debugLog(true, "callJsCallback() " msg ": %s (key=\"%s\")", ##__VA_ARGS__, errorStr.c_str(), callbackData->key.c_str()); \
		} while (0)
#else
	#define CALL_JS_CB_DEBUG_LOG(msg, ...) \
		do { \
			; \
		} while (0)
#endif

#define CALL_JS_CB_NAPI_STATUS_CHECK(call, code, msg, ...) \
	do { \
		napi_status status = (call); \
		if (status != napi_ok) { \
			CALL_JS_CB_DEBUG_LOG(msg, ##__VA_ARGS__); \
			code; \
			return; \
		} \
	} while (0)

/**
 * Custom wrapper used by `napi_call_threadsafe_function()` to call user-
 * defined lock callback function. If the lock callback returns a Promise, it
 * is awaited before calling the `onCallbackComplete()` handler.
 *
 * For example, the callback passed into `db.tryLock()` or `db.withLock()` is
 * what is passed in as `jsCallback`. The code then invokes `jsCallback` and
 * checks if it returned a promise. If it did, it calls `then()` on the promise
 * with resolve and reject callbacks that call `onCallbackComplete()`.
 *
 * This mechanism is key to ensuring that only a single async lock callback
 * is running at a time.
 *
 * Note: Node.js runs this function which ever thread (main or worker) that
 * created the threadsafe function.
 */
static void callJsCallback(napi_env env, napi_value jsCallback, void* context, void* data) {
	if (env == nullptr || jsCallback == nullptr) {
		return;
	}

	// get the callback data from the function's data
	LockCallbackCompletionData* callbackData = static_cast<LockCallbackCompletionData*>(data);
	if (callbackData == nullptr) {
		DEBUG_LOG("callJsCallback callbackData is nullptr - calling js callback\n");
		// this is a tryLock callback - call it without completion callback
		napi_value global;
		napi_status status = ::napi_get_global(env, &global);
		if (status == napi_ok) {
			napi_value result;
			::napi_call_function(env, global, jsCallback, 0, nullptr, &result);
		}
		return;
	}

	// create shared_ptr from raw pointer for RAII management
	std::shared_ptr<LockCallbackCompletionData> callbackDataPtr(callbackData);

	// create a completion callback function
	napi_value completionCallback;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_create_function(
			env,
			"rocksdb-js.lock.callback.complete",
			NAPI_AUTO_LENGTH,
			[](napi_env env, napi_callback_info info) -> napi_value {
				// get the callback data from the function's data
				void* data;
				::napi_get_cb_info(env, info, nullptr, nullptr, nullptr, &data);
				LockCallbackCompletionData* callbackData = static_cast<LockCallbackCompletionData*>(data);

				if (callbackData) {
					// check if this callback is still valid
					if (auto desc = callbackData->descriptor.lock()) {
						// call the completion handler
						DEBUG_LOG("callJsCallback calling onCallbackComplete() (key=\"%s\")\n", callbackData->key.c_str());
						desc->onCallbackComplete(callbackData->key);
					} else {
						DEBUG_LOG("callJsCallback completion callback has no descriptor (key=\"%s\")\n", callbackData->key.c_str());
					}
					delete callbackData;
				}

				NAPI_RETURN_UNDEFINED();
			},
			callbackData,
			&completionCallback
		),
		delete callbackData,
		"failed to create completion callback"
	);

	// call the original callback without any arguments
	napi_value global;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_get_global(env, &global),
		delete callbackData,
		"napi_get_global() failed"
	);

	napi_value result;
	DEBUG_LOG("callJsCallback calling js callback (key=\"%s\")\n", callbackData->key.c_str());
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_call_function(env, global, jsCallback, 0, nullptr, &result),
		{
			if (auto desc = callbackData->descriptor.lock()) {
				desc->onCallbackComplete(callbackData->key);
			}
			delete callbackData;
		},
		"napi_call_function() failed"
	);

	// check if the result is a Promise
	napi_value promiseCtor;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_get_named_property(env, global, "Promise", &promiseCtor),
		// not a promise environment, complete immediately
		if (auto desc = callbackData->descriptor.lock()) {
			desc->onCallbackComplete(callbackData->key);
		}
		delete callbackData,
		"failed to get Promise constructor"
	);

	bool isPromise;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_instanceof(env, result, promiseCtor, &isPromise),
		// assume not a promise, complete immediately
		if (auto desc = callbackData->descriptor.lock()) {
			desc->onCallbackComplete(callbackData->key);
		}
		delete callbackData,
		"napi_instanceof() failed"
	);

	if (!isPromise) {
		DEBUG_LOG("callJsCallback result is not a Promise, completing immediately (key=\"%s\")\n", callbackData->key.c_str());

		// If this is a withLock call with a deferred promise, resolve it
		if (callbackData->deferred != nullptr) {
			DEBUG_LOG("callJsCallback resolving deferred promise for synchronous withLock (key=\"%s\")\n", callbackData->key.c_str());
			napi_value undefined;
			napi_get_undefined(env, &undefined);
			napi_resolve_deferred(env, callbackData->deferred, undefined);
		}

		if (auto desc = callbackData->descriptor.lock()) {
			desc->onCallbackComplete(callbackData->key);
		}
		return;
	}

	DEBUG_LOG("callJsCallback result is a Promise, attaching .then() callback (key=\"%s\")\n", callbackData->key.c_str());

	// get the 'then' method from the promise
	napi_value thenMethod;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_get_named_property(env, result, "then", &thenMethod),
		if (auto desc = callbackData->descriptor.lock()) {
			desc->onCallbackComplete(callbackData->key);
		}
		delete callbackData,
		"failed to get .then() method"
	);

	// create resolve and reject callbacks that both complete the lock
	// we need to store the shared_ptr in a way N-API callbacks can access it
	auto* resolveDataPtr = new std::shared_ptr<LockCallbackCompletionData>(callbackDataPtr);

	napi_value resolveCallback;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_create_function(
			env,
			"rocksdb-js.lock.callback.resolve",
			NAPI_AUTO_LENGTH,
			[](napi_env env, napi_callback_info info) -> napi_value {
				napi_value result;
				::napi_get_undefined(env, &result);

				DEBUG_LOG("callJsCallback promise resolve callback\n");

				void* data;
				::napi_get_cb_info(env, info, nullptr, nullptr, nullptr, &data);
				auto* callbackDataPtr = static_cast<std::shared_ptr<LockCallbackCompletionData>*>(data);

				if (callbackDataPtr && *callbackDataPtr) {
					auto& callbackData = **callbackDataPtr;
					auto desc = callbackData.descriptor.lock();
					if (!callbackData.completed.exchange(true) && desc) {
						DEBUG_LOG("callJsCallback promise resolved, calling onCallbackComplete() (key=\"%s\")\n", callbackData.key.c_str());

						// if this is a withLock call with a deferred promise, resolve it
						if (callbackData.deferred != nullptr) {
							DEBUG_LOG("callJsCallback resolving deferred promise for withLock (key=\"%s\")\n", callbackData.key.c_str());
							napi_value undefined;
							napi_get_undefined(env, &undefined);
							napi_resolve_deferred(env, callbackData.deferred, undefined);
						}

						desc->onCallbackComplete(callbackData.key);
					} else {
						DEBUG_LOG("callJsCallback promise resolve callback already completed (key=\"%s\")\n", callbackData.key.c_str());
					}
				}

				// clean up the shared_ptr wrapper
				delete callbackDataPtr;
				return result;
			},
			resolveDataPtr,
			&resolveCallback
		),
		/* cleanup */ {
			if (auto desc = callbackData->descriptor.lock()) {
				desc->onCallbackComplete(callbackData->key);
			}
			delete resolveDataPtr;
		},
		"failed to create resolve callback"
	);

	// create reject callback - shared_ptr handles safe sharing between resolve/reject
	auto* rejectDataPtr = new std::shared_ptr<LockCallbackCompletionData>(callbackDataPtr);

	napi_value rejectCallback;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_create_function(
			env,
			"rocksdb-js.lock.callback.reject",
			NAPI_AUTO_LENGTH,
			[](napi_env env, napi_callback_info info) -> napi_value {
				napi_value result;
				::napi_get_undefined(env, &result);

				void* data;
				::napi_get_cb_info(env, info, nullptr, nullptr, nullptr, &data);
				auto* callbackDataPtr = static_cast<std::shared_ptr<LockCallbackCompletionData>*>(data);

				if (callbackDataPtr && *callbackDataPtr) {
					auto& callbackData = **callbackDataPtr;
					if (auto desc = callbackData.descriptor.lock()) {
						DEBUG_LOG("callJsCallback promise rejected, calling onCallbackComplete() (key=\"%s\")\n", callbackData.key.c_str());

						// if this is a withLock call with a deferred promise, reject it
						if (callbackData.deferred != nullptr) {
							DEBUG_LOG("callJsCallback rejecting deferred promise for withLock (key=\"%s\")\n", callbackData.key.c_str());
							// get the error from the first argument of the reject callback
							size_t argc = 1;
							napi_value argv[1];
							napi_get_cb_info(env, info, &argc, argv, nullptr, nullptr);
							napi_value error = argc > 0 ? argv[0] : nullptr;
							if (error == nullptr) {
								napi_get_undefined(env, &error);
							}
							napi_reject_deferred(env, callbackData.deferred, error);
						}

						desc->onCallbackComplete(callbackData.key);
					}
				}

				// clean up the shared_ptr wrapper
				delete callbackDataPtr;
				return result;
			},
			rejectDataPtr,
			&rejectCallback
		),
		/* cleanup */ {
			if (auto desc = callbackData->descriptor.lock()) {
				desc->onCallbackComplete(callbackData->key);
			}
			delete rejectDataPtr;
			delete resolveDataPtr;
		},
		"failed to create reject callback"
	);

	// call `promise.then(resolveCallback, rejectCallback)` for key "key"
	napi_value thenArgs[] = { resolveCallback, rejectCallback };
	napi_value thenResult;
	CALL_JS_CB_NAPI_STATUS_CHECK(
		::napi_call_function(env, result, thenMethod, 2, thenArgs, &thenResult),
		{
			if (auto desc = callbackData->descriptor.lock()) {
				desc->onCallbackComplete(callbackData->key);
			}
			delete resolveDataPtr;
			delete rejectDataPtr;
		},
		"failed to call .then()"
	);
}

/**
 * Finalize callback for when the user shared ArrayBuffer is garbage collected.
 * It removes the corresponding entry from the `userSharedBuffers` map to and
 * calls the finalize function, which removes the event listener, if applicable.
 */
static void userSharedBufferFinalize(napi_env env, void* unusedData, void* hint) {
	auto* finalizeData = static_cast<UserSharedBufferFinalizeData*>(hint);

	if (auto dbHandle = finalizeData->dbHandle.lock()) {
		DEBUG_LOG("userSharedBufferFinalize GC'd dbHandle=%p\n", dbHandle.get());
		// Remove this buffer's listener by identity, never by its napi_ref: the
		// ref is owned by the listener's tsfn (see UserSharedBufferFinalizeData),
		// so re-resolving it here was the shutdown UAF in #790. removeListener is
		// a no-op if the listener is already gone (close / env teardown).
		if (auto listener = finalizeData->listener.lock()) {
			if (dbHandle->descriptor) {
				DEBUG_LOG("%p userSharedBufferFinalize removing listener for key:", dbHandle.get());
				DEBUG_LOG_KEY_LN(finalizeData->key);
				dbHandle->descriptor->removeListener(finalizeData->key, listener);
			}
		}
	} else {
		DEBUG_LOG("userSharedBufferFinalize GC'd dbHandle was already destroyed for key:");
		DEBUG_LOG_KEY_LN(finalizeData->key);
	}

	if (auto columnDescriptor = finalizeData->columnDescriptor.lock()) {
		if (finalizeData->sharedData) {
			DEBUG_LOG("%p userSharedBufferFinalize releasing user shared buffer (column=%p) for key:", columnDescriptor.get(), columnDescriptor->column.get());
			DEBUG_LOG_KEY(finalizeData->key);
			DEBUG_LOG_MSG(" (use_count: %ld)\n", finalizeData->sharedData.use_count());
			columnDescriptor->releaseUserSharedBuffer(finalizeData->key, finalizeData->sharedData);
		}
	} else {
		DEBUG_LOG("userSharedBufferFinalize columnDescriptor was already destroyed for key:");
		DEBUG_LOG_KEY_LN(finalizeData->key);
	}

	// Destroying finalizeData drops the last strong ref to the shared data
	// when this was the final external ArrayBuffer; the buffer storage is
	// released here rather than in DBDescriptor::close().
	delete finalizeData;
}

napi_value DBDescriptor::getUserSharedBuffer(
	napi_env env,
	std::string& key,
	std::shared_ptr<DBHandle> dbHandle,
	napi_value defaultBuffer,
	std::shared_ptr<ListenerCallback> listener
) {
	bool isArrayBuffer;
	NAPI_STATUS_THROWS(::napi_is_arraybuffer(env, defaultBuffer, &isArrayBuffer));
	if (!isArrayBuffer) {
		::napi_throw_error(env, nullptr, "Default buffer must be an ArrayBuffer");
		return nullptr;
	}

	std::lock_guard<std::mutex> lock(dbHandle->columnDescriptor->userSharedBuffersMutex);

	auto userSharedBufferIter = dbHandle->columnDescriptor->userSharedBuffers.find(key);
	if (userSharedBufferIter == dbHandle->columnDescriptor->userSharedBuffers.end()) {
		// shared buffer does not exist, create it
		void* data;
		size_t size;

		NAPI_STATUS_THROWS(::napi_get_arraybuffer_info(
			env,
			defaultBuffer,
			&data,
			&size
		));

		DEBUG_LOG("%p DBHandle::getUserSharedBuffer Initializing user shared buffer with default buffer size: %zu\n", this, size);
		userSharedBufferIter = dbHandle->columnDescriptor->userSharedBuffers.emplace(key, std::make_shared<UserSharedBufferData>(data, size)).first;
	} else {
		DEBUG_LOG("%p DBHandle::getUserSharedBuffer User shared buffer already initialized for key:", this);
	}

	auto userSharedBuffer = userSharedBufferIter->second;

	DEBUG_LOG("%p DBHandle::getUserSharedBuffer Creating external ArrayBuffer with size %zu for key:", this, userSharedBuffer->size);
	DEBUG_LOG_KEY_LN(key);

	// Hold a strong ref to the user shared buffer data here so the external
	// ArrayBuffer's storage outlives DBDescriptor / ColumnFamilyDescriptor
	// teardown (the map may be cleared on close() while JS still retains the
	// ArrayBuffer). The data is released when this finalize data is destroyed.
	auto* finalizeData = new UserSharedBufferFinalizeData(
		key,
		std::weak_ptr<DBHandle>(dbHandle),
		std::weak_ptr<ColumnFamilyDescriptor>(dbHandle->columnDescriptor),
		userSharedBuffer,
		std::weak_ptr<ListenerCallback>(listener)
	);

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_external_arraybuffer(
		env,
		userSharedBuffer->data,   // data
		userSharedBuffer->size,   // size
		userSharedBufferFinalize, // finalize_cb
		finalizeData,             // finalize_hint
		&result                   // [out] result
	));
	return result;
}

/**
 * Adds an event listener to this descriptor's event emitter.
 */
std::shared_ptr<ListenerCallback> DBDescriptor::addListener(
	napi_env env,
	std::string& key,
	napi_value callback,
	std::weak_ptr<DBHandle> owner
) {
	// Convert the typed weak_ptr to a type-erased weak_ptr<void> for the
	// EventEmitter. The pointer value held by the weak_ptr is preserved
	// because DBHandle has no virtual/multiple-inheritance offset for void*.
	auto sp = owner.lock();
	std::weak_ptr<void> erasedOwner;
	if (sp) {
		erasedOwner = std::shared_ptr<void>(sp, sp.get());
	}
	return this->events.addListener(env, key, callback, erasedOwner);
}

bool DBDescriptor::notify(std::string key, ListenerData* data) {
	return this->events.notify(key, data);
}

napi_value DBDescriptor::listeners(napi_env env, std::string& key) {
	return this->events.listeners(env, key);
}

napi_value DBDescriptor::removeListener(napi_env env, std::string& key, napi_value callback) {
	return this->events.removeListener(env, key, callback);
}

void DBDescriptor::removeListener(const std::string& key, const std::shared_ptr<ListenerCallback>& target) {
	this->events.removeListener(key, target);
}

void DBDescriptor::removeListenersByOwner(DBHandle* owner) {
	this->events.removeListenersByOwner(static_cast<void*>(owner));
}

void DBDescriptor::removeListenersByEnv(napi_env env) {
	this->events.removeListenersByEnv(env);
}

/**
 * Lists all transaction logs in the database.
 *
 * @param env The environment of the current callback.
 */
napi_value DBDescriptor::listTransactionLogStores(napi_env env) {
	return TransactionLogStoreRegistry::ListStores(env, this->path);
}

/**
 * Purges transaction logs.
 */
napi_value DBDescriptor::purgeTransactionLogs(napi_env env, napi_value options) {
	return TransactionLogStoreRegistry::PurgeStores(env, this->path, options);
}

/**
 * Finds or creates a transaction log store by name.
 *
 * @param name The name of the transaction log store.
 * @returns The transaction log store.
 */
std::shared_ptr<TransactionLogStore> DBDescriptor::resolveTransactionLogStore(const std::string& name) {
	return TransactionLogStoreRegistry::ResolveStore(this->path, name);
}

void DBDescriptor::setLastError(std::string json) {
	// Store, then (for a real error) emit — a single place so "stored" and
	// "emitted" never drift apart, whether the source is OnBackgroundError on a
	// RocksDB background thread or db.setLastError() on the JS thread.
	const bool hasError = !json.empty();
	{
		std::lock_guard<std::mutex> lock(this->lastErrorMutex);
		this->lastError = json;
	}
	// Emit OUTSIDE lastErrorMutex: notify takes the emitter's own lock and
	// dispatches asynchronously. Clearing (empty json) is a silent reset — no
	// event — mirroring Win32 SetLastError(0).
	if (hasError && this->events.hasListeners()) {
		this->events.notify("error", ListenerData::backgroundError(json));
	}
}

// Maps rocksdb::WriteStallCondition to a stable lowercase name for the
// 'writeStall' event.
static const char* writeStallConditionName(rocksdb::WriteStallCondition condition) {
	switch (condition) {
		case rocksdb::WriteStallCondition::kDelayed: return "delayed";
		case rocksdb::WriteStallCondition::kStopped: return "stopped";
		case rocksdb::WriteStallCondition::kNormal: return "normal";
		default: return "unknown";
	}
}

// Rate-limit window for the 'writeStall' rising edge, in milliseconds
// (`ROCKSDB_JS_WRITE_STALL_DEBOUNCE_MS`, default 1000): during one oscillating
// stall episode a CF re-emits at most once per window. Resolved once at
// DBDescriptor construction (JS thread) rather than on the emit path, so the
// RocksDB background thread never touches ::getenv (the parkTimeoutMs
// getenv-vs-setenv caveat). 0 disables the window (every rising edge emits);
// malformed/negative falls back to the default. Mirrors parkTimeoutMs' parsing,
// except 0 is honored as an explicit opt-out here rather than treated as ambiguous.
static uint64_t writeStallDebounceMs() {
	static const uint64_t ms = []() -> uint64_t {
		constexpr uint64_t kDefault = 1000;
		const char* v = ::getenv("ROCKSDB_JS_WRITE_STALL_DEBOUNCE_MS");
		if (v == nullptr) {
			return kDefault;
		}
		const char* firstNonSpace = v;
		while (*firstNonSpace != '\0' && ::isspace(static_cast<unsigned char>(*firstNonSpace))) {
			++firstNonSpace;
		}
		if (*firstNonSpace == '\0' || *firstNonSpace == '-') {
			return kDefault;
		}
		char* end = nullptr;
		errno = 0;
		unsigned long long parsed = ::strtoull(v, &end, 10);
		if (end == v || *end != '\0' || errno == ERANGE) {
			return kDefault;
		}
		return static_cast<uint64_t>(parsed); // 0 = debounce disabled
	}();
	return ms;
}

void DBDescriptor::emitWriteStall(
	const std::string& columnFamily,
	rocksdb::WriteStallCondition previous,
	rocksdb::WriteStallCondition current
) {
	// The FSM decides and emits under one lock so a CF's decision -> enqueue is
	// atomic: RocksDB does not guarantee serialized per-CF listener callbacks, so
	// releasing between them could let a later transition's enqueue overtake this
	// one and reorder what JS sees. The enqueue is a non-blocking tsfn call, so the
	// critical section stays short. The FSM advances regardless of listeners (a
	// detach mid-stall can't strand state); only the enqueue is gated on
	// hasListeners() to avoid building a payload nobody will receive.
	const bool isStalled = current != rocksdb::WriteStallCondition::kNormal;
	this->writeStallDebounce.onTransition(
		columnFamily, isStalled, std::chrono::steady_clock::now(), this->writeStallDebounceWindowMs,
		[&]() {
			if (this->events.hasListeners()) {
				this->events.notify("writeStall", ListenerData::fromStrings({
					columnFamily,
					writeStallConditionName(previous),
					writeStallConditionName(current)
				}));
			}
		});
}

/**
 * Returns a copy of the last error that occurred on this database.
 */
std::string DBDescriptor::getLastError() {
	std::lock_guard<std::mutex> lock(this->lastErrorMutex);
	return this->lastError;
}

rocksdb::Status DBDescriptor::flush(bool allowWriteStall) {
	if (this->readOnly) {
		DEBUG_LOG("%p DBDescriptor::flush Skipping flush for readonly database\n", this);
		return rocksdb::Status::OK();
	}

	// Snapshot the column family descriptors under the columns mutex. flush()
	// can run on a libuv worker thread while the JS thread drops a column
	// family (which erases from the map); the shared_ptr copies also pin the
	// handles so they cannot be destroyed mid-Flush.
	std::vector<std::shared_ptr<ColumnFamilyDescriptor>> pinnedColumns;
	{
		std::lock_guard<std::mutex> lock(this->columnsMutex);
		pinnedColumns.reserve(this->columns.size());
		for (const auto& [name, columnDescriptor] : this->columns) {
			pinnedColumns.push_back(columnDescriptor);
		}
	}
	std::vector<rocksdb::ColumnFamilyHandle*> columnHandles;
	columnHandles.reserve(pinnedColumns.size());
	for (const auto& columnDescriptor : pinnedColumns) {
		columnHandles.push_back(columnDescriptor->column.get());
	}
	// Perform flush
	rocksdb::FlushOptions flushOptions;
	flushOptions.allow_write_stall = allowWriteStall;
	return this->db->Flush(
		flushOptions,
		columnHandles
	);
}

rocksdb::Status DBDescriptor::compactRange(
	rocksdb::ColumnFamilyHandle* column,
	const rocksdb::Slice* start,
	const rocksdb::Slice* end,
	bool bottommost
) {
	std::lock_guard<std::mutex> lock(this->compactMutex);
	DEBUG_LOG("%p DBDescriptor::compactRange Compacting range (bottommost=%d)\n", this, bottommost);
	rocksdb::CompactRangeOptions options;
	if (bottommost) {
		// RocksDB defaults this to kIfHaveCompactionFilter, so with no compaction filter installed
		// the bottommost level is skipped — and that is where the bulk of the data sits. Rewriting
		// it is the only way to re-encode existing files (a changed compression codec applies to
		// newly written files only), so it has to be requested explicitly. kForceOptimized (rather
		// than kForce) still avoids double-compacting bottommost files this same manual compaction
		// already produced.
		options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForceOptimized;
		// SST re-encoding alone leaves large values on the old codec: they live in blob files, and
		// blob GC's default age cutoff only reclaims the oldest fraction of blob files. Force GC
		// across the full age range so a bottommost compaction re-encodes blobs too.
		options.blob_garbage_collection_policy = rocksdb::BlobGarbageCollectionPolicy::kForce;
		options.blob_garbage_collection_age_cutoff = 1.0;
	}
	return this->db->CompactRange(
		options,
		column,
		start,
		end
	);
}

/**
 * Records where a column family created after the open keeps its blob files,
 * and mirrors the layout into the registry so it survives this descriptor.
 *
 * `layoutMutex` is released before that call: `DBRegistry::DestroyDB` takes it
 * while holding `databasesMutex`, so recording must not reach back for a
 * registry lock from under it.
 */
void DBDescriptor::recordColumnFamilyLayout(const std::string& name, const std::string& blobDir) {
	DBFileLayout layout;
	{
		std::lock_guard<std::mutex> lock(this->layoutMutex);
		this->layoutBlobDirs[name] = blobDir;
		layout = DBFileLayout{ this->layoutDbPaths, this->layoutBlobDirs };
	}
	DBRegistry::RecordLayout(this->path, std::move(layout));
}

void DBDescriptor::removeColumnFamilyLayout(const std::string& name) {
	std::lock_guard<std::mutex> lock(this->layoutMutex);
	this->layoutBlobDirs.erase(name);
}

} // namespace rocksdb_js
