#include <chrono>
#include <vector>
#include "database/db_registry.h"
#include "transaction/transaction_handle.h"
#include "database/db_settings.h"
#include "core/test_seam.h"
#include "napi/macros.h"
#include "core/platform.h"
#include "core/compression.h"
#include "napi/helpers.h"
#include "napi/async.h"
#include "napi/global_events.h"
#include "rocksdb/table.h"
#include <exception>
#include <thread>

namespace rocksdb_js {

namespace {

struct ClosingDescriptor final {
	DBKey key;
	std::shared_ptr<DBDescriptor> descriptor;
	std::shared_ptr<std::condition_variable> condition;
	bool closed = false;
	std::string closeError;

	ClosingDescriptor(
		const DBKey& key,
		std::shared_ptr<DBDescriptor> descriptor,
		std::shared_ptr<std::condition_variable> condition
	) : key(key), descriptor(std::move(descriptor)), condition(std::move(condition)) {}
};

void emitCloseFailure(const std::string& path, const std::string& error) {
	if (!error.empty() && GlobalEvents::hasListeners()) {
		emitGlobalEvent("database:closeFailed", ListenerData::fromStrings({path, error}));
	}
}

void emitCloseFailures(const std::vector<ClosingDescriptor>& descriptors) {
	for (const auto& closing : descriptors) {
		// The descriptor's spelling, not the key's resolved identity -- the same
		// rule registryStatus() follows, so a listener can match the event's path
		// against the path it opened.
		emitCloseFailure(
			closing.descriptor ? closing.descriptor->path : closing.key.path, closing.closeError);
	}
}

struct ClaimedCloseOptions final {
	// destroy() is deleting the data, so it forces teardown and does not treat a
	// close that finished but reported an error (a failed close-time flush) as
	// fatal. Every other caller does: dropping that error silently would hide
	// possible data loss. The entry is erased either way, so the failure is
	// reported once rather than wedging the path.
	bool destroying = false;
	bool failOnCompletedWithError = true;
};

/**
 * Runs finishClose() over descriptors already claimed by the caller, then
 * erases each entry or quarantines it with its close error, notifies that
 * path's waiters, and emits `database:closeFailed`.
 *
 * Returns the first exception the caller should rethrow, or null. Claiming
 * differs per caller (one path, every path, or a single unreferenced
 * descriptor); everything after the claim is this one policy.
 */
std::exception_ptr closeClaimedDescriptors(
	std::vector<ClosingDescriptor>& claimed,
	const ClaimedCloseOptions& options,
	std::unordered_map<DBKey, DBRegistryEntry, DBKeyHash>& databases,
	std::mutex& databasesMutex
) {
	std::exception_ptr closeError;

	for (auto& closing : claimed) {
		DEBUG_LOG("DBRegistry::closeClaimedDescriptors Closing descriptor %p for \"%s\" (ref count = %ld)\n",
			closing.descriptor.get(), closing.key.path.c_str(), closing.descriptor.use_count());

		std::exception_ptr thrown;
		try {
			closing.descriptor->finishClose(options.destroying);
			closing.closed = true;
		} catch (const std::exception& error) {
			closing.closed = closing.descriptor->isClosed();
			closing.closeError = error.what();
			thrown = std::current_exception();
		} catch (...) {
			closing.closed = closing.descriptor->isClosed();
			closing.closeError = "unknown native close failure";
			thrown = std::current_exception();
		}

		if (thrown && !closeError && (!closing.closed || options.failOnCompletedWithError)) {
			closeError = thrown;
		}

		{
			std::lock_guard<std::mutex> lock(databasesMutex);
			auto entry = databases.find(closing.key);
			if (entry != databases.end() && entry->second.descriptor == closing.descriptor) {
				if (closing.closed) {
					databases.erase(entry);
				} else {
					entry->second.closeError = closing.closeError;
					entry->second.closeRetrying = false;
					DEBUG_LOG("DBRegistry::closeClaimedDescriptors Quarantined \"%s\": %s\n",
						closing.key.path.c_str(), closing.closeError.c_str());
				}
			}
		}
		closing.condition->notify_all();
	}

	emitCloseFailures(claimed);
	return closeError;
}

} // namespace

// Initialize the static instance
std::unique_ptr<DBRegistry> DBRegistry::instance;

/**
 * Close a RocksDB database handle.
 */
CloseResult DBRegistry::CloseDB(const std::shared_ptr<DBHandle> handle) {
	if (!instance) {
		DEBUG_LOG("%p DBRegistry::CloseDB Registry not initialized\n", instance.get());
		return {};
	}

	if (!handle) {
		DEBUG_LOG("%p DBRegistry::CloseDB Invalid handle\n", instance.get());
		return {};
	}

#ifdef DEBUG
	DBRegistry::DebugLogDescriptorRefs();
#endif

	if (!handle->descriptor) {
		DEBUG_LOG("%p DBRegistry::CloseDB Database not opened\n", instance.get());
		return {};
	}

	DBKey key = descriptorKey(*handle->descriptor);
	std::shared_ptr<DBDescriptor> descriptor = handle->descriptor;

	// close the handle, decrements the descriptor ref count
	handle->close();

	// Detach only after close() returns, not before: while this handle stays
	// attached to `closables`, a concurrent destroy()/shutdown() that claims
	// this descriptor in the same window sees it in its sweep and calls
	// close() on it too -- serialized against this call by `closeMutex`, and
	// a no-op once it runs (closeIfOpen-style idempotency; see close()'s
	// per-substep guards). That makes the foreign finishClose() wait out this
	// close's async-work drain instead of skipping the handle: an admitted
	// Flush/Compact/Get releases its OperationGuard at setup handoff, so
	// nothing else would stop finishClose() from reaching db.reset() while
	// that op's execute callback is still using descriptor->db. Detaching
	// first (the previous order) left that in-flight op with no closer
	// waiting on it. Snapshot `descriptor` before close(), which resets
	// `handle->descriptor` to null on this (the owning) thread.
	descriptor->detach(handle);
	// Release this local ref before the purge check below: it counts
	// live references via use_count(), and a ref still held here would
	// make it see 2 instead of 1 and wrongly skip the purge.
	descriptor.reset();

	return DBRegistry::PurgeIfUnreferenced(key);
}

/**
 * Purges (closes and erases) the registry entry for `key` if no DBHandle
 * references its descriptor anymore; a no-op otherwise. This is the tail of
 * every close: CloseDB calls it after detaching the handle, and the async
 * operations that hold their own `shared_ptr<DBDescriptor>` for the duration
 * of a copy (backup, backup stream, checkpoint) call it when they release that
 * reference — a close that raced such an operation saw use_count() > 1 and
 * skipped the purge, so the releasing operation must retry it or the entry
 * (and the open RocksDB) would linger in the registry forever.
 *
 * The decision is made, and ownership of the descriptor taken, all under
 * databasesMutex. Multiple threads can race here for one path (worker envs
 * tearing down concurrently, or an async op's release racing a CloseDB), so
 * the decision MUST be atomic:
 *
 *   - We never hold a raw pointer into the map across the unlocked
 *     finishClose() below. An earlier implementation cached `&entry` under the
 *     lock and dereferenced it afterward; a concurrent purge that erased the
 *     map node freed that storage, so the survivor called close() on a freed
 *     DBDescriptor and locked its destroyed mutex (manifests on glibc as
 *     "malloc(): unaligned tcache chunk detected").
 *   - The registry always holds one ref, so use_count() <= 1 means no open
 *     DBHandles remain. OpenDB bumps use_count under this same lock, so the
 *     check serializes with it: if an open raced ahead it already pushed the
 *     count past 1 and we skip; if we win, beginClose() publishes the closing
 *     state while we still hold the lock, so a subsequent OpenDB observes
 *     isClosing() and waits instead of being handed a descriptor we then
 *     close out from under it. beginClose() also makes the claim single-shot.
 *   - The entry stays in the map (descriptor non-null and isClosing()) for
 *     the duration of finishClose(), so a concurrent OpenDB keeps waiting on
 *     the condition rather than re-opening the path mid-close.
 */
CloseResult DBRegistry::PurgeIfUnreferenced(const DBKey& key) {
	if (!instance) {
		return CloseResult{};
	}

	std::shared_ptr<DBDescriptor> descriptor;
	std::shared_ptr<std::condition_variable> condition;
	{
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		auto entryIterator = instance->databases.find(key);
		if (entryIterator != instance->databases.end()) {
			DBRegistryEntry& entry = entryIterator->second;
			DEBUG_LOG("%p DBRegistry::PurgeIfUnreferenced Found DBDescriptor for \"%s\" (ref count = %ld)\n", instance.get(), key.path.c_str(), entry.descriptor.use_count());
			if (entry.descriptor && entry.descriptor.use_count() <= 1 && entry.descriptor->beginClose()) {
				DEBUG_LOG("%p DBRegistry::PurgeIfUnreferenced Claiming descriptor purge for \"%s\"\n", instance.get(), key.path.c_str());
				descriptor = entry.descriptor;
				condition = entry.condition;
			}
		} else {
			DEBUG_LOG("%p DBRegistry::PurgeIfUnreferenced DBDescriptor not found! \"%s\"\n", instance.get(), key.path.c_str());
		}
	}

	if (!descriptor) {
		return CloseResult{};
	}

	// We claimed the close under the lock via beginClose(); run the actual
	// teardown now. The local copy keeps the descriptor alive throughout.
	// Only the entry we claimed is erased -- a brand-new descriptor cannot
	// appear because OpenDB blocks until the helper notifies.
	std::vector<ClosingDescriptor> claimed;
	claimed.emplace_back(key, descriptor, condition);
	// The close error is reported through CloseResult, not thrown.
	closeClaimedDescriptors(
		claimed, ClaimedCloseOptions{}, instance->databases, instance->databasesMutex);
	const std::string& closeError = claimed.front().closeError;
	const bool quarantined = !closeError.empty() && !claimed.front().closed;
	return CloseResult{closeError, quarantined};
}

/**
 * Debug log the reference count of all descriptors in the registry.
 */
#ifdef DEBUG
void DBRegistry::DebugLogDescriptorRefs() {
	std::lock_guard<std::mutex> lock(instance->databasesMutex);
	DEBUG_LOG("DBRegistry::DebugLogDescriptorRefs %zu descriptor%s in registry:\n", instance->databases.size(), instance->databases.size() == 1 ? "" : "s");
	for (auto& [key, entry] : instance->databases) {
		DEBUG_LOG("  %p for \"%s\" (ref count = %ld)\n", entry.descriptor.get(), key.path.c_str(), entry.descriptor.use_count());
	}
}
#endif

/**
 * Destroy a RocksDB database.
 *
 * @param path - The (already resolved-identity) path to the database to destroy.
 */
namespace {

/**
 * RAII release for a path claimed in `destroyingPaths`. Always removes and
 * notifies on scope exit -- success, a claim/close failure, or a physical
 * deletion failure -- so a failed destroy() never leaves the path
 * permanently unopenable (registry-level quarantine, when it applies, is
 * carried by the entry's `closeError` instead; see DestroyDB).
 */
class DestroyPathGuard final {
public:
	DestroyPathGuard(std::mutex& mutex, std::condition_variable& condition,
		std::unordered_set<std::string>& destroyingPaths, const std::string& path)
		: mutex(mutex), condition(condition), destroyingPaths(destroyingPaths), path(path) {}

	~DestroyPathGuard() {
		{
			std::lock_guard<std::mutex> lock(this->mutex);
			this->destroyingPaths.erase(this->path);
		}
		this->condition.notify_all();
	}

private:
	std::mutex& mutex;
	std::condition_variable& condition;
	std::unordered_set<std::string>& destroyingPaths;
	const std::string& path;
};

} // namespace

void DBRegistry::DestroyDB(const std::string& path) {
	if (!instance) {
		DEBUG_LOG("%p DBRegistry::DestroyDB Registry not initialized\n", instance.get());
		return;
	}

	DEBUG_LOG("%p DBRegistry::DestroyDB Destroying \"%s\"\n", instance.get(), path.c_str());

	const std::string& identityPath = path;
	if (identityPath.empty()) {
		// This function ends in remove_all(), so an empty target is never a
		// no-op worth risking: it is a caller bug, and on a libc++ platform an
		// empty path resolves to the process CWD.
		throw rocksdb_js::DBException("Cannot destroy database: no database path");
	}

	// Each wait below gets its own budget rather than sharing one deadline
	// across the whole call: a slow earlier wait would otherwise eat into a
	// later one's budget and report a timeout with work still legitimately in
	// progress.
	const auto waitBudget =
		std::chrono::seconds(DBSettings::getInstance().getLifecycleWaitSeconds());

	// Claim the path-level gate before touching any entry. This is what keeps
	// a brand-new key on this path (one this destroy never saw in the registry
	// scan below, e.g. a first-ever secondary open, or ANY open once every
	// entry has already been erased) from racing the physical deletion further
	// down -- that deletion runs without databasesMutex held, so the registry
	// alone cannot gate it once the entries are gone.
	{
		std::unique_lock<std::mutex> lock(instance->databasesMutex);
		if (!instance->lifecycleCondition.wait_until(lock, std::chrono::steady_clock::now() + waitBudget, [&]() {
			return instance->destroyingPaths.find(identityPath) == instance->destroyingPaths.end();
		})) {
			throw rocksdb_js::DBException("Timed out waiting to destroy database \"" + path + "\": another lifecycle operation is still in progress");
		}
		instance->destroyingPaths.insert(identityPath);
	}
	DestroyPathGuard pathGuard(instance->databasesMutex, instance->lifecycleCondition, instance->destroyingPaths, identityPath);

	// One path can hold several descriptors — read-write, read-only, and any
	// number of secondaries (the registry key is {path, readOnly,
	// secondaryPath}) — and destroy deletes the files under all of them, so
	// every one must be claimed and closed, not just the first found: an entry
	// erased unclosed leaks its resources for the life of the process (a
	// secondary's workspace `.secondary.lock` is only released by
	// finishClose(), so a leaked secondary wedges its workspace permanently).
	// A quarantined entry (a prior close/destroy left `closeError` set) is
	// retried here too, rather than skipped, so destroy() is the caller's
	// recovery path for a wedged path even when no handle survived to retry it.
	//
	// The spelling to report to JS if this destroy ends in a tombstone: the key
	// and `identityPath` are resolved identity, which a caller matching against
	// the path it opened would not recognize. Taken from the first descriptor
	// claimed below, since the descriptors are gone by the time the tombstone is
	// written; `identityPath` is the only option for a path no descriptor in this
	// process ever opened.
	std::string reportedPath = identityPath;
	bool reportedPathKnown = false;
	while (true) {
		std::vector<ClosingDescriptor> claimed;
		std::vector<ClosingDescriptor> alreadyClosing;
		{
			std::unique_lock<std::mutex> lock(instance->databasesMutex);
			claimed.reserve(instance->databases.size());
			alreadyClosing.reserve(instance->databases.size());
			for (auto& [key, entry] : instance->databases) {
				if (key.path != identityPath || !entry.descriptor) {
					continue;
				}
				ClosingDescriptor closing{key, entry.descriptor, entry.condition};
				if (!entry.closeError.empty() && !entry.closeRetrying) {
					entry.closeRetrying = true;
					claimed.push_back(std::move(closing));
				} else if (entry.descriptor->beginClose()) {
					claimed.push_back(std::move(closing));
				} else {
					alreadyClosing.push_back(std::move(closing));
				}
			}
			if (!reportedPathKnown) {
				for (const auto& [key, entry] : instance->databases) {
					if (key.path != identityPath) {
						continue;
					}
					if (entry.descriptor) {
						reportedPath = entry.descriptor->path;
						reportedPathKnown = true;
						break;
					}
					// A tombstone left by an earlier failed destroy() already
					// remembers the spelling, which is all a retry of that destroy
					// has: it finds no descriptor to ask. Keep scanning for a live
					// one anyway -- it is the more authoritative source.
					if (!entry.reportedPath.empty()) {
						reportedPath = entry.reportedPath;
					}
				}
			}
		}

		// Each entry stays discoverable until its own close finishes: env
		// cleanup uses the registry to remove callbacks owned by a worker that
		// exits mid-close.
		std::exception_ptr closeError = closeClaimedDescriptors(
			claimed,
			ClaimedCloseOptions{.destroying = true, .failOnCompletedWithError = false},
			instance->databases,
			instance->databasesMutex
		);
		if (alreadyClosing.empty()) {
			if (closeError) std::rethrow_exception(closeError);
			break;
		}
		for (const auto& closing : alreadyClosing) {
			std::unique_lock<std::mutex> lock(instance->databasesMutex);
			const auto drainDeadline = std::chrono::steady_clock::now() + waitBudget;
			if (!closing.condition->wait_until(lock, drainDeadline, [&]() {
				auto entry = instance->databases.find(closing.key);
				return entry == instance->databases.end() ||
					entry->second.descriptor != closing.descriptor ||
					(!entry->second.closeError.empty() && !entry->second.closeRetrying);
			})) {
				throw rocksdb_js::DBException("Timed out waiting to destroy database \"" + path + "\": an open descriptor is still closing");
			}
		}
		if (closeError) std::rethrow_exception(closeError);
	}

	// Every descriptor for this path is now closed and erased from the
	// registry. From here the `destroyingPaths` gate above is the only thing
	// keeping a concurrent OpenDB from recreating this path while physical
	// deletion (potentially slow: a large directory, a slow disk, or the test
	// seam below) is still in flight -- so this runs WITHOUT databasesMutex.
	const int destroyDelayMs = destroyDelayMsFlag().load(std::memory_order_relaxed);
	if (destroyDelayMs > 0) {
		std::this_thread::sleep_for(std::chrono::milliseconds(destroyDelayMs));
	}
	if (destroyFailureFlag().load(std::memory_order_relaxed)) {
		// Nothing was touched -- neither rocksdb::DestroyDB nor remove_all ran
		// -- so this leaves no tombstone: the path is exactly as it was, and a
		// caller can retry destroy() or just reopen it.
		throw rocksdb_js::DBException("Injected database destruction failure");
	}

	DEBUG_LOG("%p DBRegistry::DestroyDB Calling rocksdb::DestroyDB for \"%s\"\n", instance.get(), identityPath.c_str());
	rocksdb::Status status = rocksdb::DestroyDB(identityPath, rocksdb::Options());
	std::string destroyError;
	if (!status.ok()) {
		destroyError = status.ToString();
	} else {
		std::error_code cleanupError;
		std::filesystem::remove_all(identityPath, cleanupError);
		if (cleanupError) {
			destroyError = "Failed to remove database directory: " + cleanupError.message();
		}
	}
	if (!destroyError.empty()) {
		// rocksdb::DestroyDB (if it ran) already dropped RocksDB's own view of
		// this path, so reopening now would silently create a fresh, empty
		// database over whatever the filesystem failure left behind. Leave a
		// tombstone -- surfaced by registryStatus().destroyCleanupPending and
		// rejected by OpenDB's quarantine check -- so only an explicit destroy()
		// retry (not a plain reopen, and not shutdown(), which is deliberately
		// non-destructive) can clear it.
		{
			std::lock_guard<std::mutex> lock(instance->databasesMutex);
			DBRegistryEntry& tombstone = instance->databases[DBKey{identityPath, false, ""}];
			tombstone.closeError = destroyError;
			if (tombstone.reportedPath.empty()) {
				tombstone.reportedPath = reportedPath;
			}
		}
		emitCloseFailure(reportedPath, destroyError);
		throw rocksdb_js::DBException(destroyError);
	}

	// A retried destroy() can find a tombstone (descriptor already null) left by
	// an earlier failed attempt at this same path: the claim/close loop above
	// skips it (there is no descriptor to close), so nothing else erases it.
	// Physical deletion just succeeded, so nothing legitimately belongs at this
	// path anymore -- clear every remaining entry, tombstone or otherwise.
	std::vector<std::shared_ptr<std::condition_variable>> staleConditions;
	{
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		for (auto it = instance->databases.begin(); it != instance->databases.end(); ) {
			if (it->first.path == identityPath) {
				staleConditions.push_back(it->second.condition);
				it = instance->databases.erase(it);
			} else {
				++it;
			}
		}
	}
	for (const auto& condition : staleConditions) {
		condition->notify_all();
	}

	DEBUG_LOG("%p DBRegistry::DestroyDB Successfully destroyed database at \"%s\"\n", instance.get(), identityPath.c_str());
}

bool DBRegistry::CollectWriteBufferManagerInventory(
	const rocksdb::WriteBufferManager* wbm,
	uint64_t& columnFamilies,
	std::map<int64_t, uint64_t>& maxWriteBufferSizeToMaintain
) {
	columnFamilies = 0;
	maxWriteBufferSizeToMaintain.clear();
	if (!instance || wbm == nullptr) {
		return false;
	}
	std::unique_lock<std::mutex> lock(instance->databasesMutex, std::try_to_lock);
	if (!lock.owns_lock()) {
		return false;
	}
	uint64_t collectedColumnFamilies = 0;
	std::map<int64_t, uint64_t> collectedMaxWriteBufferSizeToMaintain;
	for (const auto& [key, entry] : instance->databases) {
		const auto& descriptor = entry.descriptor;
		if (!descriptor || descriptor->attachedWriteBufferManager != wbm) {
			continue;
		}
		std::unique_lock<std::mutex> columnsLock(descriptor->columnsMutex, std::try_to_lock);
		if (!columnsLock.owns_lock()) {
			return false;
		}
		for (const auto& [name, columnDescriptor] : descriptor->columns) {
			if (!columnDescriptor) {
				continue;
			}
			collectedColumnFamilies++;
			collectedMaxWriteBufferSizeToMaintain[columnDescriptor->maxWriteBufferSizeToMaintain]++;
		}
		// `expired()` only, never `lock()` — see `DBDescriptor::DroppedColumnFamily`.
		for (const auto& dropped : descriptor->droppedColumns) {
			if (dropped.descriptor.expired()) {
				continue;
			}
			collectedColumnFamilies++;
			collectedMaxWriteBufferSizeToMaintain[dropped.maxWriteBufferSizeToMaintain]++;
		}
	}
	columnFamilies = collectedColumnFamilies;
	maxWriteBufferSizeToMaintain = std::move(collectedMaxWriteBufferSizeToMaintain);
	return true;
}

/**
 * Initialize the singleton instance of the registry.
 */
void DBRegistry::Init(napi_env env, napi_value exports) {
	if (!instance) {
		instance = std::unique_ptr<DBRegistry>(new DBRegistry());
		DEBUG_LOG("%p DBRegistry::Initialize Initialized DBRegistry\n", instance.get());
	}

	napi_value registryStatusFn;
	NAPI_STATUS_THROWS_VOID(::napi_create_function(env, "registryStatus", NAPI_AUTO_LENGTH, DBRegistry::RegistryStatus, nullptr, &registryStatusFn));
	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, "registryStatus", registryStatusFn));
}

/**
 * Open a RocksDB database with column family, caches it in the registry, and
 * attaches the provided handle to it.
 *
 * @param handle - The handle that will own the selected database and column family.
 * @param path - The filesystem path to the database.
 * @param options - The options for the database.
 */
void DBRegistry::OpenDB(
	const std::shared_ptr<DBHandle>& handle,
	const std::string& path,
	const DBOptions& options
) {
	// ensure the registry has already been initialized
	if (!instance) {
		DEBUG_LOG("DBRegistry::OpenDB Registry not initialized!\n");
		throw rocksdb_js::DBException("DBRegistry not initialized!");
	}
	if (!handle) {
		throw rocksdb_js::DBException("Cannot open a database with an invalid handle");
	}

	DEBUG_LOG("%p DBRegistry::OpenDB Opening database \"%s\" (mode=%s read-only=%s column family=\"%s\")\n", instance.get(), path.c_str(), options.mode == DBMode::Optimistic ? "optimistic" : "pessimistic", options.readOnly ? "true" : "false", options.name.empty() ? "default" : options.name.c_str());

	// A secondary open is read-only by construction; Database::Open normalizes
	// the flag, and everything downstream (write guards, registry identity,
	// create_if_missing) relies on it.
	if (!options.secondaryPath.empty() && !options.readOnly) {
		throw rocksdb_js::DBException("Internal error: a secondary open must set readOnly");
	}

	std::unordered_map<std::string, std::shared_ptr<ColumnFamilyDescriptor>> columns;
	std::string name = options.name.empty() ? "default" : options.name;

	// The single identity for this open: the registry key, the workspace scan
	// and (via the descriptor) every transaction-log registry call all use this
	// one string, so two spellings of one directory cannot become two
	// descriptors. Resolved before the lock — it touches the filesystem, and
	// databasesMutex serializes every open and close in the process.
	const std::string identityPath = rocksdb_js::resolveIdentityPath(path).string();

	const auto deadline = std::chrono::steady_clock::now() +
		std::chrono::seconds(DBSettings::getInstance().getLifecycleWaitSeconds());

	std::unique_lock<std::mutex> lock(instance->databasesMutex);

	// A secondary workspace belongs to exactly one secondary instance (RocksDB
	// does not enforce this itself — see db_descriptor.h::secondaryLockToken).
	// Same primary + same workspace shares the descriptor via the registry key;
	// a different primary on the same workspace is rejected.
	auto rejectConflictingSecondaryWorkspace = [&]() {
		if (options.secondaryPath.empty()) {
			return;
		}
		for (const auto& [existingKey, existingEntry] : instance->databases) {
			// Both sides are resolved identities, so this compares databases, not
			// spellings: the same database + workspace shares this descriptor via
			// the key below and never reaches here.
			if (existingKey.secondaryPath == options.secondaryPath &&
				existingKey.path != identityPath) {
				// Name the other database the way its caller spelled it; the key
				// is resolved identity, which is not what an operator typed.
				const std::string& conflicting = existingEntry.descriptor
					? existingEntry.descriptor->path
					: existingKey.path;
				throw rocksdb_js::DBException(
					"secondaryPath \"" + options.secondaryPath + "\" is already in use by database \"" +
					conflicting + "\"; each secondary instance requires its own workspace directory"
				);
			}
		}
	};

	DBKey key{identityPath, options.readOnly, options.secondaryPath};
	auto entryIterator = instance->databases.end();

	// Wait for any closing database on this path to be fully removed. The map
	// node must not be held across the wait: DestroyDB erases every entry for
	// the path, so a reference into it would dangle and its condition variable
	// would be destroyed with this thread still parked on it. Re-find the entry
	// after every wake and park on whatever condition the CURRENT entry has —
	// an entry erased and re-created while we waited carries a new condition,
	// and staying on the old one would miss its notify.
	while (true) {
		// DestroyDB erases every entry for this path before its (potentially
		// slow) physical deletion runs, to avoid holding databasesMutex across
		// that I/O -- so once physical deletion starts, no registry entry is
		// left to gate a concurrent open on. `destroyingPaths` is the gate for
		// that window: wait for it to clear before trusting the registry scan
		// below, since an empty scan at this instant could mean "free" or
		// "mid-deletion" and only this check tells them apart.
		if (instance->destroyingPaths.find(identityPath) != instance->destroyingPaths.end()) {
			DEBUG_LOG("%p DBRegistry::OpenDB Database \"%s\" is being destroyed, waiting\n", instance.get(), path.c_str());
			if (!instance->lifecycleCondition.wait_until(lock, deadline, [&]() {
				return instance->destroyingPaths.find(identityPath) == instance->destroyingPaths.end();
			})) {
				throw rocksdb_js::DBException("Timed out opening database \"" + path + "\": destruction is still in progress");
			}
			continue;
		}

		// Destroy closes every handle kind for one physical path. A new key (for
		// example, a fresh secondary workspace) must wait too, or it can open
		// during finishClose() and be deleted before it ever joined the claim.
		//
		// A quarantined entry (closeError set, not currently retrying) is
		// EXCLUDED here even though isClosing() is still true for it -- that flag
		// never resets once set, so a quarantined entry would otherwise satisfy
		// this predicate forever and this wait would never observe "no longer
		// closing", timing out instead of the quarantine check below ever
		// getting a chance to reject with its more helpful message. A retry in
		// flight (closeRetrying) is a distinct, still-transient state handled by
		// its own wait further down.
		//
		// Only ONE entry's condition is picked here, and the predicate below
		// checks only THAT entry (by key) -- not a path-wide scan. Each entry's
		// own finishClose() notifies only its own condition (see
		// closeClaimedDescriptors), so a path-wide predicate parked on one
		// entry's condition would never wake for a DIFFERENT entry's notify: two
		// closing descriptors on one path (e.g. a writable and a secondary) can
		// leave the predicate false forever while this wait sleeps on the wrong
		// condition, stalling the opener for the full deadline even though the
		// path is long since free. Waiting on one entry at a time and looping
		// back to reselect keeps the wait and its wake source the same object.
		DBKey pathClosingKey;
		std::shared_ptr<std::condition_variable> pathClosingCondition;
		for (const auto& [existingKey, existingEntry] : instance->databases) {
			if (existingKey.path == identityPath && existingEntry.descriptor &&
				existingEntry.descriptor->isClosing() &&
				(existingEntry.closeError.empty() || existingEntry.closeRetrying)
			) {
				pathClosingKey = existingKey;
				pathClosingCondition = existingEntry.condition;
				break;
			}
		}
		if (pathClosingCondition) {
			if (!pathClosingCondition->wait_until(lock, deadline, [&]() {
				auto found = instance->databases.find(pathClosingKey);
				return found == instance->databases.end() ||
					found->second.condition != pathClosingCondition ||
					!found->second.descriptor || !found->second.descriptor->isClosing() ||
					(!found->second.closeError.empty() && !found->second.closeRetrying);
			})) {
				throw rocksdb_js::DBException("Timed out opening database \"" + path + "\": another instance on this path is still closing");
			}
			continue;
		}

		// A quarantined entry (a prior close/destroy left `closeError` set) means
		// the path's last known state was not cleanly reached; opening over it
		// would silently accept whatever unflushed/partial state that close left
		// behind. Reject until an explicit shutdown()/destroy() clears it -- the
		// same guard OpenDB always applied to a still-closing descriptor, just
		// for a descriptor that stopped retrying instead of one mid-retry.
		for (const auto& [existingKey, existingEntry] : instance->databases) {
			if (existingKey.path == identityPath &&
				!existingEntry.closeError.empty() && !existingEntry.closeRetrying
			) {
				const bool destroyCleanupFailed = !existingEntry.descriptor;
				throw rocksdb_js::DBException(
					"Cannot open database \"" + path + "\": previous " +
					(destroyCleanupFailed ? "destroy cleanup" : "close") + " failed: " +
					existingEntry.closeError +
					(destroyCleanupFailed
						? ". Call destroy() to retry cleanup"
						: ". Call shutdown() to retry close, or destroy() to delete the database")
				);
			}
		}
		// A retry in flight (closeRetrying) is a transient state a fresh open
		// should wait out rather than reject, since the retry may still succeed
		// and leave the path openable. Same one-entry-at-a-time discipline as
		// the closing-condition wait above, for the same reason: each entry
		// notifies only its own condition, so a path-wide predicate parked on
		// one entry's condition can miss a different entry's retry finishing.
		DBKey retryKey;
		std::shared_ptr<std::condition_variable> retryCondition;
		for (const auto& [existingKey, existingEntry] : instance->databases) {
			if (existingKey.path == identityPath && existingEntry.closeRetrying) {
				retryKey = existingKey;
				retryCondition = existingEntry.condition;
				break;
			}
		}
		if (retryCondition) {
			if (!retryCondition->wait_until(lock, deadline, [&]() {
				auto found = instance->databases.find(retryKey);
				return found == instance->databases.end() ||
					found->second.condition != retryCondition || !found->second.closeRetrying;
			})) {
				throw rocksdb_js::DBException("Timed out opening database \"" + path + "\": close retry is still in progress");
			}
			continue;
		}

		rejectConflictingSecondaryWorkspace();
		entryIterator = instance->databases.find(key);
		if (entryIterator == instance->databases.end()) {
			entryIterator = instance->databases.emplace(key, DBRegistryEntry()).first;
			break; // no database on this path: proceed to open
		}
		auto& current = entryIterator->second;
		if (!current.descriptor) {
			break; // entry exists but holds no database
		}
		if (!current.descriptor->isClosing()) {
			break; // database exists and is not closing
		}
		DEBUG_LOG("%p DBRegistry::OpenDB Database \"%s\" is closing, waiting for removal\n", instance.get(), path.c_str());
		// Keep the descriptor visible so a spurious wake cannot reopen early.
		// Also wake (and re-loop from the top) the moment this entry quarantines
		// -- isClosing() never resets on its own, so without the closeError
		// check this predicate would otherwise wait out the full deadline
		// instead of immediately re-entering the quarantine check above.
		std::shared_ptr<std::condition_variable> condition = current.condition;
		if (!condition->wait_until(lock, deadline, [&]() {
			auto found = instance->databases.find(key);
			return found == instance->databases.end() ||
				!found->second.descriptor || !found->second.descriptor->isClosing() ||
				!found->second.closeError.empty();
		})) {
			throw rocksdb_js::DBException("Timed out opening database \"" + path + "\": the previous instance is still closing");
		}
	}

	auto& entry = entryIterator->second;

	// at this point, either:
	// 1. descriptor is set to a valid, non-closing database, or
	// 2. descriptor is nullptr (database doesn't exist)

	if (entry.descriptor) {
		// database exists and is not closing, proceed with existing logic
		// check if the database is already open with a different mode
		if (options.mode != entry.descriptor->mode) {
			throw rocksdb_js::DBException(
				"Database already open in '" +
				(entry.descriptor->mode == DBMode::Optimistic ? std::string("optimistic") : std::string("pessimistic")) +
				"' mode"
			);
		}

		// max_log_file_size and info_log_level are DB-wide (`DBOptions`) settings
		// fixed at first open; the process-global descriptor is reused across
		// handles/envs, so a second open can't change them. Reject an explicitly
		// different request rather than silently ignore it — but let a plain
		// reopen (non-explicit default / unset) inherit the live value, so a
		// default-carrying reopen after a custom first open does NOT falsely
		// reject (mirrors the compression discipline below).
		{
			rocksdb::DBOptions current = entry.descriptor->db->GetDBOptions();
			// Widen the live size_t to uint64_t rather than narrowing the request to
			// size_t: on a 32-bit build narrowing would truncate a >4GB request and
			// could falsely compare equal (skipping a real conflict).
			if (options.maxLogFileSizeExplicit &&
				static_cast<uint64_t>(current.max_log_file_size) != options.maxLogFileSize
			) {
				throw rocksdb_js::DBException(
					"Database \"" + path + "\" is already open with maxLogFileSize " +
					std::to_string(current.max_log_file_size) + " bytes; cannot reopen it with " +
					std::to_string(options.maxLogFileSize) + " bytes"
				);
			}
			if (options.infoLogLevel.has_value() &&
				static_cast<int>(current.info_log_level) != static_cast<int>(*options.infoLogLevel)
			) {
				throw rocksdb_js::DBException(
					"Database \"" + path + "\" is already open with infoLogLevel " +
					std::to_string(static_cast<int>(current.info_log_level)) + "; cannot reopen it with " +
					std::to_string(static_cast<int>(*options.infoLogLevel))
				);
			}
		}

		DEBUG_LOG("%p DBRegistry::OpenDB Database already open \"%s\"\n", instance.get(), path.c_str());
		DEBUG_LOG("%p DBRegistry::OpenDB Checking for column family \"%s\"\n", instance.get(), name.c_str());

		// manually copy the columns because we don't know which ones are valid.
		// Hold the descriptor's columns mutex across the copy-check-insert so a
		// concurrent drop (which erases its entry via unregisterColumnFamily)
		// cannot interleave and let us reuse a just-dropped column family.
		std::lock_guard<std::mutex> columnsLock(entry.descriptor->columnsMutex);
		bool columnExists = false;
		for (auto& it : entry.descriptor->columns) {
			columns[it.first] = it.second;
			if (it.first == name) {
				DEBUG_LOG("%p DBRegistry::OpenDB Column family \"%s\" already exists\n", instance.get(), name.c_str());
				columnExists = true;
			}
		}
		if (!columnExists) {
			if (entry.descriptor->readOnly) {
				throw rocksdb_js::DBException("Column family \"" + name + "\" not found: cannot create column family in read-only mode");
			}
			DEBUG_LOG("%p DBRegistry::OpenDB Creating column family \"%s\"\n", instance.get(), name.c_str());
			// Preserve retained settings while applying every per-CF option from
			// the handle creating this family. "Attached" is the descriptor's own record, not
			// RocksDB's sanitized DBOptions: SanitizeOptions fills a missing manager with a
			// disabled WriteBufferManager(0), so GetDBOptions().write_buffer_manager is never
			// null post-open and would clamp every late family regardless of whether one was
			// ever configured (#823).
			auto cfOptions = buildColumnFamilyOptions(
				options,
				entry.descriptor->attachedWriteBufferManager != nullptr,
				entry.descriptor->cfOptions
			);
			if (options.compression) {
				cfOptions.compression = *options.compression;
				cfOptions.blob_compression_type = *options.compression;
				cfOptions.compression_opts.level = options.compressionLevel
					? *options.compressionLevel
					: rocksdb::CompressionOptions::kDefaultCompressionLevel;
			}
			auto column = rocksdb_js::createRocksDBColumnFamily(
				entry.descriptor->db, name, cfOptions
			);
			auto columnDescriptor = std::make_shared<ColumnFamilyDescriptor>(
				column,
				entry.descriptor->db->GetOptions(column.get()).max_write_buffer_size_to_maintain
			);
			columns[name] = columnDescriptor;
			entry.descriptor->columns[name] = columnDescriptor;
		} else if (options.compressionExplicit && options.compression) {
			// The column family is already open in this process (the DBDescriptor
			// is process-global and shared across handles/envs). Compression is
			// fixed per column family at creation, so a second open explicitly
			// asking for a different algorithm or level cannot take effect on the
			// reused handle — reject it rather than silently ignore the request. A
			// plain reopen (compression defaulted, not explicit) inherits the live
			// setting and skips this check.
			rocksdb::ColumnFamilyHandle* cf = columns[name]->column.get();
			rocksdb::Options current = entry.descriptor->db->GetOptions(cf);
			// The effective request omitting a level is "the algorithm's default
			// level" (see applyCompression in db_descriptor.cpp), so compare against
			// the default sentinel rather than skipping the level check — otherwise
			// reopening a zstd-level-19 CF as plain zstd would silently inherit 19.
			int requestedLevel = options.compressionLevel
				? *options.compressionLevel
				: rocksdb::CompressionOptions::kDefaultCompressionLevel;
			bool algorithmDiffers = current.compression != *options.compression;
			// The request applies the algorithm to blob files too, so a live CF whose
			// blobs are at a different algorithm (e.g. a legacy CF opened plainly with
			// block=snappy but blob=none) is also a conflict — otherwise values at the
			// 2KB blob threshold would stay uncompressed while the open appears to succeed.
			bool blobDiffers = current.blob_compression_type != *options.compression;
			bool levelDiffers = current.compression_opts.level != requestedLevel;
			if (algorithmDiffers || blobDiffers || levelDiffers) {
				std::string requested = rocksdb_js::compressionNameFromType(*options.compression);
				if (options.compressionLevel) {
					requested += " (level " + std::to_string(*options.compressionLevel) + ")";
				}
				throw rocksdb_js::DBException(
					"Column family \"" + name + "\" is already open with compression \"" +
					rocksdb_js::compressionNameFromType(current.compression) + " (blob " +
					rocksdb_js::compressionNameFromType(current.blob_compression_type) + ", level " +
					std::to_string(current.compression_opts.level) + ")\"; cannot reopen it with \"" +
					requested + "\""
				);
			}
		}
	} else {
		try {
			entry.descriptor = DBDescriptor::open(path, identityPath, options);
			entry.reportedPath = entry.descriptor->path;
		} catch (...) {
			// Remove the stale entry (null descriptor) so it does not pollute the
			// registry and cause null-dereference crashes in callers such as
			// RegistryStatus that iterate every entry without guarding for null.
			instance->databases.erase(entryIterator);
			throw;
		}
		DEBUG_LOG("%p DBRegistry::OpenDB Stored DBDescriptor %p for \"%s\" (ref count = %ld)\n", instance.get(), entry.descriptor.get(), path.c_str(), entry.descriptor.use_count());
		columns = entry.descriptor->columns;
	}

	// handle the column family
	std::shared_ptr<ColumnFamilyDescriptor> columnDescriptor;
	auto colIterator = columns.find(name);
	if (colIterator != columns.end()) {
		// column family already exists
		DEBUG_LOG("%p DBRegistry::OpenDB Column family \"%s\" found\n", instance.get(), name.c_str());
		columnDescriptor = colIterator->second;
	} else {
		// use the default column family
		DEBUG_LOG("%p DBRegistry::OpenDB Column family \"%s\" not found, using \"default\"\n", instance.get(), name.c_str());
		columnDescriptor = columns[rocksdb::kDefaultColumnFamilyName];
	}

	const uint64_t verificationTableDbId = entry.descriptor->vtEpoch;
	const uint32_t verificationTableColumnFamilyId = columnDescriptor->column->GetID();

	// The registry lock is the lifecycle linearization point: reset is ordered
	// after every wait above, and teardown cannot claim this descriptor between
	// publishing the handle's native state and making it visible in closables.
	handle->resetCancelled();
	handle->compactCancelRequested.store(false);
	entry.descriptor->attach(handle);

	handle->columnDescriptor = std::move(columnDescriptor);
	handle->descriptor = entry.descriptor;
	handle->verificationTableDbId = verificationTableDbId;
	handle->verificationTableColumnFamilyId = verificationTableColumnFamilyId;
	handle->disableWAL = options.disableWAL;
	handle->enableVerificationTable = options.verificationTable;

	DEBUG_LOG("%p DBRegistry::OpenDB Attached DBHandle %p for \"%s\" (ref count = %ld)\n",
		instance.get(), handle.get(), path.c_str(), entry.descriptor.use_count());
}

/**
 * Purge expired database descriptors from the registry.
 */
void DBRegistry::PurgeAll() {
	if (instance) {
		std::vector<ClosingDescriptor> descriptorsToClose;
		std::vector<std::shared_ptr<std::condition_variable>> removedConditions;
		std::exception_ptr closeError;
		{
			std::lock_guard<std::mutex> lock(instance->databasesMutex);
#ifdef DEBUG
			size_t initialSize = instance->databases.size();
			DEBUG_LOG("%p DBRegistry::PurgeAll Purging %zu databases:\n", instance.get(), initialSize);
#endif
			descriptorsToClose.reserve(instance->databases.size());
			for (auto it = instance->databases.begin(); it != instance->databases.end();) {
				if (!it->second.closeError.empty()) {
					// Quarantined: leave it for shutdown()/destroy() to retry.
					++it;
					continue;
				}
				auto descriptor = it->second.descriptor;
				if (descriptor) {
					if (!descriptor->beginClose()) {
						++it;
						continue;
					}
					DEBUG_LOG("%p DBRegistry::PurgeAll Claiming \"%s\" (ref count = %ld)\n",
						instance.get(), it->first.path.c_str(), descriptor.use_count());
					descriptorsToClose.emplace_back(it->first, descriptor, it->second.condition);
					++it;
					continue;
				}
				removedConditions.push_back(it->second.condition);
				it = instance->databases.erase(it);
			}
#ifdef DEBUG
			DEBUG_LOG("%p DBRegistry::PurgeAll Claimed %zu of %zu descriptors\n",
				instance.get(), descriptorsToClose.size(), initialSize);
#endif
		}
		for (const auto& condition : removedConditions) {
			condition->notify_all();
		}

		closeError = closeClaimedDescriptors(
			descriptorsToClose, ClaimedCloseOptions{}, instance->databases, instance->databasesMutex);
		if (closeError) {
			std::rethrow_exception(closeError);
		}
	}
}

/**
 * Get the status of the database registry.
 *
 * @param env - The environment of the Node.js process.
 * @param info - The callback info.
 * @return A JavaScript object with the database registry status.
 */
napi_value DBRegistry::RegistryStatus(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_array(env, &result));

	struct ColumnSummary {
		std::string name;
		uint32_t userSharedBuffers;
	};
	struct TxnSummary {
		uint32_t id;
		double ageMs;
	};
	struct RegistryStatusEntry {
		std::string reportedPath;
		std::string closeError;
		bool closeRetrying = false;
		bool hasDescriptor = false;
		bool optimistic = false;
		uint32_t refCount = 0;
		std::vector<ColumnSummary> columnSummaries;
		std::vector<TxnSummary> txnSummaries;
		uint32_t closables = 0;
		uint32_t locks = 0;
		uint32_t listenerCallbacks = 0;
	};
	std::vector<RegistryStatusEntry> entries;
	if (instance) {
		std::unique_lock<std::mutex> lock(instance->databasesMutex);
		entries.reserve(instance->databases.size());
		for (const auto& [key, entry] : instance->databases) {
			DBDescriptor* descriptor = entry.descriptor.get();
			RegistryStatusEntry statusEntry;
			statusEntry.reportedPath = descriptor ? descriptor->path
				: !entry.reportedPath.empty() ? entry.reportedPath
				: key.path;
			statusEntry.closeError = entry.closeError;
			statusEntry.closeRetrying = entry.closeRetrying;
			if (descriptor) {
				statusEntry.hasDescriptor = true;
				statusEntry.optimistic = descriptor->mode == DBMode::Optimistic;
				statusEntry.refCount = static_cast<uint32_t>(entry.descriptor.use_count());
				// Snapshot the column families under columnsMutex, then build the JS
				// object outside it -- the same shape as the transaction snapshot
				// below, and for two reasons. databasesMutex does not alone cover this
				// map: dropSync() can erase from it while this walk is in flight, and
				// name.c_str() would then point into a freed map node. The child mutex
				// also must not be held across N-API calls, which can run a finalizer.
				{
					std::lock_guard<std::mutex> columnsLock(descriptor->columnsMutex);
					statusEntry.columnSummaries.reserve(descriptor->columns.size());
					const int columnsDelayMs =
						registryStatusColumnsDelayMsFlag().load(std::memory_order_relaxed);
					for (const auto& [name, columnDescriptor] : descriptor->columns) {
						if (!columnDescriptor) {
							continue;
						}
						if (columnsDelayMs > 0) {
							std::this_thread::sleep_for(std::chrono::milliseconds(columnsDelayMs));
						}
						std::lock_guard<std::mutex> buffersLock(columnDescriptor->userSharedBuffersMutex);
						statusEntry.columnSummaries.push_back(
							ColumnSummary{name, static_cast<uint32_t>(columnDescriptor->userSharedBuffers.size())}
						);
					}
				}
				// txnsMutex covers map membership, not a handle's mutable fields. id
				// and createdAt are fixed before the handle is published.
				{
					auto now = std::chrono::steady_clock::now();
					std::lock_guard<std::mutex> txnsLock(descriptor->txnsMutex);
					statusEntry.txnSummaries.reserve(descriptor->transactions.size());
					for (auto& [txnId, txnHandle] : descriptor->transactions) {
						if (!txnHandle) {
							continue;
						}
						statusEntry.txnSummaries.push_back({
							txnId,
							std::chrono::duration<double, std::milli>(now - txnHandle->createdAt).count()
						});
					}
					statusEntry.closables = static_cast<uint32_t>(descriptor->closables.size());
				}
				// locksMutex is required even for the count because teardown erases
				// entries through lockReleaseByOwner().
				{
					std::lock_guard<std::mutex> locksLock(descriptor->locksMutex);
					statusEntry.locks = static_cast<uint32_t>(descriptor->locks.size());
				}
				statusEntry.listenerCallbacks = static_cast<uint32_t>(descriptor->events.size());
			}
			entries.push_back(std::move(statusEntry));
		}
		lock.unlock();

		size_t i = 0;
		for (auto& entry : entries) {
			napi_value database;
			NAPI_STATUS_THROWS(::napi_create_object(env, &database));
			napi_value pathValue;
			// The descriptor's path, not the key's, when we have one: the key is
			// resolved identity, and a caller matching this against the path it
			// opened would miss wherever the two spell the same directory
			// differently. A tombstoned entry (destroy cleanup failed) has no
			// descriptor, so fall back to the spelling its last descriptor was
			// opened with, and only then to the key's resolved identity.
			NAPI_STATUS_THROWS(::napi_create_string_utf8(
				env, entry.reportedPath.c_str(), entry.reportedPath.size(), &pathValue));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "path", pathValue));
			if (!entry.closeError.empty()) {
				napi_value closeErrorValue;
				NAPI_STATUS_THROWS(::napi_create_string_utf8(
					env,
					entry.closeError.c_str(),
					entry.closeError.size(),
					&closeErrorValue
				));
				NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "closeError", closeErrorValue));
			}
			if (entry.closeRetrying) {
				napi_value closeRetryingValue;
				NAPI_STATUS_THROWS(::napi_get_boolean(env, true, &closeRetryingValue));
				NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "closeRetrying", closeRetryingValue));
			}
			if (!entry.hasDescriptor) {
				napi_value pending;
				NAPI_STATUS_THROWS(::napi_get_boolean(env, true, &pending));
				NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "destroyCleanupPending", pending));
				napi_value zero;
				NAPI_STATUS_THROWS(::napi_create_uint32(env, 0, &zero));
				for (const char* property : {"refCount", "transactions", "closables", "locks", "listenerCallbacks"}) {
					NAPI_STATUS_THROWS(::napi_set_named_property(env, database, property, zero));
				}
				napi_value columnFamilies;
				NAPI_STATUS_THROWS(::napi_create_object(env, &columnFamilies));
				NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "columnFamilies", columnFamilies));
				// A monitor reading `entry.transactionDetails.length` must not throw
				// on the one entry shape that only appears when a destroy's
				// physical cleanup failed. (`RegistryStatusDB` also declares
				// `userSharedBuffers` non-optional and types `columnFamilies` as
				// `string[]`; neither matches what any branch here builds, which
				// predates this change and wants its own fix.)
				napi_value transactionDetails;
				NAPI_STATUS_THROWS(::napi_create_array(env, &transactionDetails));
				NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "transactionDetails", transactionDetails));
				NAPI_STATUS_THROWS(::napi_set_element(env, result, i++, database));
				continue;
			}
			napi_value modeValue;
			std::string mode = entry.optimistic ? "optimistic" : "pessimistic";
			NAPI_STATUS_THROWS(::napi_create_string_utf8(env, mode.c_str(), mode.size(), &modeValue));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "mode", modeValue));
			napi_value refCount;
			NAPI_STATUS_THROWS(::napi_create_uint32(env, entry.refCount, &refCount));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "refCount", refCount));
			napi_value columnFamilies;
			NAPI_STATUS_THROWS(::napi_create_object(env, &columnFamilies));
			for (const auto& column : entry.columnSummaries) {
				napi_value columnDescriptorValue;
				NAPI_STATUS_THROWS(::napi_create_object(env, &columnDescriptorValue));

				napi_value userSharedBuffers;
				NAPI_STATUS_THROWS(::napi_create_uint32(env, column.userSharedBuffers, &userSharedBuffers));
				NAPI_STATUS_THROWS(::napi_set_named_property(env, columnDescriptorValue, "userSharedBuffers", userSharedBuffers));

				NAPI_STATUS_THROWS(::napi_set_named_property(env, columnFamilies, column.name.c_str(), columnDescriptorValue));
			}
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "columnFamilies", columnFamilies));
			napi_value transactions;
			NAPI_STATUS_THROWS(::napi_create_uint32(env, static_cast<uint32_t>(entry.txnSummaries.size()), &transactions));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "transactions", transactions));
			napi_value transactionDetails;
			NAPI_STATUS_THROWS(::napi_create_array(env, &transactionDetails));
			for (size_t t = 0; t < entry.txnSummaries.size(); t++) {
				const auto& summary = entry.txnSummaries[t];
				napi_value detail;
				NAPI_STATUS_THROWS(::napi_create_object(env, &detail));
				napi_value value;
				NAPI_STATUS_THROWS(::napi_create_uint32(env, summary.id, &value));
				NAPI_STATUS_THROWS(::napi_set_named_property(env, detail, "id", value));
				NAPI_STATUS_THROWS(::napi_create_double(env, summary.ageMs, &value));
				NAPI_STATUS_THROWS(::napi_set_named_property(env, detail, "ageMs", value));
				NAPI_STATUS_THROWS(::napi_set_element(env, transactionDetails, static_cast<uint32_t>(t), detail));
			}
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "transactionDetails", transactionDetails));
			napi_value closables;
			NAPI_STATUS_THROWS(::napi_create_uint32(env, entry.closables, &closables));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "closables", closables));
			napi_value locks;
			NAPI_STATUS_THROWS(::napi_create_uint32(env, entry.locks, &locks));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "locks", locks));
			napi_value listenerCallbacks;
			NAPI_STATUS_THROWS(::napi_create_uint32(env, entry.listenerCallbacks, &listenerCallbacks));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, database, "listenerCallbacks", listenerCallbacks));
			NAPI_STATUS_THROWS(::napi_set_element(env, result, i, database));
			i++;
		}
	}

	return result;
}

/**
 * Close each descriptor's transactions owned by handles created on the given
 * env. Called from the module env-cleanup hook, on the dying env's own thread,
 * before Node frees the env — so a worker that exits with a pending
 * transaction does not leak it into the process-global descriptor for a later
 * Shutdown to walk with a dangling env (HarperFast/rocksdb-js#741). Mirrors
 * RemoveListenersByEnv: snapshot descriptors under databasesMutex, close
 * outside the lock.
 */
void DBRegistry::CloseTransactionsByEnv(napi_env env) {
	if (!instance) {
		return;
	}

	std::vector<std::shared_ptr<DBDescriptor>> descriptors;
	{
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		descriptors.reserve(instance->databases.size());
		for (auto& [_key, entry] : instance->databases) {
			if (entry.descriptor) {
				descriptors.push_back(entry.descriptor);
			}
		}
	}

	for (auto& descriptor : descriptors) {
		descriptor->closeTransactionsByEnv(env);
	}
}

/**
 * Scrub per-descriptor event listeners owned by the given env. Called from the
 * env-cleanup hook so a worker thread exiting does not leave threadsafe-fn
 * pointers in shared descriptors that the main thread (or a surviving worker)
 * would later dereference via notify().
 *
 * Snapshots the descriptors under databasesMutex, then drops the lock before
 * calling into each EventEmitter. This keeps the registry lock window short;
 * RegistryStatus is the path that establishes the documented
 * databasesMutex -> events.mutex ordering.
 */
void DBRegistry::RemoveListenersByEnv(napi_env env) {
	if (!instance) {
		return;
	}

	std::vector<std::shared_ptr<DBDescriptor>> descriptors;
	{
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		descriptors.reserve(instance->databases.size());
		for (auto& [_key, entry] : instance->databases) {
			if (entry.descriptor) {
				descriptors.push_back(entry.descriptor);
			}
		}
	}

	for (auto& descriptor : descriptors) {
		descriptor->removeListenersByEnv(env);
	}
}

/**
 * Release each descriptor's commit-completion tsfn owned by the given env.
 * Called from the module env-cleanup hook so a worker thread exiting does not
 * leave a threadsafe-fn the shared commit thread would later call into a
 * torn-down env. Mirrors RemoveListenersByEnv: snapshot the descriptors under
 * databasesMutex, then release outside the lock (releaseCommitCompletionsByEnv
 * takes each descriptor's own commitMutex).
 */
void DBRegistry::ReleaseCommitCompletionsByEnv(napi_env env) {
	if (!instance) {
		return;
	}

	std::vector<std::shared_ptr<DBDescriptor>> descriptors;
	{
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		descriptors.reserve(instance->databases.size());
		for (auto& [_key, entry] : instance->databases) {
			if (entry.descriptor) {
				descriptors.push_back(entry.descriptor);
			}
		}
	}

	for (auto& descriptor : descriptors) {
		descriptor->releaseCommitCompletionsByEnv(env);
	}
}

/**
 * Release each attached DBHandle's `logRefs` for handles created on the given
 * env. Called from the module env-cleanup hook, on the dying env's own
 * thread, before Node frees the env -- so a worker that exits without
 * closing its DBHandle does not leave a `napi_ref` for a later foreign
 * `close()` to touch through a recycled `std::thread::id` (AGENTS.md
 * invariant 18). Mirrors RemoveListenersByEnv.
 */
void DBRegistry::ReleaseLogRefsByEnv(napi_env env) {
	if (!instance) {
		return;
	}

	std::vector<std::shared_ptr<DBDescriptor>> descriptors;
	{
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		descriptors.reserve(instance->databases.size());
		for (auto& [_key, entry] : instance->databases) {
			if (entry.descriptor) {
				descriptors.push_back(entry.descriptor);
			}
		}
	}

	for (auto& descriptor : descriptors) {
		descriptor->releaseLogRefsByEnv(env);
	}
}

/**
 * Cancels each descriptor's pending park timeouts owned by the given env.
 * Called from the module env-cleanup hook so a worker thread exiting does not
 * leave a coordinated-retry park's timeout thread calling into a torn-down
 * env ~ROCKSDB_JS_PARK_TIMEOUT_MS later. Mirrors ReleaseCommitCompletionsByEnv.
 */
void DBRegistry::ReleaseParkTimeoutsByEnv(napi_env env) {
	if (!instance) {
		return;
	}

	std::vector<std::shared_ptr<ParkTimeoutRegistry>> registries;
	{
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		registries.reserve(instance->databases.size());
		for (auto& [_key, entry] : instance->databases) {
			if (entry.descriptor) {
				registries.push_back(entry.descriptor->parkTimeouts);
			}
		}
	}

	for (auto& registry : registries) {
		registry->releaseByEnv(env);
	}
}

/**
 * Shutdown will force all databases to flush in-memory data to disk and purge the registry.
 */
void DBRegistry::Shutdown() {
	if (instance) {
		// One budget per wait, not one for the whole shutdown; see DestroyDB.
		const auto waitBudget =
			std::chrono::seconds(DBSettings::getInstance().getLifecycleWaitSeconds());
		std::unique_lock<std::timed_mutex> shutdownLock(instance->shutdownMutex, std::defer_lock);
		if (!shutdownLock.try_lock_until(std::chrono::steady_clock::now() + waitBudget)) {
			throw rocksdb_js::DBException("Timed out waiting for another database shutdown to finish");
		}

		while (true) {
			std::vector<ClosingDescriptor> descriptorsToClose;
			std::vector<ClosingDescriptor> descriptorsToWaitFor;
			bool destroysInFlight;
			{
				std::unique_lock<std::mutex> lock(instance->databasesMutex);
				// A path mid-DestroyDB's physical deletion has no registry entry
				// left (DestroyDB erases them before that runs) for the scan below
				// to find, so it would otherwise look identical to "nothing left to
				// do" and let shutdown() return while files are still being
				// removed. Wait it out here instead.
				destroysInFlight = !instance->destroyingPaths.empty();
				DEBUG_LOG("%p DBRegistry::Shutdown Shutting down %zu databases\n", instance.get(), instance->databases.size());
				descriptorsToClose.reserve(instance->databases.size());
				descriptorsToWaitFor.reserve(instance->databases.size());

				for (auto& [key, entry] : instance->databases) {
					if (!entry.descriptor) {
						// A prior destroy() left a tombstone (descriptor cleared,
						// closeError set) after its physical cleanup failed. That
						// failure was already surfaced via database:closeFailed and
						// stays visible in registryStatus().destroyCleanupPending.
						// shutdown() is deliberately non-destructive -- only an
						// explicit destroy() retries path deletion -- so skip it
						// here rather than re-throwing the same error forever.
						continue;
					}
					ClosingDescriptor closing{key, entry.descriptor, entry.condition};
					if (!entry.closeError.empty() && !entry.closeRetrying) {
						entry.closeRetrying = true;
						descriptorsToClose.push_back(std::move(closing));
					} else if (entry.closeError.empty() && entry.descriptor->beginClose()) {
						descriptorsToClose.push_back(std::move(closing));
					} else {
						descriptorsToWaitFor.push_back(std::move(closing));
					}
				}
			}

			std::exception_ptr closeError = closeClaimedDescriptors(
				descriptorsToClose, ClaimedCloseOptions{}, instance->databases, instance->databasesMutex);

			for (const auto& closing : descriptorsToWaitFor) {
				std::unique_lock<std::mutex> lock(instance->databasesMutex);
				const auto drainDeadline = std::chrono::steady_clock::now() + waitBudget;
				if (!closing.condition->wait_until(lock, drainDeadline, [&]() {
					auto entry = instance->databases.find(closing.key);
					return entry == instance->databases.end() ||
						entry->second.descriptor != closing.descriptor ||
						(!entry->second.closeError.empty() && !entry->second.closeRetrying);
				})) {
					throw rocksdb_js::DBException("Timed out waiting for database close to finish during shutdown");
				}
			}

			if (destroysInFlight) {
				std::unique_lock<std::mutex> lock(instance->databasesMutex);
				const auto destroyDeadline = std::chrono::steady_clock::now() + waitBudget;
				if (!instance->lifecycleCondition.wait_until(lock, destroyDeadline, [&]() {
					return instance->destroyingPaths.empty();
				})) {
					throw rocksdb_js::DBException("Timed out waiting for database destruction to finish during shutdown");
				}
				if (closeError) std::rethrow_exception(closeError);
				continue;
			}
			if (closeError) std::rethrow_exception(closeError);
			if (descriptorsToClose.empty() && descriptorsToWaitFor.empty()) break;
		}

		// Purge any remaining unreferenced-but-untouched entries (e.g. one whose
		// last handle detached between two passes above).
		PurgeAll();

		DEBUG_LOG("%p DBRegistry::Shutdown Shutdown complete\n", instance.get());
	}
}

/**
 * Release every remaining registry entry (see the header for why this cannot be
 * left to the singleton's static destructor).
 */
void DBRegistry::Teardown() {
	if (!instance) {
		return;
	}

	std::unordered_map<DBKey, DBRegistryEntry, DBKeyHash> entries;
	{
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		entries.swap(instance->databases);
	}

	// Destroy outside the lock: ~DBDescriptor closes the RocksDB database and
	// joins its worker threads. A descriptor already claimed by a failed close
	// short-circuits its own close() and is simply released here.
	DEBUG_LOG("%p DBRegistry::Teardown Releasing %zu remaining descriptor(s)\n",
		instance.get(), entries.size());
	entries.clear();
}

/**
 * Get the number of databases in the registry.
 */
size_t DBRegistry::Size() {
	if (instance) {
		std::lock_guard<std::mutex> lock(instance->databasesMutex);
		return instance->databases.size();
	}
	return 0;
}

} // namespace rocksdb_js
