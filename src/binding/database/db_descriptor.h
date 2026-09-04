#ifndef __DB_DESCRIPTOR_H__
#define __DB_DESCRIPTOR_H__

#include <memory>
#include <node_api.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <queue>
#include <set>
#include <thread>
#include <unordered_map>
#include <functional>
#include <vector>
#include "rocksdb/db.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/options_util.h"
#include "options/db_options.h"
#include "database/commit_worker.h"
#include "transaction_log/transaction_log_store_registry.h"
#include "core/background_error.h"
#include "core/platform.h"
#include "core/write_stall_debounce.h"
#include "napi/event_emitter.h"
#include "napi/helpers.h"
#include "napi/async.h"

namespace rocksdb_js {

// forward declarations
struct ColumnFamilyDescriptor;
struct DBDescriptor;
struct DBHandle;
struct LockHandle;
struct TransactionHandle;
struct UserSharedBufferData;
struct UserSharedBufferFinalizeData;

/**
 * `writeBufferManagerAttached` must describe the database this column family belongs to, not the
 * current global setting: `writeBufferManagerSize` is mutable at runtime while a database keeps
 * whichever manager it was opened with (`write_buffer_manager` is an immutable `DBOptions` field),
 * and the retained-history clamp it drives is fixed when the family is created. See
 * `resolveMaxWriteBufferSizeToMaintain`.
 */
rocksdb::ColumnFamilyOptions buildColumnFamilyOptions(
	const DBOptions& options,
	bool writeBufferManagerAttached,
	rocksdb::ColumnFamilyOptions cfOptions = {}
);

/**
 * Custom deleter for RocksDB that waits for any background compaction to
 * complete before destroying the database instance. Compaction is triggered
 * by DBDescriptor::close() before this deleter runs.
 */
struct DBDeleter {
	void operator()(rocksdb::DB* db) const {
		if (db) {
			DEBUG_LOG("DBDeleter::operator() Waiting for compaction and closing database\n");
			// Wait for any background compaction to complete and close the database
			rocksdb::WaitForCompactOptions options;
			options.close_db = true;
			db->WaitForCompact(options);
			DEBUG_LOG("DBDeleter::operator() Closed database, deleting\n");
			delete db;
			DEBUG_LOG("DBDeleter::operator() Deleted database\n");
		}
	}
};

/**
 * Bounded waits for coordinated-retry commits parked on a conflicting holder's
 * VT lock, so a holder that never releases resolves RETRY_NOW instead of
 * parking forever (harper#2001, see AGENTS.md note 12). One instance per
 * `DBDescriptor`, with one lazily-started thread tracking every outstanding
 * deadline.
 *
 * Deliberately standalone — it holds no reference back to its descriptor, and
 * nothing reachable from `fire()` touches the descriptor or `DBRegistry`.
 * `fire()` is called from a `LockTracker` wake callback, which
 * `LockTracker::wake()` invokes inline while the *process-global* VT
 * `writerMutex_` is held (`VerificationTable::releaseWriteIntent` and
 * `cancelForDB` both wake under it), so the standing invariant on that path is:
 * do not block, and do not re-enter the VT or the registry. A
 * `DBRegistry::PurgeIfUnreferenced` from there can claim the purge and run
 * `finishClose()` -> `cancelForDB()` -> a second lock of that same
 * non-recursive `writerMutex_`, wedging every database's write-intent path.
 * Holding the wake closure's `weak_ptr` on this object rather than on the
 * descriptor is also what keeps its transient `.lock()` out of the
 * descriptor's `use_count`, which `PurgeIfUnreferenced` keys its "no handles
 * left" decision on — so there is no purge-skip window to retry in the first
 * place (the HarperFast/rocksdb-js#672 hazard).
 *
 * Parks are keyed by a monotonic `id`, not the entry's address:
 * `LockTracker::wakeCallbacks` has no removal API, so a stale closure can
 * outlive its entry and an address-keyed lookup could resolve a later,
 * unrelated park that reused the freed address. `fired` is the exactly-once
 * gate shared with that park's wake callback — whichever side wins the CAS
 * calls+releases `tsfn`, always under `mutex` so a concurrent `releaseByEnv()`
 * for a dying env cannot observe "nothing to cancel" while the other side is
 * mid-call on that env's tsfn.
 */
class ParkTimeoutRegistry final {
public:
	~ParkTimeoutRegistry();

	/**
	 * JS thread (`completeCommitWork`). Registers a bounded wait, lazily
	 * starting the single timeout thread. Returns the new park's id, or 0 if
	 * the registry is shut down or thread creation failed -- either way the
	 * caller resolves inline instead of parking with no timeout behind it.
	 */
	uint64_t schedule(
		napi_env env,
		unsigned timeoutMs,
		napi_threadsafe_function tsfn,
		std::shared_ptr<std::atomic<bool>> fired
	);

	/**
	 * Resolves a specific park early because its VT lock's holder released
	 * (LockTracker wake callback, any thread, VT `writerMutex_` held). A no-op
	 * if the id is already gone -- claimed by the timeout thread, by
	 * `releaseByEnv`, or drained by `shutdown`.
	 */
	void fire(uint64_t id);

	/**
	 * Module env-cleanup hook. Cancels every pending park registered for a
	 * dying env -- released, never called, so neither the timeout thread nor a
	 * later real wake can fire into a tsfn Node is about to free.
	 */
	void releaseByEnv(napi_env env);

	/**
	 * Descriptor close: stop and join the timeout thread, then resolve
	 * (call+release) every park still pending. `cancelForDB`, called just
	 * before this, cannot be relied on to have woken everything -- a park can
	 * be registered on a foreign-`dbId` tracker (colliding VT slot) that only
	 * wakes a different database. Idempotent (called from both `finishClose()`
	 * and the descriptor's destructor, matching `commitWorker`).
	 */
	void shutdown();

private:
	using DeadlineIndex = std::multimap<std::chrono::steady_clock::time_point, uint64_t>;

	struct ParkTimeout {
		uint64_t id;
		napi_env env;
		napi_threadsafe_function tsfn;
		std::shared_ptr<std::atomic<bool>> fired;
		DeadlineIndex::iterator deadlineIt;
	};

	/** Detaches `id` from both indexes; null if already claimed. Holds `mutex`. */
	std::unique_ptr<ParkTimeout> take(uint64_t id);

	/**
	 * Calls+releases a claimed park's tsfn, unless another side already won the
	 * exactly-once gate. Every caller holds `mutex`.
	 */
	static void resolve(ParkTimeout& park);

	/** Runs on `thread` until `shutdown()` stops it. */
	void runLoop();

	std::mutex mutex;
	std::condition_variable cv;
	std::unordered_map<uint64_t, std::unique_ptr<ParkTimeout>> parks;
	// Deadline-ordered view of `parks`: the timeout thread needs the earliest
	// deadline on every wakeup, and scanning for it under `mutex` would put an
	// O(N) loop on the same lock `fire()` must take while holding the global VT
	// `writerMutex_`.
	DeadlineIndex deadlines;
	uint64_t nextId = 1;
	std::thread thread;
	bool threadStarted = false;
	bool stopped = false;
};

/**
 * Descriptor for a RocksDB database, its column families, and any in-flight
 * transactions. The DBRegistry uses this to track active databases and reuse
 * RocksDB instances.
 */
struct DBDescriptor final : public std::enable_shared_from_this<DBDescriptor> {
	/**
	 * The path of the database.
	 */
	std::string path;

	/**
	 * Process-unique identity for this descriptor's *open lifecycle*, used as the
	 * database component of every VerificationTable slot address (with cfId + key).
	 *
	 * The descriptor heap pointer is NOT usable for this: it is freed on close and
	 * routinely re-used by the allocator on the next reopen of the same path, while
	 * RocksDB keeps cfId stable across reopens. That let a version cached by a prior
	 * in-process incarnation be addressed — and trusted (spurious FRESH) — by the
	 * next one, resolving present keys as stale/absent until the slots settled
	 * (HarperFast/harper#1864). A monotonic per-open epoch is unique across the whole
	 * process lifetime, so a reopened DB can never collide with a prior incarnation's
	 * slots regardless of address reuse. It is only ever compared for equality (slot
	 * hashing, cancelForDB), never dereferenced.
	 */
	const uint64_t vtEpoch;

	/**
	 * The mode of the database: optimistic or pessimistic. `DBRegistry`
	 * defaults this to `DBMode::Optimistic`.
	 */
	DBMode mode;

	/**
	 * Whether the database was opened in readonly mode via
	 * `DB::OpenForReadOnly`. When true, write operations are not supported.
	 */
	bool readOnly;

	/**
	 * Base column family options retained from `DB::Open`. Families created
	 * later preserve these table/blob settings while applying the current
	 * handle's per-CF memory options.
	 */
	rocksdb::ColumnFamilyOptions cfOptions;

	/**
	 * The RocksDB database instance.
	 */
	std::shared_ptr<rocksdb::DB> db;

	/**
	 * Map of column family name to column family handle.
	 */
	std::unordered_map<std::string, std::shared_ptr<ColumnFamilyDescriptor>> columns;

	/**
	 * Mutex to protect the columns map. Column families can be unregistered on
	 * drop (see `unregisterColumnFamily`) while other threads iterate the map:
	 * the JS thread via the `columns` getter or `DBRegistry::OpenDB`, libuv
	 * worker threads via `flush()`, and a closing thread via `close()`. Lock
	 * ordering: when both are held, `DBRegistry::databasesMutex` is acquired
	 * BEFORE `columnsMutex`; `columnsMutex` is never held while acquiring the
	 * registry mutex.
	 */
	std::mutex columnsMutex;

	/**
	 * The RocksDB statistics instance.
	 */
	std::shared_ptr<rocksdb::Statistics> statistics;

	/**
	 * Map of transaction id to transaction handle.
	 */
	std::unordered_map<uint32_t, std::shared_ptr<TransactionHandle>> transactions;

	/**
	 * Atomic counter for generating unique transaction IDs for this RocksDB
	 * instance. Sadly we cannot use RocksDB's transaction IDs because they are
	 * implementation-dependent and are assigned lazily with a default of 0
	 * causing collisions in the transactions map.
	 */
	std::atomic<uint32_t> nextTransactionId{1};

	/**
	 * Mutex to protect the transactions map and closables set.
	 */
	std::mutex txnsMutex;

	/**
	 * Set of closables to be closed when the descriptor is closed.
	 */
	std::map<Closable*, std::weak_ptr<Closable>> closables;

	/**
	 * Mutex to protect the locks map.
	 */
	std::mutex locksMutex;

	/**
	 * Map of lock key to lock handle.
	 */
	std::unordered_map<std::string, std::shared_ptr<LockHandle>> locks;

	/**
	 * A flag used by the `DBRegistry` to indicate the database is being closed,
	 * this descriptor should not be used, and it should create a new
	 * descriptor.
	 */
	std::atomic<bool> closing{false};

	/**
	 * Counter tracking in-flight database operations. close() uses
	 * atomic::wait() to block until this reaches zero.
	 */
	std::atomic<uint32_t> operationsInFlight{0};

	/**
	 * Mutex to prevent concurrent compaction operations.
	 */
	std::mutex compactMutex;

	/**
	 * Per-database event emitter. Listeners attached here only fire for events
	 * emitted on this descriptor. Cleaned up per-DBHandle on close and fully
	 * cleared when the descriptor itself closes.
	 */
	EventEmitter events;

	/**
	 * The most recent background error, serialized to a JSON string
	 * (`backgroundErrorToJson`), or empty when none has occurred. Stored as a
	 * plain string — not any N-API value — so `OnBackgroundError` can write it
	 * from a RocksDB background thread with no `napi_env` involved; the JS thread
	 * reconstructs a `BackgroundError` from it on demand (`getLastError()`) and
	 * when emitting the `'error'` event. Guarded by `lastErrorMutex`. Purely
	 * historical: it is NOT cleared by `resume()` (see HarperFast/rocksdb-js#730).
	 */
	std::mutex lastErrorMutex;
	std::string lastError;

	/**
	 * Stores the latest serialized background error AND, for a non-empty `json`,
	 * emits the per-database `'error'` event with the reconstructed
	 * `BackgroundError`. An empty `json` is a silent reset (no event) — the
	 * clear path behind `db.setLastError(null)`. Safe to call from a RocksDB
	 * background thread (store) or the JS thread; the emit is dispatched
	 * asynchronously via the thread-safe emitter.
	 */
	void setLastError(std::string json);

	/** Returns the latest serialized background error, or empty when none (JS thread). */
	std::string getLastError();

	/**
	 * Emits the per-database `'writeStall'` event when a column family's RocksDB
	 * write-stall condition changes. Listeners receive three string args:
	 * `(columnFamily, previousCondition, currentCondition)` where each condition
	 * is `'normal' | 'delayed' | 'stopped'`. Safe to call from a RocksDB
	 * background thread; the emit is dispatched asynchronously via the
	 * thread-safe emitter, and is a no-op when there are no listeners.
	 */
	void emitWriteStall(
		const std::string& columnFamily,
		rocksdb::WriteStallCondition previous,
		rocksdb::WriteStallCondition current
	);

	/**
	 * Per-column-family debounce for the `'writeStall'` event (rising-edge,
	 * rate-limited; see `core/write_stall_debounce.h`). The window is resolved once
	 * on the JS thread at construction into `writeStallDebounceWindowMs`, so the
	 * emit path — a RocksDB background thread — never calls `::getenv`.
	 */
	WriteStallDebounce writeStallDebounce;
	uint64_t writeStallDebounceWindowMs = 0;

	/**
	 * Commit lanes executing async transaction commits off the libuv
	 * threadpool, shared by all envs/handles on this database. In the default
	 * single-lane mode only commitWorker runs: each commit executes its log
	 * write and RocksDB commit back to back in dispatch order (logWorker is
	 * never started). In two-lane mode (ROCKSDB_JS_COMMIT_THREAD=2) the log
	 * lane writes the transaction-log batch (a pass-through no-op for txns
	 * with no log entries, preserving total order), then forwards to the
	 * commit lane, letting the stages overlap across transactions while each
	 * lane preserves order — see commitThreadMode() in transaction.cpp and
	 * CommitWorker for the rationale.
	 *
	 * Declared commit-lane-first so member destruction (reverse order) tears
	 * down the log lane before the commit lane it feeds; finishClose() shuts
	 * both down explicitly in that order first.
	 */
	CommitWorker commitWorker{"rocksdb-commit"};
	CommitWorker logWorker{"rocksdb-txnlog"};

	/**
	 * Per-env commit-completion plumbing. The commit thread is shared across
	 * every env that opened this database, but each async commit's completion
	 * must run on the env that issued it — so completions are marshalled back
	 * via a threadsafe function created lazily per env.
	 *
	 * `commitMutex` guards both the commit thread's tsfn call
	 * (`dispatchCommitCompletion`) and the release of an env's tsfn
	 * (`releaseCommitCompletionsByEnv`, run from the module env-cleanup hook
	 * when a worker env exits). Making the call while holding the mutex is what
	 * keeps it safe against env teardown: a dying env's cleanup hook must take
	 * the same mutex to release, and Node runs that hook before freeing the
	 * env's tsfns — so the tsfn cannot be freed mid-call. This is the same
	 * discipline `EventEmitter::notify` uses (HarperFast/harper#1370). A
	 * per-commit `napi_acquire_threadsafe_function` does NOT close this window
	 * (env teardown does not honor the tsfn-level acquire count).
	 */
	struct CommitCompletion {
		napi_threadsafe_function tsfn = nullptr;
		// In-flight commits for this env; drives ref/unref so the event loop is
		// kept alive until completions run, but can still exit when idle.
		uint32_t pending = 0;
	};
	std::mutex commitMutex;
	std::unordered_map<napi_env, CommitCompletion> commitCompletions;
	// Set (under commitMutex) by finishClose()'s release pass. Blocks any
	// later registerCommitCompletion from re-creating a tsfn that would never
	// be released (which would pin that env's event loop forever); a commit
	// racing the close falls back to the legacy libuv path instead.
	bool commitCompletionsClosed = false;

	/**
	 * JS thread. Ensures a completion tsfn exists for `env` (created with
	 * `callJs`) and accounts a newly dispatched commit, ref-ing the tsfn as the
	 * env goes from idle to busy. Call on the env's own JS thread before
	 * enqueuing the commit. Sets `closed` (leaving the maps untouched) when the
	 * descriptor's completion plumbing has already shut down — the caller must
	 * then use the legacy commit path.
	 */
	napi_status registerCommitCompletion(napi_env env, napi_threadsafe_function_call_js callJs, bool& closed);

	/**
	 * Commit thread. Delivers a completed commit's `state` to its originating
	 * env. Returns false if that env's completion tsfn is gone (env torn down
	 * or released) — the caller then drops the state.
	 */
	bool dispatchCommitCompletion(napi_env env, void* state);

	/**
	 * JS thread (completion callback). Accounts a finished commit, unref-ing the
	 * tsfn when the env goes idle so the event loop can exit.
	 */
	void finishCommitCompletion(napi_env env);

	/**
	 * Module env-cleanup hook. Releases and forgets a dying env's completion
	 * tsfn so the commit thread stops marshalling into a torn-down env.
	 */
	void releaseCommitCompletionsByEnv(napi_env env);

	/**
	 * Bounded waits for this database's parked coordinated-retry commits.
	 * Never null; owned by shared_ptr so a LockTracker wake callback can hold
	 * a weak reference to it without referencing the descriptor (see
	 * `ParkTimeoutRegistry`). Drained and joined by `finishClose()`.
	 */
	const std::shared_ptr<ParkTimeoutRegistry> parkTimeouts =
		std::make_shared<ParkTimeoutRegistry>();

private:
	DBDescriptor(
		const std::string& path,
		const DBOptions& options,
		const rocksdb::ColumnFamilyOptions& cfOptions,
		std::shared_ptr<rocksdb::DB> db,
		std::unordered_map<std::string, std::shared_ptr<ColumnFamilyDescriptor>>&& columns,
		std::shared_ptr<rocksdb::Statistics> statistics
	);

public:
	static std::shared_ptr<DBDescriptor> open(const std::string& path, const DBOptions& options);
	~DBDescriptor();

	void close();
	bool isClosing() const { return this->closing.load(); }

	/**
	 * Atomically transitions the descriptor into the closing state. Returns
	 * true if this call performed the transition (the caller now owns the
	 * close and must run `finishClose()`), false if it was already closing.
	 *
	 * Lets `DBRegistry::CloseDB` publish the closing state while still holding
	 * `databasesMutex`, so a concurrent `OpenDB` (which inspects `isClosing()`
	 * under the same lock) waits instead of handing the descriptor to a new
	 * handle that would then be closed out from under it.
	 */
	bool beginClose() { return !this->closing.exchange(true); }

	/**
	 * Performs the actual close work (flush, close handles, release resources).
	 * Only valid after `beginClose()` returned true; `close()` is the all-in-one
	 * entry point that claims and then runs this.
	 */
	void finishClose();

	void attach(std::shared_ptr<Closable> closable);
	void detach(std::shared_ptr<Closable> closable);

	/**
	 * Gets a single statistic value.
	 *
	 * @example
	 * ```typescript
	 * const stat = db.getStat('rocksdb.block.cache.miss');
	 * ```
	 */
	napi_value getStat(napi_env env, const std::string& statName);

	/**
	 * Gets all statistics.
	 *
	 * @example
	 * ```typescript
	 * const stats = db.getStats();
	 * ```
	 */
	bool getStats(napi_env env, bool all, napi_value* result);

	void lockCall(
		napi_env env,
		std::string& key,
		napi_value callback,
		napi_deferred deferred,
		std::shared_ptr<DBHandle> owner
	);
	void lockEnqueueCallback(
		napi_env env,
		std::string& key,
		napi_value callback,
		std::shared_ptr<DBHandle> owner,
		bool skipEnqueueIfExists,
		napi_deferred deferred,
		bool* isNewLock
	);
	bool lockExistsByKey(std::string& key);
	bool lockReleaseByKey(std::string& key);
	void lockReleaseByOwner(DBHandle* owner);
	void onCallbackComplete(const std::string& key);

	void transactionAdd(std::shared_ptr<TransactionHandle> txnHandle);
	std::shared_ptr<TransactionHandle> transactionGet(uint32_t id);
	void transactionRemove(std::shared_ptr<TransactionHandle> txnHandle);
	uint32_t transactionGetNextId();

	/**
	 * Closes every registered transaction whose owning DBHandle was created by
	 * `env`, from that env's module cleanup hook. Pending transactions are only
	 * removed from the registry by commit/abort, so a worker env that dies with
	 * one still open leaks the TransactionHandle — holding a live RocksDB
	 * transaction/snapshot — into this process-global descriptor, and the last
	 * env's Shutdown later walks it with a dangling env
	 * (HarperFast/rocksdb-js#741).
	 *
	 * Deliberately env-scoped, NOT part of DBHandle::close(): a user-called
	 * db.close() runs with live microtasks — db.transaction() awaits its
	 * callback, so a legitimate commit can be one microtask behind the close
	 * and must still run (reaping there rejects it with "Database not open"
	 * and, on Deno, strands the caller). At env teardown no such continuation
	 * can exist.
	 */
	void closeTransactionsByEnv(napi_env env);

	/**
	 * Removes a dropped column family from the columns map (under
	 * `columnsMutex`) so a later open-by-name creates a fresh column family
	 * instead of reusing the dangling dropped handle. DBHandles still holding
	 * the descriptor keep it alive via their shared_ptr; only the by-name
	 * lookup is removed.
	 *
	 * @param columnName The name of the dropped column family.
	 */
	void unregisterColumnFamily(const std::string& columnName);

	/**
	 * Creates a new user shared buffer or returns an existing one.
	 *
	 * @param env The environment of the current callback.
	 * @param key The key of the user shared buffer.
	 * @param defaultBuffer The default buffer to use if the user shared buffer does
	 * not exist.
	 * @param listener An optional listener (from addListener) to remove when the
	 * user shared buffer is garbage collected.
	 */
	napi_value getUserSharedBuffer(
		napi_env env,
		std::string& key,
		std::shared_ptr<DBHandle> dbHandle,
		napi_value defaultBuffer,
		std::shared_ptr<ListenerCallback> listener = nullptr
	);

	std::shared_ptr<ListenerCallback> addListener(napi_env env, std::string& key, napi_value callback, std::weak_ptr<DBHandle> owner);
	bool notify(std::string key, ListenerData* data);
	napi_value listeners(napi_env env, std::string& key);
	napi_value removeListener(napi_env env, std::string& key, napi_value callback);
	void removeListener(const std::string& key, const std::shared_ptr<ListenerCallback>& target);
	void removeListenersByOwner(DBHandle* owner);
	void removeListenersByEnv(napi_env env);

	napi_value listTransactionLogStores(napi_env env);
	napi_value purgeTransactionLogs(napi_env env, napi_value options);
	std::shared_ptr<TransactionLogStore> resolveTransactionLogStore(const std::string& name);
	/**
	 * Flushes every column family's memtable. `allowWriteStall = false` (the RocksDB default)
	 * makes this WAIT, unbounded, on the calling thread — see the `FlushOptions` JSDoc in
	 * `src/load-binding.ts` and AGENTS invariant 15.
	 */
	rocksdb::Status flush(bool allowWriteStall = false);

	/**
	 * Compacts a range of keys in the specified column family. This method is
	 * thread-safe and uses a mutex to prevent concurrent compaction operations.
	 *
	 * @param column The column family to compact.
	 * @param start The start key of the range (nullptr for beginning).
	 * @param end The end key of the range (nullptr for end).
	 * @returns The status of the compaction operation.
	 */
	rocksdb::Status compactRange(
		rocksdb::ColumnFamilyHandle* column,
		const rocksdb::Slice* start,
		const rocksdb::Slice* end,
		bool bottommost = false
	);
};

/**
 * State to pass into `napi_call_threadsafe_function()` for a lock callback.
 */
struct LockCallbackCompletionData final {
	LockCallbackCompletionData(const std::string& k, std::weak_ptr<DBDescriptor> d, napi_deferred def = nullptr)
		: key(k), descriptor(d), deferred(def) {}

	/**
	 * The key of the lock.
	 */
	std::string key;

	/**
	 * The descriptor of the database.
	 */
	std::weak_ptr<DBDescriptor> descriptor;

	/**
	 * Optional deferred promise to resolve when the lock is released (for withLock).
	 */
	napi_deferred deferred;

	/**
	 * Flag indicating resolve/reject callback has been called and prevent
	 * the callback from being called again.
	 */
	std::atomic<bool> completed{false};
};

/**
 * Holds a threadsafe callback and its associated deferred promise (if any).
 */
struct LockCallback final {
	LockCallback(napi_threadsafe_function callback, napi_deferred deferred = nullptr)
		: callback(callback), deferred(deferred) {}

	napi_threadsafe_function callback;
	napi_deferred deferred;
};

/**
 * Tracks a queue of callbacks for a lock, the lock owner, and whether a
 * callback is currently running to prevent multiple callbacks from being
 * executed at the same time.
 */
struct LockHandle final {
	LockHandle(std::weak_ptr<DBHandle> owner, napi_env env)
		: owner(owner), env(env) {}

	~LockHandle() {
		while (!threadsafeCallbacks.empty()) {
			LockCallback lockCallback = threadsafeCallbacks.front();
			threadsafeCallbacks.pop();
			NAPI_STATUS_THROWS_VOID(::napi_release_threadsafe_function(lockCallback.callback, napi_tsfn_release));
		}
	}

	/**
	 * A queue of threadsafe callbacks to fire in sequence.
	 */
	std::queue<LockCallback> threadsafeCallbacks;

	/**
	 * The owner of the lock. Used to release any locks owned by a database
	 * instance that is being closed.
	 */
	std::weak_ptr<DBHandle> owner;

	/**
	 * Flag indicating whether the current callback is running. It's used when
	 * a new callback is enqueued to determine if we should call the callback
	 * immediately (false) or add it to the queue (true).
	 */
	std::atomic<bool> isRunning = false;

	/**
	 * The environment of the current callback.
	 */
	napi_env env;
};

/**
 * Contains the buffer and buffer size for a user shared buffer.
 */
struct UserSharedBufferData final {
	/**
	 * The data of the user shared buffer.
	 */
	char* data;

	/**
	 * The size of the user shared buffer.
	 */
	size_t size;

	UserSharedBufferData(void* sourceData, size_t size) : size(size) {
		this->data = new char[size];
		::memcpy(this->data, sourceData, size);
	}

	~UserSharedBufferData() {
		delete[] this->data;
	}

	// delete copy constructor and copy assignment to prevent accidental copying
	UserSharedBufferData(const UserSharedBufferData&) = delete;
	UserSharedBufferData& operator=(const UserSharedBufferData&) = delete;
};

/**
 * Finalize data for user shared buffer ArrayBuffers to clean up map entries
 * when the ArrayBuffer is garbage collected.
 *
 * Holds a strong reference to the underlying `UserSharedBufferData` so the
 * backing storage outlives any ColumnFamilyDescriptor / DBDescriptor teardown
 * until JS releases every retained ArrayBuffer for the key. The weak pointers
 * to `DBHandle` / `ColumnFamilyDescriptor` are used for opportunistic cleanup
 * (removing listeners, erasing map entries) when those are still alive.
 *
 * The listener is held as a `weak_ptr` (not the raw `napi_ref`): the ref's
 * ownership belongs to the listener's threadsafe function, which deletes it
 * once the listener is torn down. A `weak_ptr` lets the finalizer remove the
 * listener by identity only while it is still live, without ever dereferencing
 * a ref it does not own (HarperFast/rocksdb-js#790).
 */
struct UserSharedBufferFinalizeData final {
	std::string key;
	std::weak_ptr<DBHandle> dbHandle;
	std::weak_ptr<ColumnFamilyDescriptor> columnDescriptor;
	std::shared_ptr<UserSharedBufferData> sharedData;
	std::weak_ptr<ListenerCallback> listener;

	UserSharedBufferFinalizeData(
		const std::string& k,
		std::weak_ptr<DBHandle> d,
		std::weak_ptr<ColumnFamilyDescriptor> c,
		std::shared_ptr<UserSharedBufferData> data,
		std::weak_ptr<ListenerCallback> listener = {}
	) : key(k), dbHandle(d), columnDescriptor(c), sharedData(std::move(data)), listener(std::move(listener)) {}
};

/**
 * Contains the column family handle and map of user shared buffers.
 */
struct ColumnFamilyDescriptor final {
	/**
	 * The column family handle.
	 */
	std::shared_ptr<rocksdb::ColumnFamilyHandle> column;

	/**
	 * Map of user shared buffers by key.
	 */
	std::unordered_map<std::string, std::shared_ptr<UserSharedBufferData>> userSharedBuffers;

	/**
	 * Mutex to protect the user shared buffers map.
	 */
	std::mutex userSharedBuffersMutex;

	ColumnFamilyDescriptor(std::shared_ptr<rocksdb::ColumnFamilyHandle> column) : column(column) {}

	~ColumnFamilyDescriptor() {
		DEBUG_LOG("%p ColumnFamilyDescriptor::~ColumnFamilyDescriptor destroying column family descriptor\n", this);
	}

	void releaseUserSharedBuffer(const std::string& key, const std::shared_ptr<UserSharedBufferData>& sharedData) {
		DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer releasing user shared buffer (use_count: %ld) for key:", this, sharedData.use_count());
		DEBUG_LOG_KEY_LN(key);

		std::lock_guard<std::mutex> lock(this->userSharedBuffersMutex);
		DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer locked user shared buffers map (size: %ld)\n", this, this->userSharedBuffers.size());
		auto iter = this->userSharedBuffers.find(key);
		DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer created iterator\n", this);
		if (iter != this->userSharedBuffers.end() && iter->second == sharedData) {
			DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer found user shared buffer (use_count: %ld) for key:", this, sharedData.use_count());
			DEBUG_LOG_KEY_LN(key);

			// Each live external ArrayBuffer keeps one strong ref via its
			// finalize data; the map entry is a second strong ref. If the
			// current finalizer's ref + the map entry are the only two left,
			// no other ArrayBuffers exist for this key and the map entry is
			// safe to evict here. Otherwise leave the entry in place so future
			// getUserSharedBuffer() calls keep returning the same mapping.
			if (sharedData.use_count() <= 2) {
				this->userSharedBuffers.erase(key);
				DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer removed user shared buffer for key:", this);
				DEBUG_LOG_KEY_LN(key);
			}
		} else {
			DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer user shared buffer not found for key:", this);
			DEBUG_LOG_KEY_LN(key);
		}
	}
};

} // namespace rocksdb_js

#endif
