# Abort drains in-flight async gets

`rocksdb::Transaction` is not thread-safe, but a cold-cache `get()` runs `txn->Get()` on a libuv
worker while the transaction stays usable on its JS thread. `Transaction::Abort` therefore ends the
transaction with `TransactionHandle::close()`, which cancels and drains the handle's admitted async
work before it deletes `txn`. It does not call `Rollback()`: abort only ever sees an unprepared
transaction, and deleting one discards its writes and releases its locks. After a failed pessimistic
commit, `Rollback()` reports an invalid state, which abort used to throw as "Transaction rollback
failed" after the transaction was already gone.

That drain can wait on a disk read, so with work in flight Abort holds a descriptor `OperationGuard`
until `close()` returns, and leaves the close to a teardown that is already under way (a closing or
destroyed descriptor) rather than wait for its sweep. The guard pins
`TransactionHandle::owningDescriptor`, a weak reference fixed at construction:
`dbHandle->descriptor` is gone once the owner has closed its handle, and another env can still
hold the database open. Locking it can make a concurrent last-handle close skip its purge, so
Abort retries `DBRegistry::PurgeIfUnreferenced` after releasing it, as backup and checkpoint do.

`AsyncWorkHandle::cancelIfIdle()` makes the common abort, with nothing admitted, cheap: under the
handle's own mutex it either closes admission or reports work in flight, so no get from any env
can slip in between. Only the in-flight case touches the descriptor (Harper ends every read-only
transaction with an abort). The idle case needs no guard: `close()` itself keeps a foreign sweep
waiting ([close ownership](#close-ownership)). An already-cancelled handle means a teardown `close()`
owns the transaction. Abort then only sets the atomic `writesAbandoned`, so the transaction refuses
later writes and commits before that `close()` runs; it leaves `state` to the `close()`, which writes
it concurrently.

Covered by `test/transactions.test.ts` (aborts with async gets in flight; an ASan build with
`max_free_fill_size` faults on an abort that does not drain first) and
`test/fixtures/fork-abort-get-destroy.mts` (abort and a foreign destroy, in each order).

Other owning-thread mutations still run while an admitted get may be inside `txn->Get()`:
commit (`Clear()` after the write),
`putSync`/`removeSync` (batch append), and `resetTransaction()` (deletes and recreates `txn` on a
coordinated retry). Draining in each would block `putSync` on unrelated disk reads; the candidate
fix is for the worker to read through the database with a pinned transaction snapshot. That the key
is absent from the batch when the worker is queued is established by the caller: the async get's
only caller, `Store.get()`, reaches it after a cache-tier `getSync()` in the same synchronous turn,
which consulted the batch and did not resolve the key. The async single-key setup does not consult
the batch; its worker's `txn->Get()` still does, which is the race above.

`TransactionHandle::multiGet()` captures staged writes, deletes and cache hits on the owning thread
with a cache-tier `MultiGet`, retaining their `PinnableSlice`s and a shared transaction snapshot
(`GetTimestampedSnapshot()`). Its worker reads only unresolved keys through `DB::MultiGet`, so it
never accesses a transaction that a write, commit or retry may mutate or reset. The shared snapshot
survives a reset. With snapshots disabled, unresolved keys read the database's current state.
Values, the snapshot and the column descriptor are released before signaling execute completion,
so teardown cannot destroy their database first. The existing transaction async registration still
lets abort and orphan cleanup cancel and drain the read.

# Synchronous admission

A call that reaches a transaction through its own `NativeTransaction` wrapper (`getSync`, the setup of
`get`, `getMany`, `getManySync`, `getCount`, `putSync`, `removeSync`, `useLog`, `setTimestamp`,
`abandonWrites`, and a transaction-context `getRange`) holds a `TransactionOperation` whenever it
reads `dbHandle` or `txn`, and releases it only around steps that run JavaScript (below). A foreign `destroy()`/`shutdown()` resets those in
`TransactionHandle::close()`, from `finishClose()`'s closables sweep on its own thread.

`TransactionOperation` registers with the transaction's own async-work drain through the lock-free
`AsyncWorkHandle::enterSync()`, which `close()` waits on before it deletes `txn` or resets `dbHandle`.
It is refused once `close()` has published cancellation, and the call then reports a closed
transaction. A descriptor `OperationGuard` would also make that sweep wait, but its counter and the
descriptor's reference count are shared by every thread using the database: with four workers on one
database it roughly doubled the cost of `txn.getSync()`, which is what `db.getSync()` pays today
(HarperFast/rocksdb-js#1138).

The admission covers the transaction's `DBHandle` only because `DBDescriptor::closeClosables()` drains
`transactions` before it closes any other closable. A `DBHandle::close()` from that sweep resets
`columnDescriptor`, which these calls read; in the map's own order the sweep could reach the handle
first. The order holds only if no transaction is registered once the drain has started, so
`Transaction::Constructor` holds a descriptor `OperationGuard` from its `isClosing()` check through
`transactionAdd()`, after its option getters have run. The drain first cancels every transaction, which aborts
parked async reads and refuses new calls, then closes idle ones first; while every one still has a call
in flight it waits for one to finish rather than close any. Closing a pessimistic writer blocked on a
row lock waits for its call, and the holder of that lock may be another transaction in the same map.

No JavaScript runs while a call is admitted: JavaScript that aborts, commits or closes the
transaction on the same thread would wait on the admission it is running under. Each N-API step that
can run JavaScript therefore happens before admission or after it ends: an option getter, `instanceof`,
a coded error (`createJSError`/`createRocksDBError` call `Object.create` and set properties, so patched
globals run there), and creating async work (`napi_create_async_work` runs `async_hooks` init hooks).
Errors without a code (`napi_throw_error(env, nullptr, ...)`) run none. So `putSync`, `removeSync` and
`setTimestamp` decide their error while admitted and build it after. `useLog` admits once to read the
transaction's handle, releases, validates with `instanceof`, admits again to recheck that handle and
bind the store, and constructs the JavaScript `TransactionLog` after. An async `get`/`getMany` first
owns its keys (`get` copies one out of the handle's shared default key buffer while admitted;
`getMany` copies with `MultiGetBatch::copyKeys()`), so JavaScript that reuses or detaches that buffer
cannot change which records the read returns. `prepareGet`/`prepareMultiGet` then create the work
without reading transaction or handle state. `getMany` then admits and calls `multiGet()`; `get` calls
`get()` unadmitted, which rechecks its own handle (`ownHandleDetached()`) under its registration.
Both register on the transaction and queue. An init hook therefore runs before the read
establishes its snapshot and looks at staged writes, like JavaScript run before the call. `get()`/
`multiGet()` take the prepared state after their registration, so a failed setup releases the snapshot
and column descriptor it pinned while the registration still holds off `close()`. The `Database::*`
by-id reads prepare the same way. In builds with fatal asserts, `enterSync()` counts admissions per
thread and `assertJavaScriptMayRun()`, called by both error builders and before each prepared work
creation, fails while one is held. It does not see async-work registrations or handle guards.

Still exposed: `commit()` creates its completion's threadsafe function (and, in legacy mode, its async
work) while it holds the transaction's registration and a descriptor operation (#1172), and a
`Database::*` call, by-id transaction reads included, holds its handle's `HandleOperationGuard` across
its own error building and work creation, so a same-thread `destroy()`/`shutdown()` there still waits
on itself (#1143).

After admission, a handle the owner has closed or reopened no longer belongs to this transaction's
descriptor, so calls that use it refuse with "Database not open". `setTimestamp` and
`abandonWrites` use only the transaction and are not refused for that.

`Database::*` calls that name a transaction by id take `ACQUIRE_OPERATIONS_LOCK()` before the
lookup. It counts on the caller's handle, which `finishClose()` drains before its sweep, and the lookup
resolves in the caller's descriptor, which is the transaction's because ids are per descriptor. The
iterator constructor takes a descriptor `OperationGuard` before its lookup. `TransactionHandle::getSync`,
`putSync` and `removeSync` therefore admit nothing themselves. Covered by
`test/fixtures/fork-txn-sync-shutdown.mts` (a pause past the handle read, shutdown from another
thread; without the sweep order it crashes in about two runs of five) and
`test/fixtures/fork-txn-sync-refusal.mts` (its `reentrant` mode patches `Object.create`, installs an
`async_hooks` init hook and an `instanceof` hook that end the transaction or shut down).

# Write-intent refusal

With `transactionLogAbortMarkers` and `transactionLogWriteIntents` on (both read when an attempt begins; a handle still holding intents keeps the setting it took them under), a transaction's verification-table write intents carry its logical age (`TransactionHandle::age`, the constructor's timestamp, kept when JS changes the timestamp and across retries; `getMonotonicTimestamp()` never repeats within a process, and the handle address breaks a tie anyway). `lockSlotForWrite()` reports whether the slot already had a live holder that is, or may be, older: each tracker keeps its live holders' ages (four distinct holders inline, a holder's repeat intents sharing one entry, allocated only by an aged registration, under the VT `writerMutex_`). A holder without an age (a non-transactional write, an intent taken with either flag off) counts as possibly older. Aged holders past the inline four go to an overflow list, so precedence stays exact; one whose entry cannot be allocated is only counted, as possibly older. A monotonic minimum would not do: it would keep refusing behind a holder that has already released.

An attempt that joined an older holder is refused with `Busy` before writing its batch (`refusedBeforeLog`), or, if an earlier attempt already wrote it, before `Commit()`. It releases its intents and, with `coordinatedRetry`, parks on that slot. The older of two crossing transactions is never refused by the younger, so they resolve. Only transactions with a log batch are refused: the refusal exists so an attempt that will fail never logs, and an unlogged transaction keeps RocksDB's own validation. A refused attempt is reset like any conflicted one, so its log entries are gone and the retry must stage them again; a caller that skips re-logging on every retry must check `logWritten()` instead. Slots are hashed and process-wide, so a holder of an unrelated key that collides with this one can refuse an attempt too; the attempt parks until that holder commits or aborts, as coordinated retry already does after a commit conflict, and `verificationTableEntries` sets how often that happens. `commitSync()` refuses without parking, because the holder may need the same event loop. A park is bounded by `ROCKSDB_JS_PARK_TIMEOUT_MS` and by `Transaction#setParkDeadline`.

Refusal is off by default. Most workloads have little write contention, and there it only adds cost: with large transactions over a wide key space (500 random keys of 100,000 per transaction, 8 workers) it halved throughput and tripled retries, nearly all of them refusals before the log write. What it buys is fairness under heavy overlap: transactions that all write the same 1,000 keys converge either way, but without refusal the slowest wait about 6-7x longer. With the flag off, `agedIntents` is false, so `lockVTSlot()` takes the unaged `lockSlotForWrite()` and release passes no age: no `HolderAges` is allocated, `intentConflictSlot` stays null, and neither refusal site can fire. That is the same verification-table work as with abort markers off.

# Conflict check before the log write

With `transactionLogAbortMarkers` on, a retried optimistic attempt about to write its first batch also runs RocksDB's own commit-time conflict check, `CheckOptimisticTransactionForConflicts()` (exported by the `11.12.0` prebuild, HarperFast/rocksdb-prebuilds#39). A conflict with a write that already committed refuses it before logging, with `refusedBeforeLog` and `ConflictKind.checkBeforeLog`, and without a park: there is no holder left to wait for. Intent refusal (`transactionLogWriteIntents`) catches conflicts with writers still in flight; this catches the ones that have finished. The check is advisory: `Commit()` validates again, so a writer that commits in between still makes the logged attempt conflict.

It runs on retries alone (`TransactionHandle::retries > 0`): most commits do not conflict, and a first attempt that paid for the check would pay for it on every commit. In Harper's hot-key benchmark at 8 workers, running it on every attempt raised commit-thread CPU per cold commit by 14% (+8.7 µs), while retries alone cost +1.9 µs. At 4 records per transaction on 32 hot keys, with write intents on, retries alone cut p99 from 211 to 109 ms; every attempt reached 77 ms. A first attempt that conflicts with a finished writer therefore still logs, fails at `Commit()` and is retried write-once. A logged batch is write-once, so a retry reaches the check only when no earlier attempt wrote one: after an intent or check refusal, or a conflict on an attempt that staged no entries. With write intents off, the retries of a transaction that logged on its first attempt are never checked. Tests and benchmark arms can still run it on every attempt (`setPrelogConflictCheckRetriesOnlyForTesting(false)`, `ROCKSDB_JS_PRELOG_CHECK=always`).

It runs off RocksDB's write thread without the DB mutex, as RocksDB's own parallel-validated `Commit()` does, so it runs only under `occValidation: 'parallel'` (the default); under `'serial'` RocksDB validates inside the write group, and the check is skipped. The column families it reads must not be dropped meanwhile. It runs after commit admission, whose claims guarantee that (invariant 24). It is compiled only when the linked RocksDB defines `ROCKSDB_HAS_CHECK_OPTIMISTIC_TRANSACTION_FOR_CONFLICTS`; `constants.PRELOG_CONFLICT_CHECK` reports whether it is.

# State transitions

`TransactionHandle::state` (`TransactionStateCell`, `core/transaction_state.h`) has more than one writer. The owning JS thread moves it `Pending` → `Committing` and back. Successful async execution moves it to `Committed` on the commit worker (or the legacy libuv worker); `CommitSync` does so on the owning JS thread. `close()` settles `Aborted` on whichever thread tears the transaction down: a foreign `destroy()`/`shutdown()` runs it from another env, and a failed commit's completion is no longer registered async work, so the drain in `close()` does not wait for it. The cell therefore has no plain store. Every change is a compare-exchange from the state its writer expects, so `Committed` and `Aborted` are terminal.

The old completion did a check-then-store (`if (state == Committing) state = Pending`). A `close()` that landed between the two set `Aborted` and deleted `txn`, and the store then put the handle back to `Pending`. That leaves a `Pending` handle with no transaction, which `commit()`/`commitSync()` would admit again (HarperFast/rocksdb-js#769). `Commit`/`CommitSync` take `Pending` → `Committing` with a compare-exchange for the same reason, and reject as already aborted when it fails.

Reads are relaxed. They gate operations on the owning thread and order no data, and `state` is read on every get, put and remove, where a `seq_cst` access would add a barrier. A check that must not run past a concurrent abort needs a `transition()`, not a `load()`. A `commitSync()` registers with the transaction's async-work drain, as an async commit does, so a foreign `close()` waits for it (`test/fixtures/fork-commit-sync-shutdown.mts`). It then reads `dbHandle->descriptor`, through `DBHandle::ownerDescriptor()`, for the descriptor operation it holds through the commit. That read is safe only on the handle's owner thread, the one thread that resets it, which the accessor asserts in debug builds. A foreign close that lands before either registration is refused, by the transaction or by the closing descriptor (`test/fixtures/fork-commit-sync-closing.mts`).

See [lifecycle cost accounting](../../../docs/performance/transaction-state.md) for per-path CAS counts and the reproducible state-only measurement.

# Close ownership

`finishClose()` destroys the database right after its closables sweep, so the sweep must not return
while any `TransactionHandle::close()` is still using `txn`. `close()` makes that hold for every caller
(commit completions, abort, the GC finalizer's orphan close, the env reaper, the sweep itself), with no
descriptor operation: the first call wins `closed` and leaves `DBDescriptor::transactions` and
`closables` only after `txn` is deleted, and a later call waits on `closeProgress` until the winner
finishes. Registration is what makes the sweep find an unfinished close; the sweep's own `close()`,
which loses the gate, is what makes it wait (the sweep erases the entry before calling it). This is invariant 6's rule for
self-closing handles, which `DBIteratorHandle::closeIfOpen()` follows with `iteratorMutex`.

The handle leaves `transactions` and `closables` together, under one `txnsMutex` acquisition, as
the last step: a second acquisition cost 4 workers sharing a database about a third more per abort.
`lookupOwnedTransaction()` therefore refuses a handle once `closed` is set, so a by-id call does not
resolve a transaction being torn down. The uncontended close is one exchange on `closeProgress`,
with no notify. A nested close of the same handle
on the winner's thread returns instead of waiting on itself, and `closeOrphanIfUnused()` never waits:
it runs on libuv and iterator-release threads, and a winner already owns the close. Because no close
depends on the descriptor being open, a commit completion and the env reaper close their handles
even after a failed close-time flush quarantined the descriptor, instead of leaving them to a sweep
that has not run (`test/fixtures/fork-commit-quarantine-close.mts`). Covered by
`test/fixtures/fork-txn-close-shutdown.mts` (orphan and idle-abort closes),
`fork-commit-async-shutdown.mts` (a commit completion, and the commit lane's env-gone fallback,
which runs on the enqueuing thread and so, when no lane thread could start, inline on the JS thread
inside `commit()`, a thread `finishClose()` does not join) and `fork-commit-reaper-shutdown.mts` (a
terminated worker's transactions: Node runs a dying env's queued completions before its cleanup
hooks, so the env reaper closes only a pending one), each under `shutdown()` and `destroy()`; the
`ROCKSDB_JS_TXN_CLOSE_DRAIN_LATCH` seam parks the close and reports whether the closer waited for it.

`close()` reaches the descriptor through `TransactionRegistryLink`, never through
`dbHandle->descriptor`, which the owner's handle close resets, nor through `owningDescriptor.lock()`:
that reference can make a racing last-handle close skip its purge, and `close()` runs on commit-lane
and libuv threads where retrying the purge would make `finishClose()` join or wait for the thread
running it. The link is a raw pointer whose mutex is `txnsMutex`; `~DBDescriptor` clears it under
that lock before anything else. `~TransactionHandle` can run under `txnsMutex`
(`transactions.clear()`), so `close()` skips the registry when `weak_from_this()` is empty; the
registry cannot be holding it then. Covered by `test/transactions.test.ts`,
`test/transaction-orphan-gc.test.ts` and the `async-get-handle-closed` mode of
`test/fixtures/transaction-orphan-dependents.mts`.
