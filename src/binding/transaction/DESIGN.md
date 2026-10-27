# Abort waits for in-flight async gets, under an operation guard

`rocksdb::Transaction` is not thread-safe, but a cold-cache `get()` runs `txn->Get()` on a libuv
worker while the transaction stays usable on its JS thread. `Rollback()` destroys the write-batch
index that `Get()` walks first, so `Transaction::Abort` cancels and drains the handle's admitted
async work before rolling back. `TransactionHandle::close()` drains before it deletes `txn` and
never calls `Rollback()`; Abort needs its own drain because it rolls back before calling `close()`.

That drain can wait on a disk read, so Abort also holds a descriptor `OperationGuard` from before
the drain until `close()` returns. A foreign `destroy()`/`shutdown()` closes transactions in
`finishClose()`'s closables sweep, which starts only once `operationsInFlight` is zero. Without
the guard the sweep can delete `txn` while Abort waits, or find Abort's `close()` already holding
the close gate, skip it, and destroy the database while the parked get still pins a column family.
The guard pins `TransactionHandle::owningDescriptor`, a weak reference fixed at construction:
`dbHandle->descriptor` is gone once the owner has closed its handle, and another env can still
hold the database open. Locking it can make a concurrent last-handle close skip its purge, so
Abort retries `DBRegistry::PurgeIfUnreferenced` after releasing it, as backup and checkpoint do.

`AsyncWorkHandle::cancelIfIdle()` makes the common abort, with nothing admitted, cheap: under the
handle's own mutex it either closes admission or reports work in flight, so no get from any env
can slip in between. Only the in-flight case touches the descriptor (Harper ends every read-only
transaction with an abort). The idle case takes no guard and so still races a foreign teardown's
sweep the way every abort did before. An already-cancelled handle, or a closing or destroyed
descriptor, means a teardown `close()` owns the rollback. Abort then only sets the atomic
`writesAbandoned`, so the transaction refuses later writes and commits before that `close()` runs;
it leaves `state` to the `close()`, which writes it concurrently.

Covered by `test/transactions.test.ts` (aborts with async gets in flight; an ASan build with
`max_free_fill_size` faults on the unfixed code) and `test/fixtures/fork-abort-get-destroy.mts`
(segfaults without the guard or the owning-descriptor pin, no sanitizer needed).

Other owning-thread mutations still run while an admitted get may be inside `txn->Get()`: commit
(`Clear()` after the write), `putSync`/`removeSync` (batch append), and `resetTransaction()`
(deletes and recreates `txn` on a coordinated retry). Draining in each would block `putSync` on
unrelated disk reads; the candidate fix is for the worker to read through the database with a
pinned transaction snapshot, since the setup's batch-consulting attempt already proved the key
absent from the batch.

# Write-intent refusal

With `transactionLogAbortMarkers` and `transactionLogWriteIntents` on (both read when an attempt begins; a handle still holding intents keeps the setting it took them under), a transaction's verification-table write intents carry its logical age (`TransactionHandle::age`, the constructor's timestamp, kept when JS changes the timestamp and across retries; `getMonotonicTimestamp()` never repeats within a process, and the handle address breaks a tie anyway). `lockSlotForWrite()` reports whether the slot already had a live holder that is, or may be, older: each tracker keeps its live holders' ages (four distinct holders inline, a holder's repeat intents sharing one entry, allocated only by an aged registration, under the VT `writerMutex_`). A holder without an age (a non-transactional write, an intent taken with either flag off) counts as possibly older. Aged holders past the inline four go to an overflow list, so precedence stays exact; one whose entry cannot be allocated is only counted, as possibly older. A monotonic minimum would not do: it would keep refusing behind a holder that has already released.

An attempt that joined an older holder is refused with `Busy` before writing its batch (`refusedBeforeLog`), or, if an earlier attempt already wrote it, before `Commit()`. It releases its intents and, with `coordinatedRetry`, parks on that slot. The older of two crossing transactions is never refused by the younger, so they resolve. Only transactions with a log batch are refused: the refusal exists so an attempt that will fail never logs, and an unlogged transaction keeps RocksDB's own validation. A refused attempt is reset like any conflicted one, so its log entries are gone and the retry must stage them again; a caller that skips re-logging on every retry must check `logWritten()` instead. Slots are hashed and process-wide, so a holder of an unrelated key that collides with this one can refuse an attempt too; the attempt parks until that holder commits or aborts, as coordinated retry already does after a commit conflict, and `verificationTableEntries` sets how often that happens. `commitSync()` refuses without parking, because the holder may need the same event loop. A park is bounded by `ROCKSDB_JS_PARK_TIMEOUT_MS` and by `Transaction#setParkDeadline`.

Refusal is off by default. Most workloads have little write contention, and there it only adds cost: with large transactions over a wide key space (500 random keys of 100,000 per transaction, 8 workers) it halved throughput and tripled retries, nearly all of them refusals before the log write. What it buys is fairness under heavy overlap: transactions that all write the same 1,000 keys converge either way, but without refusal the slowest wait about 6-7x longer. With the flag off, `agedIntents` is false, so `lockVTSlot()` takes the unaged `lockSlotForWrite()` and release passes no age: no `HolderAges` is allocated, `intentConflictSlot` stays null, and neither refusal site can fire. That is the same verification-table work as with abort markers off.

# Conflict check before the log write

With `transactionLogAbortMarkers` on, an optimistic attempt about to write its first batch also runs RocksDB's own commit-time conflict check, `CheckOptimisticTransactionForConflicts()` (exported by the `11.8.1-1` prebuild, HarperFast/rocksdb-prebuilds#39). A conflict with a write that already committed refuses it before logging, with `refusedBeforeLog` and `ConflictKind.checkBeforeLog`, and without a park: there is no holder left to wait for. Intent refusal catches conflicts with writers still in flight; this catches the ones that have finished. The check is advisory: `Commit()` validates again, so a writer that commits in between still makes the logged attempt conflict.

It runs off RocksDB's write thread without the DB mutex, as RocksDB's own parallel-validated `Commit()` does, so the column families it reads must not be dropped meanwhile. It runs after commit admission, whose claims guarantee that (invariant 24). It is compiled only when the linked RocksDB defines `ROCKSDB_HAS_CHECK_OPTIMISTIC_TRANSACTION_FOR_CONFLICTS`; `constants.PRELOG_CONFLICT_CHECK` reports whether it is. A failure inside the check counts as no conflict.
