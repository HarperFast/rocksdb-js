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
descriptor, means a teardown `close()` owns the rollback; Abort returns and the closing database
refuses any later write or commit through the `Store` checks.

Covered by `test/transactions.test.ts` (aborts with async gets in flight; an ASan build with
`max_free_fill_size` faults on the unfixed code) and `test/fixtures/fork-abort-get-destroy.mts`
(segfaults without the guard or the owning-descriptor pin, no sanitizer needed).

Other owning-thread mutations still run while an admitted get may be inside `txn->Get()`: commit
(`Clear()` after the write), `putSync`/`removeSync` (batch append), and `resetTransaction()`
(deletes and recreates `txn` on a coordinated retry). Draining in each would block `putSync` on
unrelated disk reads; the candidate fix is for the worker to read through the database with a
pinned transaction snapshot, since the setup's batch-consulting attempt already proved the key
absent from the batch.
