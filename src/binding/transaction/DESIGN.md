# Abort waits for in-flight async gets, under an operation guard

`rocksdb::Transaction` is not thread-safe, but a cold-cache `get()` runs `txn->Get()` on a libuv
worker while the transaction stays usable on its JS thread. `Rollback()` destroys the write-batch
index that `Get()` walks first, so `Transaction::Abort` cancels and drains the handle's admitted
async work before rolling back. `TransactionHandle::close()` drains too, but only after the
rollback has already run.

That drain can wait on a disk read, so Abort also holds a descriptor `OperationGuard` from before
the drain until `close()` returns. A foreign `destroy()`/`shutdown()` closes transactions in
`finishClose()`'s closables sweep, which starts only once `operationsInFlight` is zero. Without
the guard the sweep can delete `txn` while Abort waits, or find Abort's `close()` already holding
the close gate, skip it, and destroy the database while the parked get still pins a column family.
When registration on the handle is refused or the descriptor is already closing, teardown owns the
rollback and Abort returns without touching `txn`.

Both apply only when the handle has async work in flight. Only the owning JS thread admits that
work, so a zero count stays zero for the whole abort, and the common get-free abort (Harper ends
every read-only transaction with one) keeps its old cost: no registration, no descriptor atomics.
The guard also needs the owning `DBHandle` still open; after the owner's `db.close()` its
descriptor is gone, and an abort with a get in flight drains unguarded, so a concurrent foreign
teardown can still race its rollback.

Covered by `test/transactions.test.ts` (aborts with async gets in flight; an ASan build with
`max_free_fill_size` faults on the unfixed code) and `test/fixtures/fork-abort-get-destroy.mts`.

Other owning-thread mutations still run while an admitted get may be inside `txn->Get()`: commit
(`Clear()` after the write), `putSync`/`removeSync` (batch append), and `resetTransaction()`
(deletes and recreates `txn` on a coordinated retry). Draining in each would block `putSync` on
unrelated disk reads; the candidate fix is for the worker to read through the database with a
pinned transaction snapshot, since the setup's batch-consulting attempt already proved the key
absent from the batch.
