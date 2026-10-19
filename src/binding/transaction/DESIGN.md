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

# Conflict before the log write

A transaction-log batch describes a write that must land: log readers, replication and crash
replay apply it. Until HarperFast/harper#3253 the first commit attempt wrote the batch and only then
let `txn->Commit()` check optimistic conflicts, so every conflicting audited write left a durable
entry and Harper had to retry it without a cap (harper#3266). The commit now runs in this order,
in `prepareCommit()` and the two callers that follow it with `txn->Commit()` (`transaction.cpp`:
`executeLogWork`/`executeCommitWork` for every async mode, `CommitSync` inline):

1. **Reserve** every (column family, key) the write batch names in the descriptor's
   `CommitKeyReservation` (`core/commit_key_reservation.h`).
2. **Validate** a first-attempt logged transaction with RocksDB's own check,
   `OptimisticTransaction::CheckTransactionForConflicts` (`core/optimistic_validation.cpp`).
   `Busy`/`TryAgain` here takes the existing retry path — reset onto a fresh snapshot, VT intents
   released, coordinated retry parked — with no log byte written and `hasDurableLog` still false.
3. **Write** the log batch; `publishDurableLog()` sets `committedPosition` and the atomic the JS
   accessor reads, only after `writeBatch()` returned (a failed append forgets its reserved
   position, `TransactionLogStore::forgetFailedPosition`, so the transaction stays unlogged).
4. **Commit** in RocksDB, which validates again, then release the reservation.

The invariant: _a durable batch can be left uncommitted only by a non-conflict failure — a
stranded snapshot after a memtable flush between steps 2 and 4, an I/O error, a crash — never by an
ordinary optimistic conflict; and a handle holding one is never rolled back or forgotten silently._

**Why the reservation is taken by every writer.** Step 2's verdict only holds at step 4 if no
write to those keys lands in between, and the writer that could land one is not necessarily
logged: an unlogged transactional commit or a non-transactional `Database::PutSync/RemoveSync`
(`reserveDirectWrite`) would do. So each takes its keys while it writes. Pessimistic databases skip
it (RocksDB row locks already exclude concurrent writers, so `Commit()` cannot return `Busy`).
`clear()` is outside it on purpose: it compacts and calls `DeleteFilesInRange`, which bumps no key
sequence and therefore cannot produce a conflict. The reservation is a set of 64-bit hashes under
one mutex with all-or-nothing acquisition: no lock order, no deadlock (a holder is inside a log
write or a RocksDB write, neither of which waits on a reservation), memory proportional to keys in
flight. A stripe array was measured out: at four commit threads and 64-key transactions it needs
about 2^16 stripes (2.5 MiB per database) before collisions stop serializing unrelated commits.

**Why RocksDB's validator and not a copy.** The check must agree with `Commit()` on the same
tracked keys and sequences, and only RocksDB's tracker knows them (a mirrored per-write sequence
samples before or after `TryLock` records, so it drifts on blind writes). The member is not public
API: `optimistic_validation.cpp` declares it with its v11.8.1 signature and calls it through the
transaction's own pointer. A signature change on upgrade fails the link;
`test/native/optimistic_validation_test.cc` fails if the verdict stops matching `Commit()`.

**`hasDurableLog`** (`TransactionHandle::logDurable`, `Transaction.hasDurableLog`): false through
every pre-log outcome, true from the first attempt whose log write returned, across retries (the
batch is write-once, #668), after the commit lands, and after close. `abort()` and
`abandonWrites()` refuse with `ERR_TRANSACTION_LOGGED` before touching anything while it is true;
the caller retries `commit()` until it succeeds. The RocksDB write set a durable batch describes
survives `resetTransaction()` (`RebuildFromWriteBatch` into the fresh transaction), so a
re-commit lands what the log says even if nothing is staged again; a caller that re-stages
overwrites the same keys. `RocksDatabase.transaction()` / `transactionSync()` therefore re-commit
such a handle past `maxRetries` and `retryOnBusy` without re-running the callback, hand it back on
`TransactionLoggedError.txn` when a non-retryable error follows, and `transactionSync` reuses one
handle across attempts in an iterative loop.

The validator adapter declares RocksDB's private member and reaches it through a friend, because
MSVC mangles the access level into the symbol. The addon is built without RTTI, so the call cannot
recover the complete object and relies on `OptimisticTransaction` deriving from `Transaction`
through a single non-virtual chain (pointer identity); the agreement test is what catches a wrong
`this`, and callers gate on `DBMode::Optimistic` so a pessimistic transaction never reaches it.

**A durable, uncommitted handle that is closed anyway** — a collected wrapper, a worker env exiting,
a database closing, a completion whose env is gone — keeps its position in the store's uncommitted
set (`close()` no longer calls `commitAborted`). The committed-read watermark and the flush
correlation that writes `txn.state` cannot pass it, so readers never receive the entry ahead of its
data and a restart replays it; one `log.warn` names the transaction and position. The cost — that
committed readers of the store stop there, and retention keeps every segment from there on, until
restart — is deliberate: the alternative advanced past an entry whose data was never applied.

Covered by `test/transaction-conflict-before-log.test.ts` (in-process, including competing writers
inside the validated window and the dropped-handle pins),
`test/transaction-conflict-before-log-fixtures.test.ts` (every commit mode and OCC policy as child
processes; process kills after the log write, after the commit, and after a rejection — they prove
process-crash behavior, not power loss, which the flush-time `fsync` ordering bounds as before) and
`test/native/commit_key_reservation_test.cc`.
