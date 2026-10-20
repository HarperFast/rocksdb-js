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
in `prepareCommit()` and the two callers that follow it with `landTransaction()` (`transaction.cpp`:
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
4. **Land** the writes, then release the reservation. A logged optimistic handle writes its
   batch with a plain `db->Write()`: the verdict of step 2 holds because the reservation excludes
   every in-process writer of those keys, so a second validation inside `Commit()` could only fail
   for a reason that no longer matters to a batch the log already promises (a memtable flush that
   trimmed the history it reads, harper#1695), and it cost a second conflict check per audited
   write. Any other transaction commits through `txn->Commit()`, which validates.

The invariant: _a durable batch can be left uncommitted only by a non-conflict failure — an I/O
error, a crash — never by an ordinary optimistic conflict; and a handle holding one is never rolled
back or forgotten silently._

**A durable handle keeps a durable hold.** When a post-log attempt fails (an I/O error,
`forceTryAgainForTesting`), the transient hold is not released but promoted to a durable hold the
handle owns until the commit that lands or `close()`. Without it a later writer could log at a higher
position and commit the same keys before the retry, so log order (what replication and crash replay
produce) and data order would invert. Other writers are refused with `Busy` rather than blocked —
a transactional commit takes its ordinary conflict retry, a direct `putSync` throws `ERR_BUSY` — so a
JS thread can never wait on a hold only it can release. A transient wait is bounded by the holder's
own write, so a stalled holder delays a close exactly as a stalled commit always did. A batch whose
every write names a dropped column family can never land: that handle may be aborted and its
position is released, with a warning; a batch with writes to a surviving family keeps its
obligation (abort stays refused; a dropped handle pins, and a restart replays the survivors).

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

**A refused batch is carried.** A pre-log refusal keeps the staged log batch
(`logBatchCarried`) so a caller that staged it once still logs it on the retry; a retry that stages
again replaces it (the replacement is built before the carried batch is dropped), and a retry that
stages no write at all drops it, since it described writes the caller no longer makes. This keeps
the write-then-check era's callers correct without a lockstep change.

**`hasDurableLog`** (`TransactionHandle::logDurable`, `Transaction.hasDurableLog`): false through
every pre-log outcome, true from the first attempt whose log write returned, across retries (the
batch is write-once, #668), after the commit lands, and after close. `abort()` and
`abandonWrites()`, `putSync()` and `removeSync()` refuse with `ERR_TRANSACTION_LOGGED` before
touching anything while it is true; the caller retries `commit()` until it succeeds.

**A durable handle is never reset.** The write set a durable batch describes stays in the live
transaction, intact, because a failed landing writes nothing and clears nothing (`Commit()` and
`Write()` clear only on success), so every retry writes the same batch and its VT intents stay held
until it lands. An earlier design reset the transaction onto a fresh snapshot after a post-log
failure and replayed a saved copy into it. That reset exists to give a conflicted transaction a
snapshot to re-validate against, which a batch the log already promises does not want, and the
replay needed RocksDB's recovery-only `RebuildFromWriteBatch` (it moves the shared memtable cursor a
sole-writer write group inserts through and dereferences a null family once `DropColumnFamily` ran)
or about 250 lines of its own. `test/native/optimistic_validation_test.cc` pins what the plain write
rests on, under both OCC policies: a failed `Commit()` leaves the batch intact, `Write()` ignores the
conflict `Commit()` refused, `Rollback()` afterwards clears the transaction, and a range deletion is
refused before anything lands (`collectCommitKeyHashes` already refuses one before the log write).
`RocksDatabase.transaction()` / `transactionSync()` therefore re-commit such a handle past
`maxRetries` and `retryOnBusy` without re-running the callback, hand it back on
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
