# Optimistic commit validation: policy and lock buckets

Every writable optimistic descriptor reads two process-wide settings at open time and passes them
to `OptimisticTransactionDB::Open`: `occValidation` (`DBSettings::getOccValidateSerial()`, default
`'parallel'`) and `occLockBuckets` (`DBSettings::getOccLockBucketCount()`, default 2^16). Neither
freezes; a descriptor keeps what it opened with.

**Parallel** (`kValidateParallel`) gives each database a private `OccLockBuckets` pool. A commit
takes the buckets for its keys in sorted order, runs the conflict check, and holds them through
`DB::Write()`, so a bucket can be held for a WAL sync or a write stall. The default is 2^16 rather
than RocksDB's 2^20 (about 2.5 MiB instead of 40 MiB per database on Linux x64). Concurrent commits
to one database (its commit threads, `commitSync()` calls from any thread, and legacy libuv
commits) wait for each other only when their keys collide on a bucket, and colliding commits also
lose RocksDB write-group batching. Collisions grow with the product of the concurrent commits' key
counts over the bucket count: with four commit threads, 2^16 buckets matched 2^20 and 2^22 at 64
keys per transaction, while at 1,000 keys 2^16 committed 336 transactions/s against 466 at 2^20 and
685 at 2^22 (and 311 on a single commit lane). That is the cost of a small pool, and it is why
databases with large transactions should raise the count.

The pool must stay private. A process-wide shared pool (RocksDB's `shared_lock_buckets`) makes
commits in unrelated databases contend on hash collisions. Because buckets are held through the
write, one database in a write stall can then block commits in every other database whose keys
collide with its pending commit. Sorted acquisition keeps concurrent commits deadlock-free.

**Serial** (`kValidateSerial`) checks conflicts in an `OptimisticTransactionCallback` that runs
inside the write group, and RocksDB allocates no bucket pool for it
(`OptimisticTransactionDBImpl`'s constructor builds one only for `kValidateParallel`). The check
and the write are atomic with respect to every other writer, and `DropColumnFamily` enters the
same write thread. The callback's `AllowWriteBatching()` is false, so RocksDB never groups a
serial commit with other writers, including the database's other commit threads. With four commit
threads it gave back the whole gain they bring: four workers committing 64-key transactions got
4.5k commits/s against 9.9k under parallel validation (4.6k on a single lane), and eight workers
committing 1-key transactions 160k against 202k. It only came out ahead at 1,000 keys with the
default 2^16 buckets (361 against 336/s), where raising the bucket count does better.

`test/occ-lock-buckets.test.ts` covers config validation, conflict detection under both policies
with small pools, and Linux memory assertions for the default count, a configured count, and the
serial policy. `test/occ-serial-drop.test.ts` reruns the column-family drop suites under serial.

# Commit threads: concurrent RocksDB commits per database

Async commits run on a database's `CommitWorker` threads, never on the libuv pool (#694: a commit
blocked in a write stall or a slow transaction-log write must not take a libuv slot from fs, dns,
crypto or async gets). Until #898 that was one thread per database, and the thread was the
bottleneck: with four workers committing 64-key transactions it ran at 96-100% of a core, about
38% in optimistic validation and 33% in memtable inserts, and committed 2.1x fewer transactions
per second than concurrent commits. Both stages scale across threads when RocksDB sees concurrent
writers, so the worker now runs up to `commitThreads` threads (default `min(4, cores)`, read at
open). A thread starts only when queued commits outnumber idle threads, so a database that is never
committed to concurrently owns one. Threads are per database rather than a process-wide pool so
that a database in a write stall, or holding commit lock buckets across one, occupies only its own
threads instead of blocking commits to every other database.

Each commit thread runs a commit's transaction-log write and its RocksDB commit back to back; log
writes serialize on the store's write mutex, as legacy libuv commits always did. Routing log-bearing
commits through the ordered `rocksdb-txnlog` lane first (`ROCKSDB_JS_COMMIT_THREAD=2`) was measured
as the alternative: no faster under concurrency, and 13-25% fewer commits per second for a caller
with one commit in flight, which pays an extra thread handoff per commit. That caller is Harper's
replication receiver, which awaits each commit before applying the next.

**Concurrent commits finish in any order.** Their promises resolve out of dispatch order and their
RocksDB sequence numbers do not follow transaction-log order. Legacy libuv commits and concurrent
`commitSync()` callers always behaved this way, so the code that must tolerate it already did, with
one exception, fixed here:

- `TransactionLogStore::commitFinished()` pairs the fully committed log position with the latest
  RocksDB sequence, and a flush uses the latest pair whose sequence it covers as the replay start.
  The pair is only sound if every log position below it has a sequence no greater than it. The
  caller used to read the sequence before taking `dataSetsMutex`; an earlier log position could
  then commit at a later sequence and finish in between, so the pair claimed a replay start past
  data a flush at the smaller sequence did not hold, and crash replay skipped it. The sequence is
  now read through a callback under that mutex, after the position leaves the uncommitted set,
  when every lower position has already returned from `Commit()`.
- The committed-read watermark is the lowest position in the uncommitted set, under the same
  mutex, so it only advances over a contiguous prefix of committed transactions. Every
  successful commit emits `'committed'` on its own completion, whether or not it was the one
  that moved the watermark — with out-of-order completion, a later-dispatched commit can emit
  `'committed'` while the watermark is still pinned on an earlier, not-yet-committed position,
  so a committed `log.query()` does not yet see that transaction's entry.
- Recovery's unclosed-tail discard (AGENTS.md invariant 14) needs only that a transaction's log
  write completes before its own RocksDB commit and that log writes are serialized, which the
  store's write mutex guarantees in every mode.
- Column-family commit claims, VT intent release and coordinated-retry parking were already
  shared with legacy and `commitSync()` commits. Completion delivery takes the originating env's
  `CommitCompletion` mutex, so concurrent threads only serialize per env.
- `finishClose()` drains the log lane before the commit worker; the worker's shutdown runs every
  queued task and joins every started thread.

`test/commit-threads.test.ts` covers the thread limit, lazy start, out-of-order resolution and the
watermark; `test/native/transaction_log_flushed_state_test.cc` covers the flush correlation.

# Backup table verification

`backups.verify({ verifyTables: true })` (`verifyBackupTables`, `backup_verify_tables.cpp`) must
prove every SST in the backup is readable by _this_ build, not only intact. Blob files are not
covered (RocksDB has no public blob-file reader). Three facts hold it together:

- **`SstFileReader::Open` is not enough on its own.** It decodes the footer (an unsupported
  `format_version` fails there) and the meta blocks, but never a data block, so a table whose
  compressor this build lacks would pass. The walk therefore also decodes the table's
  `compression_name` (`tableCompressionTypes`, both the `format_version >= 7` and the legacy
  encodings) and fails closed on anything it cannot interpret.
- **Physical paths are `backupDir + "/" + relative_filename`.** `BackupFileInfo::directory` is
  the virtual `private/<id>` directory, while `relative_filename` already starts with
  `shared/`, `shared_checksum/`, or `private/<id>/`. Joining the two finds no healthy table.
  The name comes from on-disk metadata, so an absolute or `..` name is rejected, not followed.
- **One reader at a time.** Each reader is destroyed before the next table opens, so descriptor
  use stays flat however many tables a backup holds
  (`BackupVerifyTables.OpensOneTableAtATime`).

The check describes the backup only at the moment it runs: `verify` takes no lock (invariant 7).
A caller that verifies and then restores holds the shared `.backup.lock` across both calls.

# Pending-wake threadsafe functions stay ref'd

A lock waiter (`withLock()`, or `tryLock(key, onUnlocked)` on a held key) and a coordinated-retry
commit parked on another transaction's write intent are each delivered through a one-shot
threadsafe function, often called from another env's thread. These stay **ref'd** from creation
until their single release (delivery, park timeout, descriptor close, or env teardown). Unref'd, a
worker with no other handle exits with code 0 before its wake arrives (harper#2312), and an
uncontended same-thread `await db.withLock()` never runs its queued call. Each wait has its own
TSFN, so the existing release is the unref; a ref count like `registerCommitCompletion`'s would
duplicate it. A waiter on a lock that is never released therefore keeps its thread alive, like any
other pending I/O, and closing the waiter's own database does not cancel it. A parked commit is
bounded by `ROCKSDB_JS_PARK_TIMEOUT_MS`.

`lockCall`, `onLockCallbackComplete`, and env cleanup dispatch the next waiter through
`dispatchNextLockCallback` with `locksMutex` held. The mutex protects both the TSFN call and
publication of `LockHandle::activeCall`, so cleanup cannot miss a dispatched call or let Node free
its TSFN mid-call. Cleanup first drops the dying env's queued callbacks, then marks its active
completion done and hands the lock to a surviving waiter. This covers both a call still queued on
the env's loop and an async callback whose promise never settles before worker termination.
`napi_closing` means the TSFN must not be released again; that waiter is skipped. `isRunning` and
`activeCall` are read and written only under `locksMutex`.

Completion state and its shared ownership are allocated during enqueue, before a callback is
accepted into the queue. Handoff and delivery only copy existing shared pointers, so allocation
failure cannot strand an accepted queue during completion. The TSFN's context owns a reference
until its finalizer runs; each `.then()` handler owns another reference freed by its finalizer
because only one handler ever runs. The lock retains the active call independently of those JS
handlers until completion or env teardown.

`completed` makes completion exactly-once across direct delivery, promise handlers, and teardown.
Every path that does not hand the callback off completes it, including a call delivered to a dying
env (`env == nullptr`, or a failed `napi_call_function`); on a live env it also rejects the caller,
with the callback's own exception when one is pending. Completion and dispatch act only on the
`LockHandle` the callback was dispatched for, never on whatever the key maps to now: `unlock()` or
the owner's close can erase that lock mid-callback, and a key-only lookup would clear `isRunning`
on, or erase, a newer lock of the same key and break mutual exclusion. Both callback paths clear
pending exceptions, including the notification-only path used by `unlock()` and owner close:
Deno does not report an exception left pending when a TSFN callback returns, so the next waiter's
`napi_call_function` would fail on it and pass the lock on early. Completion releases the lock
before settling the caller's promise because Deno runs the caller's continuation inside the settle
call. Regressions: `test/pending-wake-liveness.test.ts` and `test/lock.test.ts`.
