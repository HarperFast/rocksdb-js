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
  mutex, so it only advances over a contiguous prefix of committed transactions, and the commit
  that advances it emits `'committed'` afterward on the same thread.
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
