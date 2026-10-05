# Optimistic commit validation: policy and lock buckets

Every writable optimistic descriptor reads two process-wide settings at open time and passes them
to `OptimisticTransactionDB::Open`: `occValidation` (`DBSettings::getOccValidateSerial()`, default
`'parallel'`) and `occLockBuckets` (`DBSettings::getOccLockBucketCount()`, default 2^16). Neither
freezes; a descriptor keeps what it opened with.

**Parallel** (`kValidateParallel`) gives each database a private `OccLockBuckets` pool. A commit
takes the buckets for its keys in sorted order, runs the conflict check, and holds them through
`DB::Write()`, so a bucket can be held for a WAL sync or a write stall. The default is 2^16 rather
than RocksDB's 2^20 (about 2.5 MiB instead of 40 MiB per database on Linux x64) because almost
nothing contends for the pool. Async commits to one database run one at a time on its
`CommitWorker` lane, so the lane never waits on itself. The only other commits that wait on the
same buckets are `commitSync()` calls on that database from any thread, plus concurrent libuv
commits in the legacy `ROCKSDB_JS_COMMIT_THREAD=0` mode. Two commits that collide on a bucket also
lose RocksDB write-group batching. That is the cost of a small pool, and it is why 1,000-key
`commitSync()` mixes need about 2^20.

The pool must stay private. A process-wide shared pool (RocksDB's `shared_lock_buckets`) makes
commits in unrelated databases contend on hash collisions. Because buckets are held through the
write, one database in a write stall can then block commits in every other database whose keys
collide with its pending commit. Sorted acquisition keeps the lane-versus-`commitSync()` case
deadlock-free.

**Serial** (`kValidateSerial`) checks conflicts in an `OptimisticTransactionCallback` that runs
inside the write group, and RocksDB allocates no bucket pool for it
(`OptimisticTransactionDBImpl`'s constructor builds one only for `kValidateParallel`). The check
and the write are atomic with respect to every other writer, and `DropColumnFamily` enters the
same write thread. The callback's `AllowWriteBatching()` is false, so RocksDB never groups a
serial commit with other writers: this costs nothing on the single commit lane, but concurrent
`commitSync()` callers lose most of their batching.

`test/occ-lock-buckets.test.ts` covers config validation, conflict detection under both policies
with small pools, and Linux memory assertions for the default count, a configured count, and the
serial policy. `test/occ-serial-drop.test.ts` reruns the column-family drop suites under serial.
