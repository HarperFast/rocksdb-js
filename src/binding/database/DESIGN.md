# Per-database optimistic commit lock buckets

Every writable optimistic descriptor opens with RocksDB's private `OccLockBuckets` pool, sized by
`DBSettings::getOccLockBucketCount()` at open time (`RocksDatabase.config({ occLockBuckets })`,
default 4,096). `kValidateParallel` takes the buckets for a commit's keys in sorted order, runs the
conflict check, and holds them through `DB::Write()`, so a bucket can be held for a WAL sync or a
write stall.

A small pool is enough because almost nothing contends for it. Async commits to one database run
one at a time on its `CommitWorker` lane, so the lane never waits on itself. The only waiters on
the same buckets are `commitSync()` calls on that database from any thread (plus concurrent libuv
commits in the legacy `ROCKSDB_JS_COMMIT_THREAD=0` mode). Two of those that collide on a bucket
also lose RocksDB write-group batching, which is the cost of a small pool. Harper also disables the RocksDB WAL for tables, so
buckets are not held across a WAL fsync there. RocksDB's 2^20 default cost about 40 MiB per
database on Linux x64, which dominated the footprint of processes that open many databases.

The pool must stay private. A process-wide shared pool (RocksDB's `shared_lock_buckets`) makes
commits in unrelated databases contend on hash collisions, and because buckets are held through
the write, one database in a write stall can block commits in every other database whose keys
collide with its pending commit. Private pools cannot couple databases. Sorted acquisition keeps
the lane-versus-`commitSync()` case deadlock-free.

`test/occ-lock-buckets.test.ts` covers validation, conflict detection with small pools, and a
Linux memory assertion that fails if opens fall back to RocksDB's 2^20 default.
