# Process-wide optimistic commit lock buckets

Every writable optimistic descriptor uses the same `DBSettings`-owned `OccLockBuckets`.
`getOccLockBuckets()` and `Config()` share one mutex, so the count freezes at materialization,
including a failed open, and cannot change across worker threads or close/shutdown/reopen.
Configuration releases this mutex before constructing a JS error, whose allocation may run finalizers.
The settings retain the pool for the process lifetime; RocksDB also retains shared ownership
until database teardown, which must drain native commits before destroying their databases.
RocksDB orders and deduplicates bucket locks and validates conflicts per database/key; sharing
adds cross-database waiting through the write (including WAL sync and stalls), not false conflicts.
A write that cannot progress can hold those buckets indefinitely and block other databases.
`test/occ-lock-buckets.test.ts` covers the lifetime/configuration contract and worker commits;
its Linux memory assertions distinguish shared pools from RocksDB's private-pool fallback.
