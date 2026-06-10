# Stats

## Overview

RocksDB exposes two kinds of `Statistics`: **tickers** (64-bit unsigned integer counters) and
**histograms** (objects describing the distribution of a measurement across operations). In
addition, RocksDB exposes a set of column-family **properties** that are read on demand — these
are referred to below as _internal_ stats.

The transaction log subsystem (a Harper feature, separate from RocksDB's `Statistics`) exposes its
own counters and gauges; these are all ticker-like scalars today.

## RocksDB Stats

`db.getStats(all?)` returns an object containing a curated set of column-family properties plus
internal ticker and histogram stats. Individual values can also be read with `db.getStat(name)`.

```typescript
const db = RocksDatabase.open('path/to/db');
const stats = db.getStats();
const keysWritten = db.getStat('rocksdb.number.keys.written');
```

- **Type** — `ticker`, `histogram`, or `property` (a column-family property read via
  `GetIntProperty`).
- **Internal** — `Yes` if the value is a column-family property rather than a `Statistics`
  ticker/histogram. Internal properties do not require statistics to be enabled.
- **Essential** — `Yes` if the stat is returned by `db.getStats()` with the default `all = false`.
  When `all = true`, `getStats()` additionally returns the **complete** RocksDB ticker and
  histogram set (every name in RocksDB's `TickersNameMap` / `HistogramsNameMap`), which is not
  enumerated here. The `Statistics` tickers and histograms require statistics to be enabled
  (`enableStats: true`); the internal properties are always available.

| Name                                        | Description                                                                                     | Type      | Internal | Essential |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------- | --------- | -------- | --------- |
| `rocksdb.num-immutable-mem-table`           | Number of immutable memtables not yet flushed.                                                  | property  | Yes      | Yes       |
| `rocksdb.num-immutable-mem-table-flushed`   | Number of immutable memtables already flushed.                                                  | property  | Yes      | Yes       |
| `rocksdb.mem-table-flush-pending`           | `1` if a memtable flush is pending, else `0`.                                                   | property  | Yes      | Yes       |
| `rocksdb.cur-size-active-mem-table`         | Approximate size in bytes of the active memtable.                                               | property  | Yes      | Yes       |
| `rocksdb.cur-size-all-mem-tables`           | Approximate size in bytes of the active and unflushed immutable memtables.                      | property  | Yes      | Yes       |
| `rocksdb.size-all-mem-tables`               | Approximate size in bytes of active, unflushed, and pinned memtables.                           | property  | Yes      | Yes       |
| `rocksdb.num-entries-active-mem-table`      | Number of entries in the active memtable.                                                       | property  | Yes      | Yes       |
| `rocksdb.num-deletes-active-mem-table`      | Number of delete entries in the active memtable.                                                | property  | Yes      | Yes       |
| `rocksdb.compaction-pending`                | `1` if at least one compaction is pending, else `0`.                                            | property  | Yes      | Yes       |
| `rocksdb.estimate-pending-compaction-bytes` | Estimated bytes compaction must rewrite to rebalance the LSM tree.                              | property  | Yes      | Yes       |
| `rocksdb.num-running-compactions`           | Number of compactions currently running.                                                        | property  | Yes      | Yes       |
| `rocksdb.num-running-flushes`               | Number of flushes currently running.                                                            | property  | Yes      | Yes       |
| `rocksdb.total-sst-files-size`              | Total size in bytes of all SST files across all versions.                                       | property  | Yes      | Yes       |
| `rocksdb.live-sst-files-size`               | Total size in bytes of SST files in the current version.                                        | property  | Yes      | Yes       |
| `rocksdb.estimate-live-data-size`           | Estimated size in bytes of the live data.                                                       | property  | Yes      | Yes       |
| `rocksdb.estimate-num-keys`                 | Estimated number of live keys.                                                                  | property  | Yes      | Yes       |
| `rocksdb.block-cache-capacity`              | Capacity in bytes of the block cache.                                                           | property  | Yes      | Yes       |
| `rocksdb.block-cache-usage`                 | Bytes currently used by block cache entries.                                                    | property  | Yes      | Yes       |
| `rocksdb.block-cache-pinned-usage`          | Bytes occupied by pinned block cache entries.                                                   | property  | Yes      | Yes       |
| `rocksdb.num-live-versions`                 | Number of live LSM versions (high values indicate versions held by iterators/snapshots).        | property  | Yes      | Yes       |
| `rocksdb.current-super-version-number`      | Current super-version number, incremented on each LSM change.                                   | property  | Yes      | Yes       |
| `rocksdb.oldest-snapshot-time`              | Unix timestamp of the oldest unreleased snapshot, or `0`.                                       | property  | Yes      | Yes       |
| `rocksdb.num-blob-files`                    | Number of blob files in the current version.                                                    | property  | Yes      | Yes       |
| `rocksdb.total-blob-file-size`              | Total size in bytes of all blob files across all versions.                                      | property  | Yes      | Yes       |
| `rocksdb.live-blob-file-size`               | Total size in bytes of blob files in the current version.                                       | property  | Yes      | Yes       |
| `rocksdb.block.cache.hit`                   | Total block cache hits.                                                                         | ticker    | No       | Yes       |
| `rocksdb.block.cache.miss`                  | Total block cache misses.                                                                       | ticker    | No       | Yes       |
| `rocksdb.block.cache.data.hit`              | Data block cache hits.                                                                          | ticker    | No       | Yes       |
| `rocksdb.block.cache.data.miss`             | Data block cache misses.                                                                        | ticker    | No       | Yes       |
| `rocksdb.block.cache.index.hit`             | Index block cache hits.                                                                         | ticker    | No       | Yes       |
| `rocksdb.block.cache.index.miss`            | Index block cache misses.                                                                       | ticker    | No       | Yes       |
| `rocksdb.block.cache.filter.hit`            | Filter block cache hits.                                                                        | ticker    | No       | Yes       |
| `rocksdb.block.cache.filter.miss`           | Filter block cache misses.                                                                      | ticker    | No       | Yes       |
| `rocksdb.bloom.filter.useful`               | Times the bloom filter avoided an unnecessary read.                                             | ticker    | No       | Yes       |
| `rocksdb.bloom.filter.full.positive`        | Times the full-file bloom filter returned positive.                                             | ticker    | No       | Yes       |
| `rocksdb.bloom.filter.full.true.positive`   | Times the full-file bloom filter returned a true positive.                                      | ticker    | No       | Yes       |
| `rocksdb.db.iter.bytes.read`                | Total bytes read during iteration.                                                              | ticker    | No       | Yes       |
| `rocksdb.number.reseeks.iteration`          | Number of reseeks performed during iteration.                                                   | ticker    | No       | Yes       |
| `rocksdb.number.keys.read`                  | Total number of keys read via point lookups.                                                    | ticker    | No       | Yes       |
| `rocksdb.number.keys.written`               | Total number of keys written.                                                                   | ticker    | No       | Yes       |
| `rocksdb.bytes.read`                        | Total uncompressed bytes read via point lookups.                                                | ticker    | No       | Yes       |
| `rocksdb.bytes.written`                     | Total uncompressed bytes written.                                                               | ticker    | No       | Yes       |
| `rocksdb.memtable.hit`                      | Number of memtable hits.                                                                        | ticker    | No       | Yes       |
| `rocksdb.memtable.miss`                     | Number of memtable misses.                                                                      | ticker    | No       | Yes       |
| `rocksdb.txn.overhead.mutex.prepare`        | Transaction prepare mutex overhead count.                                                       | ticker    | No       | Yes       |
| `rocksdb.txn.overhead.mutex.old.commit.map` | Transaction old-commit-map mutex overhead count.                                                | ticker    | No       | Yes       |
| `rocksdb.txn.overhead.mutex.snapshot`       | Transaction snapshot mutex overhead count.                                                      | ticker    | No       | Yes       |
| `rocksdb.compact.read.bytes`                | Bytes read during compaction.                                                                   | ticker    | No       | Yes       |
| `rocksdb.compact.write.bytes`               | Bytes written during compaction.                                                                | ticker    | No       | Yes       |
| `rocksdb.compaction.cancelled`              | Number of cancelled compactions.                                                                | ticker    | No       | Yes       |
| `rocksdb.stall.micros`                      | Total microseconds writes were stalled.                                                         | ticker    | No       | Yes       |
| `rocksdb.no.file.errors`                    | Number of errors opening or reading files.                                                      | ticker    | No       | Yes       |
| `rocksdb.read.amp.estimate.useful.bytes`    | Estimated useful bytes read (read-amplification accounting; requires `read_amp_bytes_per_bit`). | ticker    | No       | Yes       |
| `rocksdb.read.amp.total.read.bytes`         | Total bytes read (read-amplification accounting).                                               | ticker    | No       | Yes       |
| `rocksdb.db.get.micros`                     | Distribution of `Get()` latencies (microseconds).                                               | histogram | No       | Yes       |
| `rocksdb.db.write.micros`                   | Distribution of `Write()` latencies (microseconds).                                             | histogram | No       | Yes       |
| `rocksdb.db.seek.micros`                    | Distribution of `Seek()`/`SeekForPrev()` latencies (microseconds).                              | histogram | No       | Yes       |
| `rocksdb.db.flush.micros`                   | Distribution of memtable flush times (microseconds).                                            | histogram | No       | Yes       |
| `rocksdb.db.write.stall`                    | Distribution of write-stall durations.                                                          | histogram | No       | Yes       |
| `rocksdb.blobdb.value.size`                 | Distribution of blob value sizes.                                                               | histogram | No       | Yes       |
| `rocksdb.sst.read.micros`                   | Distribution of SST file read latencies (microseconds).                                         | histogram | No       | Yes       |
| `rocksdb.compaction.times.micros`           | Distribution of compaction times (microseconds).                                                | histogram | No       | Yes       |

## Transaction Log Stats

`log.getStats()` returns a detailed statistics snapshot for a single transaction log store. Sizes
are in bytes and timestamps are milliseconds since the Unix epoch.

```typescript
const db = RocksDatabase.open('path/to/db');
const log = db.useLog('log-name');
const stats = log.getStats();
```

A summarized, aggregate subset is also merged into `db.getStats()` (summed across all of a
database's logs) under `txnlog.*` keys and can be read individually with `db.getStat('txnlog.…')`:
`txnlog.logCount`, `txnlog.fileCount`, `txnlog.totalSizeBytes`, `txnlog.mappedBytes`,
`txnlog.overlayBytes`, `txnlog.activeMaps`, `txnlog.pendingTransactions`,
`txnlog.uncommittedTransactions`, `txnlog.transactionsWritten`, `txnlog.bytesWritten`, and
`txnlog.replayGapBytes`. These aggregate keys are available even when RocksDB statistics are
disabled.

All transaction log stats are ticker-like scalars except the position fields (objects of the form
`{ sequence, offset }`) and the `name`/`path` identifiers.

| Name                           | Description                                                                                                                                                      | Type                           |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| `name`                         | The transaction log store name.                                                                                                                                  | string                         |
| `path`                         | Filesystem path to the log store directory.                                                                                                                      | string                         |
| `fileCount`                    | Number of sequence files on disk.                                                                                                                                | ticker                         |
| `currentSequenceNumber`        | Sequence number of the active (newest) log file.                                                                                                                 | ticker                         |
| `oldestSequenceNumber`         | Sequence number of the oldest retained log file.                                                                                                                 | ticker                         |
| `totalSizeBytes`               | Total size in bytes of all sequence files.                                                                                                                       | ticker                         |
| `currentFileSize`              | Size in bytes of the active log file.                                                                                                                            | ticker                         |
| `memory.mappedBytes`           | Bytes mapped into memory across all files. Virtual address space — the active file is mapped at the full `maxFileSize` on POSIX, so this is not resident memory. | ticker                         |
| `memory.overlayBytes`          | POSIX file-backed overlay bytes (the closer proxy for resident usage); `0` on Windows.                                                                           | ticker                         |
| `memory.activeMaps`            | Number of files with a live memory map.                                                                                                                          | ticker                         |
| `pendingTransactions`          | Transactions bound to the log but not yet written.                                                                                                               | ticker                         |
| `uncommittedTransactions`      | Transactions written to the log but not yet committed to RocksDB.                                                                                                | ticker                         |
| `nextLogPosition`              | Current write-head position.                                                                                                                                     | `{ sequence, offset }`         |
| `lastCommittedPosition`        | Last fully-committed position, or `null`.                                                                                                                        | `{ sequence, offset }` \| null |
| `lastFlushedPosition`          | Last position flushed to RocksDB.                                                                                                                                | `{ sequence, offset }`         |
| `replayGapBytes`               | Bytes between the last flushed position and the write head (the recovery cost on restart).                                                                       | ticker                         |
| `purge.oldestFileAgeMs`        | Age in milliseconds of the oldest retained file.                                                                                                                 | ticker                         |
| `purge.purgeableFiles`         | Files past retention and fully flushed — what the next purge would remove.                                                                                       | ticker                         |
| `purge.retainedUnflushedFiles` | Files past retention but retained because they are not yet flushed to RocksDB (purging would be unsafe for recovery).                                            | ticker                         |
| `purge.lastPurgeMs`            | Timestamp of the last purge, or `0` if never purged.                                                                                                             | ticker                         |
| `totals.transactionsWritten`   | Lifetime count of transactions written.                                                                                                                          | ticker                         |
| `totals.entriesWritten`        | Lifetime count of entries written.                                                                                                                               | ticker                         |
| `totals.bytesWritten`          | Lifetime bytes written (entry headers plus data).                                                                                                                | ticker                         |
| `totals.rotations`             | Lifetime count of log file rotations.                                                                                                                            | ticker                         |
| `totals.filesPurged`           | Lifetime count of files purged.                                                                                                                                  | ticker                         |
| `totals.bytesPurged`           | Lifetime bytes freed by purges.                                                                                                                                  | ticker                         |
| `totals.purgeRuns`             | Lifetime count of purge scans.                                                                                                                                   | ticker                         |
| `totals.databaseFlushes`       | Lifetime count of flushed-position advances.                                                                                                                     | ticker                         |
| `totals.writeFailures`         | Lifetime count of log file open failures encountered during writes.                                                                                              | ticker                         |
| `config.maxFileSize`           | Configured maximum file size in bytes before rotation.                                                                                                           | ticker                         |
| `config.retentionMs`           | Configured retention period in milliseconds.                                                                                                                     | ticker                         |
| `config.maxAgeThreshold`       | Age-based rotation threshold as a fraction of the retention period.                                                                                              | ticker                         |
