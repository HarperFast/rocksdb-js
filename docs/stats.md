# Stats

## Overview

`rocksdb-js` provides database-level RocksDB and transaction log stats using
`db.getStats()` or `db.getStat(statName)` as well as per-store transaction log
stats using `log.getStats()`.

There are two types of stats:

- **tickers** - 64-bit unsigned integer counters
- **histograms** - objects describing the distribution of a measurement across operations

Certain stats are only available when `enableStats` is `true` when the database was opened.

## Database Stats

```typescript
const db: RocksDatabase = RocksDatabase.open('path/to/db');

const basicStats: StatsDefault = db.getStats();
// or
const allStats: StatsAll = db.getStats(true);

// get a single stat
const keysWritten: number = db.getStat('rocksdb.number.keys.written');
const keysWrittenHistogram: StatsHistogramData = db.getStat('rocksdb.db.flush.micros');
```

### Basic Stats

By default, `db.getStats()` returns properties only.

| Name                                        | Description                                                                                                                                                                                                                   | Type   |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| `rocksdb.block-cache-capacity`              | Capacity in bytes of the block cache.                                                                                                                                                                                         | ticker |
| `rocksdb.block-cache-pinned-usage`          | Bytes occupied by pinned block cache entries.                                                                                                                                                                                 | ticker |
| `rocksdb.block-cache-usage`                 | Bytes currently used by block cache entries.                                                                                                                                                                                  | ticker |
| `rocksdb.compaction-pending`                | `1` if at least one compaction is pending, else `0`.                                                                                                                                                                          | ticker |
| `rocksdb.cur-size-active-mem-table`         | Approximate size in bytes of the active memtable.                                                                                                                                                                             | ticker |
| `rocksdb.cur-size-all-mem-tables`           | Approximate size in bytes of the active and unflushed immutable memtables.                                                                                                                                                    | ticker |
| `rocksdb.current-super-version-number`      | Current super-version number, incremented on each LSM change.                                                                                                                                                                 | ticker |
| `rocksdb.estimate-live-data-size`           | Estimated size in bytes of the live data.                                                                                                                                                                                     | ticker |
| `rocksdb.estimate-num-keys`                 | Estimated number of live keys.                                                                                                                                                                                                | ticker |
| `rocksdb.estimate-pending-compaction-bytes` | Estimated bytes compaction must rewrite to rebalance the LSM tree.                                                                                                                                                            | ticker |
| `rocksdb.live-blob-file-size`               | Total size in bytes of blob files in the current version.                                                                                                                                                                     | ticker |
| `rocksdb.live-sst-files-size`               | Total size in bytes of SST files in the current version.                                                                                                                                                                      | ticker |
| `rocksdb.mem-table-flush-pending`           | `1` if a memtable flush is pending, else `0`.                                                                                                                                                                                 | ticker |
| `rocksdb.num-blob-files`                    | Number of blob files in the current version.                                                                                                                                                                                  | ticker |
| `rocksdb.num-deletes-active-mem-table`      | Number of delete entries in the active memtable.                                                                                                                                                                              | ticker |
| `rocksdb.num-entries-active-mem-table`      | Number of entries in the active memtable.                                                                                                                                                                                     | ticker |
| `rocksdb.num-immutable-mem-table`           | Number of immutable memtables not yet flushed.                                                                                                                                                                                | ticker |
| `rocksdb.num-immutable-mem-table-flushed`   | Number of immutable memtables already flushed.                                                                                                                                                                                | ticker |
| `rocksdb.num-live-versions`                 | Number of live LSM versions (high values indicate versions held by iterators/snapshots).                                                                                                                                      | ticker |
| `rocksdb.num-running-compactions`           | Number of compactions currently running.                                                                                                                                                                                      | ticker |
| `rocksdb.num-running-flushes`               | Number of flushes currently running.                                                                                                                                                                                          | ticker |
| `rocksdb.oldest-snapshot-time`              | Unix timestamp of the oldest unreleased snapshot, or `0`.                                                                                                                                                                     | ticker |
| `rocksdb.size-all-mem-tables`               | Approximate size in bytes of active, unflushed, and pinned memtables.                                                                                                                                                         | ticker |
| `rocksdb.total-blob-file-size`              | Total size in bytes of all blob files across all versions.                                                                                                                                                                    | ticker |
| `rocksdb.total-sst-files-size`              | Total size in bytes of all SST files across all versions.                                                                                                                                                                     | ticker |
| `txnlog.activeMaps`                         | Number of transaction log files currently memory-mapped, summed across all logs.                                                                                                                                              | ticker |
| `txnlog.bytesWritten`                       | Cumulative bytes written to transaction logs (entry payload plus entry header, excluding file headers), summed across all logs.                                                                                               | ticker |
| `txnlog.fileCount`                          | Total number of transaction log files on disk, summed across all logs.                                                                                                                                                        | ticker |
| `txnlog.logCount`                           | Number of transaction log stores attached to the database.                                                                                                                                                                    | ticker |
| `txnlog.mappedBytes`                        | Virtual address space in bytes reserved for memory-mapped log files, summed across all logs (on POSIX the active write file maps the full configured `maxFileSize`, so this is allocated address space, not resident memory). | ticker |
| `txnlog.overlayBytes`                       | File-backed overlay portion in bytes (POSIX only; `0` on Windows), summed across all logs — a closer proxy for real memory consumption than `mappedBytes`.                                                                    | ticker |
| `txnlog.pendingTransactions`                | Number of transactions bound to a log but not yet written via `writeBatch()`, summed across all logs.                                                                                                                         | ticker |
| `txnlog.replayGapBytes`                     | Bytes between the last flushed position and the write head (written but not yet flushed to the database), summed across all logs.                                                                                             | ticker |
| `txnlog.totalSizeBytes`                     | Total on-disk size in bytes of all transaction log files, summed across all logs.                                                                                                                                             | ticker |
| `txnlog.transactionsWritten`                | Cumulative number of transactions successfully written to the logs (lifetime total), summed across all logs.                                                                                                                  | ticker |
| `txnlog.uncommittedTransactions`            | Number of transactions written to a log but not yet committed to RocksDB, summed across all logs.                                                                                                                             | ticker |

### Basic + Curated Stats

If `enableStats` is `true` when the database was opened, then `db.getStats()`
returns everything above plus the following curated list of stats:

| Name                                        | Description                                                                                                                  | Type      |
| ------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | --------- |
| `rocksdb.blobdb.value.size`                 | Distribution of blob value sizes.                                                                                            | histogram |
| `rocksdb.block.cache.data.hit`              | Block cache hits for data blocks.                                                                                            | ticker    |
| `rocksdb.block.cache.data.miss`             | Block cache misses for data blocks.                                                                                          | ticker    |
| `rocksdb.block.cache.filter.hit`            | Block cache hits for filter blocks.                                                                                          | ticker    |
| `rocksdb.block.cache.filter.miss`           | Block cache misses for filter blocks.                                                                                        | ticker    |
| `rocksdb.block.cache.hit`                   | Total block cache hits across all block types.                                                                               | ticker    |
| `rocksdb.block.cache.index.hit`             | Block cache hits for index blocks.                                                                                           | ticker    |
| `rocksdb.block.cache.index.miss`            | Block cache misses for index blocks.                                                                                         | ticker    |
| `rocksdb.block.cache.miss`                  | Total block cache misses across all block types.                                                                             | ticker    |
| `rocksdb.bloom.filter.full.positive`        | Times a full bloom filter reported a key as possibly present.                                                                | ticker    |
| `rocksdb.bloom.filter.full.true.positive`   | Times a full bloom filter positive turned out to be a true positive (the key actually existed).                              | ticker    |
| `rocksdb.bloom.filter.useful`               | Times the bloom filter avoided reading a data block for a non-existent key.                                                  | ticker    |
| `rocksdb.bytes.read`                        | Total uncompressed bytes read by `Get()`, `MultiGet()`, and iterator operations.                                             | ticker    |
| `rocksdb.bytes.written`                     | Total uncompressed bytes written via `WriteBatch` payloads.                                                                  | ticker    |
| `rocksdb.compact.read.bytes`                | Bytes read during compaction.                                                                                                | ticker    |
| `rocksdb.compact.write.bytes`               | Bytes written during compaction.                                                                                             | ticker    |
| `rocksdb.compaction.cancelled`              | Number of compactions cancelled (e.g. on shutdown or manual cancellation).                                                   | ticker    |
| `rocksdb.compaction.times.micros`           | Distribution of total compaction durations (microseconds).                                                                   | histogram |
| `rocksdb.db.flush.micros`                   | Distribution of memtable flush durations (microseconds).                                                                     | histogram |
| `rocksdb.db.get.micros`                     | Distribution of `Get()` latencies (microseconds).                                                                            | histogram |
| `rocksdb.db.iter.bytes.read`                | Total bytes (key plus value) read through iterators.                                                                         | ticker    |
| `rocksdb.db.seek.micros`                    | Distribution of iterator `Seek()` / `SeekToFirst()` / `SeekToLast()` latencies (microseconds).                               | histogram |
| `rocksdb.db.write.micros`                   | Distribution of `Write()` latencies (microseconds).                                                                          | histogram |
| `rocksdb.db.write.stall`                    | Distribution of write stall durations (microseconds).                                                                        | histogram |
| `rocksdb.memtable.hit`                      | Number of reads served from the memtable.                                                                                    | ticker    |
| `rocksdb.memtable.miss`                     | Number of reads that missed the memtable and fell through to SST files.                                                      | ticker    |
| `rocksdb.no.file.errors`                    | Number of errors encountered while reading from files.                                                                       | ticker    |
| `rocksdb.number.keys.read`                  | Total number of keys read via `Get()` and `MultiGet()`.                                                                      | ticker    |
| `rocksdb.number.keys.written`               | Total number of keys written.                                                                                                | ticker    |
| `rocksdb.number.reseeks.iteration`          | Number of internal reseeks performed during iteration (e.g. skipping past many tombstones or stale versions).                | ticker    |
| `rocksdb.read.amp.estimate.useful.bytes`    | Estimated useful bytes read (read-amplification accounting; requires `read_amp_bytes_per_bit`).                              | ticker    |
| `rocksdb.read.amp.total.read.bytes`         | Total bytes read (read-amplification accounting).                                                                            | ticker    |
| `rocksdb.sst.read.micros`                   | Distribution of SST file read latencies (microseconds).                                                                      | histogram |
| `rocksdb.stall.micros`                      | Total microseconds writes were stalled.                                                                                      | ticker    |
| `rocksdb.txn.overhead.mutex.old.commit.map` | Number of times the old-commit-map mutex was acquired for pessimistic-transaction commit bookkeeping (overhead accounting).  | ticker    |
| `rocksdb.txn.overhead.mutex.prepare`        | Number of times the prepare-heap mutex was acquired for pessimistic-transaction prepare bookkeeping (overhead accounting).   | ticker    |
| `rocksdb.txn.overhead.mutex.snapshot`       | Number of times the snapshot-list mutex was acquired for pessimistic-transaction snapshot bookkeeping (overhead accounting). | ticker    |

### All Stats

If `enableStats` is `true` and pass `true` into `db.getStats(true)` returns all
stats which includes all of the stats above plus the following:

| Name                                                   | Description                                                                                          | Type      |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------- | --------- |
| `rocksdb.async.prefetch.abort.micros`                  | Distribution of time spent aborting outstanding async prefetch requests (microseconds).              | histogram |
| `rocksdb.async.read.bytes`                             | Distribution of bytes read per asynchronous read request.                                            | histogram |
| `rocksdb.async.read.error.count`                       | Number of errors returned by asynchronous read requests.                                             | ticker    |
| `rocksdb.backup.read.bytes`                            | Bytes read while creating backups.                                                                   | ticker    |
| `rocksdb.backup.write.bytes`                           | Bytes written while creating backups.                                                                | ticker    |
| `rocksdb.blobdb.blob.file.bytes.read`                  | Bytes read from blob files.                                                                          | ticker    |
| `rocksdb.blobdb.blob.file.bytes.written`               | Bytes written to blob files.                                                                         | ticker    |
| `rocksdb.blobdb.blob.file.read.micros`                 | Distribution of blob file read latencies (microseconds).                                             | histogram |
| `rocksdb.blobdb.blob.file.sync.micros`                 | Distribution of blob file sync latencies (microseconds).                                             | histogram |
| `rocksdb.blobdb.blob.file.synced`                      | Number of blob file syncs.                                                                           | ticker    |
| `rocksdb.blobdb.blob.file.write.micros`                | Distribution of blob file write latencies (microseconds).                                            | histogram |
| `rocksdb.blobdb.blob.index.evicted.count`              | Number of blob index entries evicted from the base DB by garbage collection (legacy stacked BlobDB). | ticker    |
| `rocksdb.blobdb.blob.index.evicted.size`               | Size in bytes of blobs evicted by garbage collection (legacy stacked BlobDB).                        | ticker    |
| `rocksdb.blobdb.blob.index.expired.count`              | Number of blobs expired due to TTL (legacy stacked BlobDB).                                          | ticker    |
| `rocksdb.blobdb.blob.index.expired.size`               | Size in bytes of blobs expired due to TTL (legacy stacked BlobDB).                                   | ticker    |
| `rocksdb.blobdb.bytes.read`                            | Total bytes read from blobs (legacy stacked BlobDB).                                                 | ticker    |
| `rocksdb.blobdb.bytes.written`                         | Total bytes written to blobs (legacy stacked BlobDB).                                                | ticker    |
| `rocksdb.blobdb.cache.add`                             | Number of blobs added to the blob cache.                                                             | ticker    |
| `rocksdb.blobdb.cache.add.failures`                    | Number of failures adding blobs to the blob cache.                                                   | ticker    |
| `rocksdb.blobdb.cache.bytes.read`                      | Bytes read from the blob cache.                                                                      | ticker    |
| `rocksdb.blobdb.cache.bytes.write`                     | Bytes written to the blob cache.                                                                     | ticker    |
| `rocksdb.blobdb.cache.hit`                             | Blob cache hits.                                                                                     | ticker    |
| `rocksdb.blobdb.cache.miss`                            | Blob cache misses.                                                                                   | ticker    |
| `rocksdb.blobdb.compression.micros`                    | Distribution of blob compression latencies (microseconds).                                           | histogram |
| `rocksdb.blobdb.decompression.micros`                  | Distribution of blob decompression latencies (microseconds).                                         | histogram |
| `rocksdb.blobdb.fifo.bytes.evicted`                    | Bytes evicted by the FIFO eviction policy (legacy stacked BlobDB).                                   | ticker    |
| `rocksdb.blobdb.fifo.num.files.evicted`                | Number of blob files evicted by the FIFO eviction policy (legacy stacked BlobDB).                    | ticker    |
| `rocksdb.blobdb.fifo.num.keys.evicted`                 | Number of keys evicted by the FIFO eviction policy (legacy stacked BlobDB).                          | ticker    |
| `rocksdb.blobdb.gc.bytes.relocated`                    | Bytes of valid blobs relocated during blob garbage collection.                                       | ticker    |
| `rocksdb.blobdb.gc.failures`                           | Number of blob garbage collection failures.                                                          | ticker    |
| `rocksdb.blobdb.gc.num.files`                          | Number of blob files obsoleted by garbage collection (legacy stacked BlobDB).                        | ticker    |
| `rocksdb.blobdb.gc.num.keys.relocated`                 | Number of valid blobs relocated during blob garbage collection.                                      | ticker    |
| `rocksdb.blobdb.gc.num.new.files`                      | Number of new blob files generated by garbage collection (legacy stacked BlobDB).                    | ticker    |
| `rocksdb.blobdb.get.micros`                            | Distribution of `Get()` latencies (microseconds) (legacy stacked BlobDB).                            | histogram |
| `rocksdb.blobdb.key.size`                              | Distribution of key sizes for BlobDB operations.                                                     | histogram |
| `rocksdb.blobdb.multiget.micros`                       | Distribution of `MultiGet()` latencies (microseconds) (legacy stacked BlobDB).                       | histogram |
| `rocksdb.blobdb.next.micros`                           | Distribution of iterator `Next()` latencies (microseconds) (legacy stacked BlobDB).                  | histogram |
| `rocksdb.blobdb.num.get`                               | Number of `Get()` calls (legacy stacked BlobDB).                                                     | ticker    |
| `rocksdb.blobdb.num.keys.read`                         | Number of keys read (legacy stacked BlobDB).                                                         | ticker    |
| `rocksdb.blobdb.num.keys.written`                      | Number of keys written (legacy stacked BlobDB).                                                      | ticker    |
| `rocksdb.blobdb.num.multiget`                          | Number of `MultiGet()` calls (legacy stacked BlobDB).                                                | ticker    |
| `rocksdb.blobdb.num.next`                              | Number of iterator `Next()` calls (legacy stacked BlobDB).                                           | ticker    |
| `rocksdb.blobdb.num.prev`                              | Number of iterator `Prev()` calls (legacy stacked BlobDB).                                           | ticker    |
| `rocksdb.blobdb.num.put`                               | Number of `Put()` calls (legacy stacked BlobDB).                                                     | ticker    |
| `rocksdb.blobdb.num.seek`                              | Number of iterator `Seek()` calls (legacy stacked BlobDB).                                           | ticker    |
| `rocksdb.blobdb.num.write`                             | Number of `Write()` calls (legacy stacked BlobDB).                                                   | ticker    |
| `rocksdb.blobdb.prev.micros`                           | Distribution of iterator `Prev()` latencies (microseconds) (legacy stacked BlobDB).                  | histogram |
| `rocksdb.blobdb.seek.micros`                           | Distribution of iterator `Seek()` latencies (microseconds) (legacy stacked BlobDB).                  | histogram |
| `rocksdb.blobdb.write.blob`                            | Number of (non-TTL) values written to blob files (legacy stacked BlobDB).                            | ticker    |
| `rocksdb.blobdb.write.blob.ttl`                        | Number of TTL values written to blob files (legacy stacked BlobDB).                                  | ticker    |
| `rocksdb.blobdb.write.inlined`                         | Number of (non-TTL) values inlined in the base DB instead of a blob file (legacy stacked BlobDB).    | ticker    |
| `rocksdb.blobdb.write.inlined.ttl`                     | Number of TTL values inlined in the base DB instead of a blob file (legacy stacked BlobDB).          | ticker    |
| `rocksdb.blobdb.write.micros`                          | Distribution of `Write()` latencies (microseconds) (legacy stacked BlobDB).                          | histogram |
| `rocksdb.block.cache.add`                              | Number of blocks added to the block cache.                                                           | ticker    |
| `rocksdb.block.cache.add.failures`                     | Number of failures adding blocks to the block cache.                                                 | ticker    |
| `rocksdb.block.cache.add.redundant`                    | Number of redundant block cache insertions (a block added concurrently by another thread).           | ticker    |
| `rocksdb.block.cache.bytes.read`                       | Bytes read from the block cache.                                                                     | ticker    |
| `rocksdb.block.cache.bytes.write`                      | Bytes inserted into the block cache.                                                                 | ticker    |
| `rocksdb.block.cache.compression.dict.add`             | Number of compression-dictionary blocks added to the block cache.                                    | ticker    |
| `rocksdb.block.cache.compression.dict.add.redundant`   | Number of redundant compression-dictionary block insertions.                                         | ticker    |
| `rocksdb.block.cache.compression.dict.bytes.insert`    | Bytes of compression-dictionary blocks inserted into the block cache.                                | ticker    |
| `rocksdb.block.cache.compression.dict.hit`             | Block cache hits for compression-dictionary blocks.                                                  | ticker    |
| `rocksdb.block.cache.compression.dict.miss`            | Block cache misses for compression-dictionary blocks.                                                | ticker    |
| `rocksdb.block.cache.data.add`                         | Number of data blocks added to the block cache.                                                      | ticker    |
| `rocksdb.block.cache.data.add.redundant`               | Number of redundant data block insertions.                                                           | ticker    |
| `rocksdb.block.cache.data.bytes.insert`                | Bytes of data blocks inserted into the block cache.                                                  | ticker    |
| `rocksdb.block.cache.filter.add`                       | Number of filter blocks added to the block cache.                                                    | ticker    |
| `rocksdb.block.cache.filter.add.redundant`             | Number of redundant filter block insertions.                                                         | ticker    |
| `rocksdb.block.cache.filter.bytes.insert`              | Bytes of filter blocks inserted into the block cache.                                                | ticker    |
| `rocksdb.block.cache.index.add`                        | Number of index blocks added to the block cache.                                                     | ticker    |
| `rocksdb.block.cache.index.add.redundant`              | Number of redundant index block insertions.                                                          | ticker    |
| `rocksdb.block.cache.index.bytes.insert`               | Bytes of index blocks inserted into the block cache.                                                 | ticker    |
| `rocksdb.block.checksum.compute.count`                 | Number of block checksums computed while reading blocks.                                             | ticker    |
| `rocksdb.block.checksum.mismatch.count`                | Number of block checksum mismatches detected (corruption).                                           | ticker    |
| `rocksdb.block.key.distribution.cv`                    | Distribution of the coefficient of variation of key sizes within data blocks.                        | histogram |
| `rocksdb.bloom.filter.prefix.checked`                  | Number of times the prefix bloom filter was checked.                                                 | ticker    |
| `rocksdb.bloom.filter.prefix.true.positive`            | Number of prefix bloom filter positives that were true positives.                                    | ticker    |
| `rocksdb.bloom.filter.prefix.useful`                   | Number of times the prefix bloom filter avoided a read.                                              | ticker    |
| `rocksdb.bytes.compressed.from`                        | Uncompressed input bytes fed into block compression.                                                 | ticker    |
| `rocksdb.bytes.compressed.to`                          | Compressed output bytes produced by block compression.                                               | ticker    |
| `rocksdb.bytes.compression_bypassed`                   | Uncompressed bytes for blocks that bypassed compression (e.g. below the size threshold).             | ticker    |
| `rocksdb.bytes.compression.rejected`                   | Uncompressed bytes for blocks where compression was rejected for an insufficient ratio.              | ticker    |
| `rocksdb.bytes.decompressed.from`                      | Compressed input bytes fed into block decompression.                                                 | ticker    |
| `rocksdb.bytes.decompressed.to`                        | Uncompressed output bytes produced by block decompression.                                           | ticker    |
| `rocksdb.bytes.per.multiget`                           | Distribution of total bytes read per `MultiGet()` call.                                              | histogram |
| `rocksdb.bytes.per.read`                               | Distribution of bytes read per `Get()` call.                                                         | histogram |
| `rocksdb.bytes.per.write`                              | Distribution of bytes written per `Write()` call.                                                    | histogram |
| `rocksdb.cold.file.read.bytes`                         | Bytes read from files with the `cold` temperature.                                                   | ticker    |
| `rocksdb.cold.file.read.count`                         | Number of reads from files with the `cold` temperature.                                              | ticker    |
| `rocksdb.compact.read.marked.bytes`                    | Bytes read while compacting files marked for compaction.                                             | ticker    |
| `rocksdb.compact.read.periodic.bytes`                  | Bytes read during periodic compaction.                                                               | ticker    |
| `rocksdb.compact.read.ttl.bytes`                       | Bytes read during TTL-triggered compaction.                                                          | ticker    |
| `rocksdb.compact.write.marked.bytes`                   | Bytes written while compacting files marked for compaction.                                          | ticker    |
| `rocksdb.compact.write.periodic.bytes`                 | Bytes written during periodic compaction.                                                            | ticker    |
| `rocksdb.compact.write.ttl.bytes`                      | Bytes written during TTL-triggered compaction.                                                       | ticker    |
| `rocksdb.compaction.aborted`                           | Number of compactions aborted.                                                                       | ticker    |
| `rocksdb.compaction.key.drop.new`                      | Keys dropped during compaction because a newer version of the key exists.                            | ticker    |
| `rocksdb.compaction.key.drop.obsolete`                 | Keys dropped during compaction because they are obsolete (deleted or expired).                       | ticker    |
| `rocksdb.compaction.key.drop.range_del`                | Keys dropped during compaction by range deletions.                                                   | ticker    |
| `rocksdb.compaction.key.drop.user`                     | Keys dropped during compaction by the user-defined compaction filter.                                | ticker    |
| `rocksdb.compaction.optimized.del.drop.obsolete`       | Obsolete keys dropped via the optimized deletion path during compaction.                             | ticker    |
| `rocksdb.compaction.outfile.sync.micros`               | Distribution of compaction output-file sync latencies (microseconds).                                | histogram |
| `rocksdb.compaction.prefetch.bytes`                    | Distribution of bytes prefetched during compaction reads.                                            | histogram |
| `rocksdb.compaction.range_del.drop.obsolete`           | Range-deletion tombstones dropped as obsolete during compaction.                                     | ticker    |
| `rocksdb.compaction.times.cpu_micros`                  | Distribution of compaction CPU times (microseconds).                                                 | histogram |
| `rocksdb.compaction.total.time.cpu_micros`             | Total CPU time spent in compaction (microseconds).                                                   | ticker    |
| `rocksdb.compressed.secondary.cache.dummy.hits`        | Dummy (placeholder) hits in the compressed secondary cache.                                          | ticker    |
| `rocksdb.compressed.secondary.cache.hits`              | Hits in the compressed secondary cache.                                                              | ticker    |
| `rocksdb.compressed.secondary.cache.promotion.skips`   | Entries not promoted from the compressed secondary cache to the primary cache.                       | ticker    |
| `rocksdb.compressed.secondary.cache.promotions`        | Entries promoted from the compressed secondary cache to the primary cache.                           | ticker    |
| `rocksdb.compression.times.nanos`                      | Distribution of block compression times (nanoseconds).                                               | histogram |
| `rocksdb.cool.file.read.bytes`                         | Bytes read from files with the `cool` temperature.                                                   | ticker    |
| `rocksdb.cool.file.read.count`                         | Number of reads from files with the `cool` temperature.                                              | ticker    |
| `rocksdb.db.multiget.micros`                           | Distribution of `MultiGet()` latencies (microseconds).                                               | histogram |
| `rocksdb.db.mutex.wait.micros`                         | Total time threads spent waiting on the DB mutex (microseconds).                                     | ticker    |
| `rocksdb.decompression.times.nanos`                    | Distribution of block decompression times (nanoseconds).                                             | histogram |
| `rocksdb.error.handler.autoresume.count`               | Number of automatic recovery attempts after a background error.                                      | ticker    |
| `rocksdb.error.handler.autoresume.retry.count`         | Distribution of retries per automatic recovery attempt.                                              | histogram |
| `rocksdb.error.handler.autoresume.retry.total.count`   | Total automatic recovery retries across all attempts.                                                | ticker    |
| `rocksdb.error.handler.autoresume.success.count`       | Number of successful automatic recoveries.                                                           | ticker    |
| `rocksdb.error.handler.bg.error.count`                 | Number of background errors flagged by the error handler.                                            | ticker    |
| `rocksdb.error.handler.bg.io.error.count`              | Number of background I/O errors flagged by the error handler.                                        | ticker    |
| `rocksdb.error.handler.bg.retryable.io.error.count`    | Number of retryable background I/O errors flagged by the error handler.                              | ticker    |
| `rocksdb.fifo.change_temperature.compactions`          | Number of FIFO compactions triggered to change file temperature.                                     | ticker    |
| `rocksdb.fifo.max.size.compactions`                    | Number of FIFO compactions triggered by exceeding the maximum size.                                  | ticker    |
| `rocksdb.fifo.ttl.compactions`                         | Number of FIFO compactions triggered by TTL.                                                         | ticker    |
| `rocksdb.file.read.compaction.micros`                  | Distribution of file read latencies during compaction (microseconds).                                | histogram |
| `rocksdb.file.read.corruption.retry.count`             | Number of file read retries triggered by corruption.                                                 | ticker    |
| `rocksdb.file.read.corruption.retry.success.count`     | Number of file read corruption retries that succeeded.                                               | ticker    |
| `rocksdb.file.read.db.iterator.micros`                 | Distribution of file read latencies during iteration (microseconds).                                 | histogram |
| `rocksdb.file.read.db.open.micros`                     | Distribution of file read latencies during DB open (microseconds).                                   | histogram |
| `rocksdb.file.read.flush.micros`                       | Distribution of file read latencies during flush (microseconds).                                     | histogram |
| `rocksdb.file.read.get.micros`                         | Distribution of file read latencies during `Get()` (microseconds).                                   | histogram |
| `rocksdb.file.read.multiget.micros`                    | Distribution of file read latencies during `MultiGet()` (microseconds).                              | histogram |
| `rocksdb.file.read.verify.db.checksum.micros`          | Distribution of file read latencies during full-DB checksum verification (microseconds).             | histogram |
| `rocksdb.file.read.verify.file.checksums.micros`       | Distribution of file read latencies during per-file checksum verification (microseconds).            | histogram |
| `rocksdb.file.write.compaction.micros`                 | Distribution of file write latencies during compaction (microseconds).                               | histogram |
| `rocksdb.file.write.db.open.micros`                    | Distribution of file write latencies during DB open (microseconds).                                  | histogram |
| `rocksdb.file.write.flush.micros`                      | Distribution of file write latencies during flush (microseconds).                                    | histogram |
| `rocksdb.files.deleted.immediately`                    | Number of files deleted immediately rather than via rate-limited (trash) deletion.                   | ticker    |
| `rocksdb.files.marked.trash`                           | Number of files marked as trash for rate-limited deletion.                                           | ticker    |
| `rocksdb.files.marked.trash.deleted`                   | Number of trash-marked files that were subsequently deleted.                                         | ticker    |
| `rocksdb.filter.operation.time.nanos`                  | Total time spent in filter operations (nanoseconds).                                                 | ticker    |
| `rocksdb.flush.write.bytes`                            | Bytes written by memtable flushes.                                                                   | ticker    |
| `rocksdb.footer.corruption.count`                      | Number of SST footer corruptions detected.                                                           | ticker    |
| `rocksdb.getupdatessince.calls`                        | Number of `GetUpdatesSince()` calls (WAL reads, e.g. for replication).                               | ticker    |
| `rocksdb.hot.file.read.bytes`                          | Bytes read from files with the `hot` temperature.                                                    | ticker    |
| `rocksdb.hot.file.read.count`                          | Number of reads from files with the `hot` temperature.                                               | ticker    |
| `rocksdb.ice.file.read.bytes`                          | Bytes read from files with the `ice` temperature.                                                    | ticker    |
| `rocksdb.ice.file.read.count`                          | Number of reads from files with the `ice` temperature.                                               | ticker    |
| `rocksdb.l0.hit`                                       | Number of `Get()` reads satisfied at level 0.                                                        | ticker    |
| `rocksdb.l1.hit`                                       | Number of `Get()` reads satisfied at level 1.                                                        | ticker    |
| `rocksdb.l2andup.hit`                                  | Number of `Get()` reads satisfied at level 2 or higher.                                              | ticker    |
| `rocksdb.last.level.read.bytes`                        | Bytes read from the last (bottommost) level.                                                         | ticker    |
| `rocksdb.last.level.read.count`                        | Number of reads from the last (bottommost) level.                                                    | ticker    |
| `rocksdb.last.level.seek.data`                         | Number of seeks that accessed data in the last level.                                                | ticker    |
| `rocksdb.last.level.seek.data.useful.filter.match`     | Last-level seeks that returned useful data following a filter match.                                 | ticker    |
| `rocksdb.last.level.seek.data.useful.no.filter`        | Last-level seeks that returned useful data with no filter present.                                   | ticker    |
| `rocksdb.last.level.seek.filter.match`                 | Last-level seeks where the bloom filter matched.                                                     | ticker    |
| `rocksdb.last.level.seek.filtered`                     | Last-level seeks filtered out by the bloom filter.                                                   | ticker    |
| `rocksdb.manifest.file.sync.micros`                    | Distribution of MANIFEST file sync latencies (microseconds).                                         | histogram |
| `rocksdb.memtable.garbage.bytes.at.flush`              | Bytes of garbage (overwritten or deleted entries) in the memtable at flush time.                     | ticker    |
| `rocksdb.memtable.payload.bytes.at.flush`              | Bytes of live payload in the memtable at flush time.                                                 | ticker    |
| `rocksdb.merge.operation.time.nanos`                   | Total time spent in merge operator operations (nanoseconds).                                         | ticker    |
| `rocksdb.multiget.coroutine.count`                     | Number of `MultiGet` operations using the coroutine-based async path.                                | ticker    |
| `rocksdb.multiget.io.batch.size`                       | Distribution of I/O batch sizes for `MultiGet`.                                                      | histogram |
| `rocksdb.multiscan.blocks.from.cache`                  | Number of blocks served from cache during multi-scan operations.                                     | ticker    |
| `rocksdb.multiscan.blocks.per.prepare`                 | Distribution of blocks accessed per multi-scan prepare.                                              | histogram |
| `rocksdb.multiscan.blocks.prefetched`                  | Number of blocks prefetched for multi-scan operations.                                               | ticker    |
| `rocksdb.multiscan.io.coalesced.nonadjacent`           | Number of non-adjacent I/O requests coalesced during multi-scan.                                     | ticker    |
| `rocksdb.multiscan.io.requests`                        | Number of I/O requests issued for multi-scan operations.                                             | ticker    |
| `rocksdb.multiscan.op.prepare.iterators.micros`        | Distribution of time spent preparing iterators for multi-scan (microseconds).                        | histogram |
| `rocksdb.multiscan.prefetch.blocks.wasted`             | Number of prefetched multi-scan blocks that went unused.                                             | ticker    |
| `rocksdb.multiscan.prefetch.bytes`                     | Bytes prefetched for multi-scan operations.                                                          | ticker    |
| `rocksdb.multiscan.prepare.calls`                      | Number of multi-scan prepare calls.                                                                  | ticker    |
| `rocksdb.multiscan.prepare.errors`                     | Number of errors encountered during multi-scan prepare.                                              | ticker    |
| `rocksdb.multiscan.prepare.micros`                     | Distribution of multi-scan prepare latencies (microseconds).                                         | histogram |
| `rocksdb.multiscan.seek.errors`                        | Number of seek errors during multi-scan.                                                             | ticker    |
| `rocksdb.no.file.opens`                                | Number of successful SST file opens.                                                                 | ticker    |
| `rocksdb.non.last.level.read.bytes`                    | Bytes read from non-last (non-bottommost) levels.                                                    | ticker    |
| `rocksdb.non.last.level.read.count`                    | Number of reads from non-last (non-bottommost) levels.                                               | ticker    |
| `rocksdb.non.last.level.seek.data`                     | Number of seeks that accessed data in non-last levels.                                               | ticker    |
| `rocksdb.non.last.level.seek.data.useful.filter.match` | Non-last-level seeks that returned useful data following a filter match.                             | ticker    |
| `rocksdb.non.last.level.seek.data.useful.no.filter`    | Non-last-level seeks that returned useful data with no filter present.                               | ticker    |
| `rocksdb.non.last.level.seek.filter.match`             | Non-last-level seeks where the bloom filter matched.                                                 | ticker    |
| `rocksdb.non.last.level.seek.filtered`                 | Non-last-level seeks filtered out by the bloom filter.                                               | ticker    |
| `rocksdb.num.index.and.filter.blocks.read.per.level`   | Distribution of index and filter blocks read per level per operation.                                | histogram |
| `rocksdb.num.iterator.created`                         | Number of iterators created.                                                                         | ticker    |
| `rocksdb.num.iterator.deleted`                         | Number of iterators destroyed.                                                                       | ticker    |
| `rocksdb.num.level.read.per.multiget`                  | Distribution of levels read per `MultiGet` call.                                                     | histogram |
| `rocksdb.num.op.per.transaction`                       | Distribution of operations per transaction.                                                          | histogram |
| `rocksdb.num.sst.read.per.level`                       | Distribution of SST files read per level per operation.                                              | histogram |
| `rocksdb.num.subcompactions.scheduled`                 | Distribution of subcompactions scheduled per compaction.                                             | histogram |
| `rocksdb.number.block_compression_bypassed`            | Number of blocks that bypassed compression.                                                          | ticker    |
| `rocksdb.number.block_compression_rejected`            | Number of blocks where compression was rejected.                                                     | ticker    |
| `rocksdb.number.block.compressed`                      | Number of blocks compressed.                                                                         | ticker    |
| `rocksdb.number.block.decompressed`                    | Number of blocks decompressed.                                                                       | ticker    |
| `rocksdb.number.db.next`                               | Number of iterator `Next()` calls.                                                                   | ticker    |
| `rocksdb.number.db.next.found`                         | Number of iterator `Next()` calls that returned a key.                                               | ticker    |
| `rocksdb.number.db.prev`                               | Number of iterator `Prev()` calls.                                                                   | ticker    |
| `rocksdb.number.db.prev.found`                         | Number of iterator `Prev()` calls that returned a key.                                               | ticker    |
| `rocksdb.number.db.seek`                               | Number of iterator `Seek()` calls.                                                                   | ticker    |
| `rocksdb.number.db.seek.found`                         | Number of iterator `Seek()` calls that landed on a key.                                              | ticker    |
| `rocksdb.number.direct.load.table.properties`          | Number of times table properties were loaded directly, bypassing the cache.                          | ticker    |
| `rocksdb.number.iter.skip`                             | Number of internal entries skipped during iteration (e.g. tombstones, stale versions).               | ticker    |
| `rocksdb.number.keys.updated`                          | Number of keys updated in place.                                                                     | ticker    |
| `rocksdb.number.merge.failures`                        | Number of merge operator failures.                                                                   | ticker    |
| `rocksdb.number.multiget.bytes.read`                   | Total bytes read by `MultiGet()` calls.                                                              | ticker    |
| `rocksdb.number.multiget.get`                          | Number of `MultiGet()` calls.                                                                        | ticker    |
| `rocksdb.number.multiget.keys.found`                   | Number of keys found by `MultiGet()` calls.                                                          | ticker    |
| `rocksdb.number.multiget.keys.read`                    | Number of keys requested across `MultiGet()` calls.                                                  | ticker    |
| `rocksdb.number.rate_limiter.drains`                   | Number of times the rate limiter was fully drained.                                                  | ticker    |
| `rocksdb.number.superversion_acquires`                 | Number of superversion acquisitions.                                                                 | ticker    |
| `rocksdb.number.superversion_cleanups`                 | Number of superversion cleanups.                                                                     | ticker    |
| `rocksdb.number.superversion_releases`                 | Number of superversion releases.                                                                     | ticker    |
| `rocksdb.number.wbwi.ingest`                           | Number of `WriteBatchWithIndex` entries ingested into the DB.                                        | ticker    |
| `rocksdb.numfiles.in.singlecompaction`                 | Distribution of the number of files in a single compaction.                                          | histogram |
| `rocksdb.persistent.cache.hit`                         | Persistent cache hits.                                                                               | ticker    |
| `rocksdb.persistent.cache.miss`                        | Persistent cache misses.                                                                             | ticker    |
| `rocksdb.poll.wait.micros`                             | Distribution of time spent polling for async read completion (microseconds).                         | histogram |
| `rocksdb.prefetch.bytes`                               | Bytes prefetched.                                                                                    | ticker    |
| `rocksdb.prefetch.bytes.useful`                        | Prefetched bytes that were actually used.                                                            | ticker    |
| `rocksdb.prefetch.hits`                                | Number of reads satisfied from prefetched data.                                                      | ticker    |
| `rocksdb.prefetch.memory.bytes.granted`                | Bytes of prefetch buffer memory granted.                                                             | ticker    |
| `rocksdb.prefetch.memory.bytes.released`               | Bytes of prefetch buffer memory released.                                                            | ticker    |
| `rocksdb.prefetch.memory.requests.blocked`             | Number of prefetch memory requests blocked by the memory limit.                                      | ticker    |
| `rocksdb.prefetched.bytes.discarded`                   | Distribution of prefetched bytes that were discarded unused.                                         | histogram |
| `rocksdb.read.async.micros`                            | Total time spent in asynchronous reads (microseconds).                                               | ticker    |
| `rocksdb.read.block.compaction.micros`                 | Distribution of block read latencies during compaction (microseconds).                               | histogram |
| `rocksdb.read.block.get.micros`                        | Distribution of block read latencies during `Get()` (microseconds).                                  | histogram |
| `rocksdb.read.num.merge_operands`                      | Distribution of the number of merge operands read per query.                                         | histogram |
| `rocksdb.readahead.trimmed`                            | Number of times the readahead size was trimmed.                                                      | ticker    |
| `rocksdb.remote.compact.read.bytes`                    | Bytes read by remote (offloaded) compactions.                                                        | ticker    |
| `rocksdb.remote.compact.resumed.bytes`                 | Bytes processed by resumed remote compactions.                                                       | ticker    |
| `rocksdb.remote.compact.write.bytes`                   | Bytes written by remote (offloaded) compactions.                                                     | ticker    |
| `rocksdb.row.cache.hit`                                | Row cache hits.                                                                                      | ticker    |
| `rocksdb.row.cache.miss`                               | Row cache misses.                                                                                    | ticker    |
| `rocksdb.secondary.cache.data.hits`                    | Secondary cache hits for data blocks.                                                                | ticker    |
| `rocksdb.secondary.cache.filter.hits`                  | Secondary cache hits for filter blocks.                                                              | ticker    |
| `rocksdb.secondary.cache.hits`                         | Total secondary cache hits.                                                                          | ticker    |
| `rocksdb.secondary.cache.index.hits`                   | Secondary cache hits for index blocks.                                                               | ticker    |
| `rocksdb.sim.block.cache.hit`                          | Simulated block cache hits (from the cache simulator).                                               | ticker    |
| `rocksdb.sim.block.cache.miss`                         | Simulated block cache misses (from the cache simulator).                                             | ticker    |
| `rocksdb.sst.batch.size`                               | Distribution of SST write batch sizes.                                                               | histogram |
| `rocksdb.sst.user.defined.index.load.fail.count`       | Number of failures loading user-defined SST indexes.                                                 | ticker    |
| `rocksdb.sst.write.micros`                             | Distribution of SST file write latencies (microseconds).                                             | histogram |
| `rocksdb.subcompaction.setup.times.micros`             | Distribution of subcompaction setup times (microseconds).                                            | histogram |
| `rocksdb.table.open.io.micros`                         | Distribution of SST table open I/O latencies (microseconds).                                         | histogram |
| `rocksdb.table.open.prefetch.tail.hit`                 | Number of table opens that hit the prefetched file tail.                                             | ticker    |
| `rocksdb.table.open.prefetch.tail.miss`                | Number of table opens that missed the prefetched file tail.                                          | ticker    |
| `rocksdb.table.open.prefetch.tail.read.bytes`          | Distribution of bytes read while prefetching the SST file tail on open.                              | histogram |
| `rocksdb.table.sync.micros`                            | Distribution of new SST table file sync latencies (microseconds).                                    | histogram |
| `rocksdb.timestamp.filter.table.checked`               | Number of SST tables checked against the timestamp filter.                                           | ticker    |
| `rocksdb.timestamp.filter.table.filtered`              | Number of SST tables filtered out by the timestamp filter.                                           | ticker    |
| `rocksdb.txn.get.tryagain`                             | Number of transaction `Get()` calls that returned `TryAgain` due to a snapshot conflict.             | ticker    |
| `rocksdb.txn.overhead.duplicate.key`                   | Transaction overhead incurred by duplicate-key checks.                                               | ticker    |
| `rocksdb.verify_checksum.read.bytes`                   | Bytes read during explicit checksum verification.                                                    | ticker    |
| `rocksdb.wal.bytes`                                    | Bytes written to the write-ahead log (WAL).                                                          | ticker    |
| `rocksdb.wal.file.sync.micros`                         | Distribution of WAL file sync latencies (microseconds).                                              | histogram |
| `rocksdb.wal.synced`                                   | Number of WAL syncs.                                                                                 | ticker    |
| `rocksdb.warm.file.read.bytes`                         | Bytes read from files with the `warm` temperature.                                                   | ticker    |
| `rocksdb.warm.file.read.count`                         | Number of reads from files with the `warm` temperature.                                              | ticker    |
| `rocksdb.write.other`                                  | Number of writes processed on behalf of another thread's write group (as a follower).                | ticker    |
| `rocksdb.write.raw.block.micros`                       | Distribution of raw block write latencies (microseconds).                                            | histogram |
| `rocksdb.write.self`                                   | Number of writes processed by the calling thread itself (as the write group leader).                 | ticker    |
| `rocksdb.write.wal`                                    | Number of writes that were written to the WAL.                                                       | ticker    |

## Transaction Log Stats

`log.getStats()` returns a detailed snapshot for a single transaction log store. All sizes are in
bytes and all timestamps are milliseconds since the Unix epoch.

```typescript
const db: RocksDatabase = RocksDatabase.open('path/to/db');
const log: TransactionLog = db.useLog('my-log');
const stats: TransactionLogStats = log.getStats();
```

`log.getStats()` returns an object with the following shape:

- `name: string` The name of the transaction log store.
- `path: string` Filesystem path to the log store's directory.
- `fileCount: number` Number of sequence log files on disk for this log.
- `currentSequenceNumber: number` Sequence number of the active write file.
- `oldestSequenceNumber: number` Sequence number of the oldest file still on disk.
- `totalSizeBytes: number` Total on-disk size of all of this log's files.
- `currentFileSize: number` Size of the active write file.
- `pendingTransactions: number` Transactions bound to this log but not yet written via
  `writeBatch()`.
- `uncommittedTransactions: number` Transactions written to this log but not yet committed to
  RocksDB.
- `replayGapBytes: number` Bytes between the last flushed position and the write head (written but
  not yet flushed to the database).
- `memory: object`
  - `mappedBytes: number` Virtual address space reserved for memory-mapped files (the active write
    file maps the full configured `maxFileSize` on POSIX), not resident memory.
  - `overlayBytes: number` File-backed overlay portion (POSIX only; `0` on Windows) — a closer
    proxy for real memory consumption than `mappedBytes`.
  - `activeMaps: number` Number of log files currently memory-mapped.
- `nextLogPosition: object` Position where the next log entry will be written.
  - `sequence: number` The log file sequence number.
  - `offset: number` The byte offset within that file.
- `lastFlushedPosition: object` Position up to which data has been flushed to the database.
  - `sequence: number` The log file sequence number.
  - `offset: number` The byte offset within that file.
- `lastCommittedPosition: object | null` Position of the last transaction committed to RocksDB, or
  `null` if nothing has been committed yet.
  - `sequence: number` The log file sequence number.
  - `offset: number` The byte offset within that file.
- `purge: object`
  - `oldestFileAgeMs: number` Age in milliseconds of the oldest file on disk.
  - `purgeableFiles: number` Number of files eligible for purge under the retention policy.
  - `retainedUnflushedFiles: number` Number of files past the retention threshold but retained
    because they are unflushed.
  - `lastPurgeMs: number` Timestamp of the last purge scan.
- `totals: object` Lifetime counters since the log store was created.
  - `transactionsWritten: number` Cumulative number of transactions written to this log.
  - `entriesWritten: number` Cumulative number of entries written to this log.
  - `bytesWritten: number` Cumulative bytes written to this log (entry payload plus entry header,
    excluding file headers).
  - `rotations: number` Number of times the log rotated to a new file.
  - `filesPurged: number` Number of files purged by the retention policy.
  - `bytesPurged: number` Bytes reclaimed by purging files.
  - `purgeRuns: number` Number of purge scans run.
  - `databaseFlushes: number` Number of database flush events observed by this log.
  - `writeFailures: number` Number of write failures encountered by this log.
- `config: object` The store's configured limits.
  - `maxFileSize: number` Configured maximum size of a single log file.
  - `retentionMs: number` Configured retention period in milliseconds.
  - `maxAgeThreshold: number` Configured age threshold (a float between `0` and `1`) at which files
    become purge-eligible.

A summarized subset of these stats is also rolled up into `db.getStats()` under the `txnlog.*`
keys documented above (summed across all of a database's logs).
