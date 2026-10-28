# Transaction-log mapping capacity

A reader's external buffer has a fixed length. `TransactionLogFile::getMemoryMap()` records the smallest exposed capacity under `fileMutex`; `writeEntriesV1()` refuses a batch whose end would exceed it. The store rotates on no progress, preserving each complete transaction in one segment. The capacity remains recorded after readers release their views.

`writeEntries()` holds `fileMutex` through lazy open, header creation, and the first append, so readers cannot force repeated rotations before an oversized first batch lands. Recovered header-only segments remain readable; an oversized batch rotates away if their capacity was already exposed.

Internal index maps may be replaced before the first reader handout. After exposure, an active map remains pinned so the POSIX writer continues extending its file overlay. Windows releases an undersized internal view before extending the file for a reader's first full-capacity mapping. A requested capacity is always raised to the written extent under the file lock. `Database::Open` rejects `transactionLogMaxSize: 0`: an unbounded segment would outgrow every fixed capacity, so preserving it needs reader remapping or, on Windows, pre-extending the file to the reservation. Internal callers that pass `0` get the default limit from the store constructor.

The JS reader needs no remapping; its filtered-entry transition still refreshes the final extent before leaving a rotated segment, and traversal skips empty retired segments.

# Retention and the sequence witness

Retention deletes any segment that is old enough and lies wholly at or before `txn.state`'s flushed position, including the segment `txn.state` names and the writer's current segment. `doPurge()` retires the current segment from the writer (`retireCurrentSequenceLocked()`) before unlinking it, so a refused unlink leaves an ordinary frozen segment; the next append creates the next segment lazily, as after any rotation.

Once every segment can go, `txn.state` is the only durable record of the highest sequence used. Three rules keep it a sound witness:

- `load()` never appends behind it: with the named segment gone, the writer starts at that sequence + 1, or at the sequence itself when the offset is 0 (a rotation published before the segment was written). Appending lower would land before the position a replay starts from.
- `writeFlushedPosition()` never moves it back, since flush jobs can complete out of order.
- Purge syncs it, and its directory, before unlinking the highest registered segment (`syncFlushedStateForPurge()`), and keeps that segment if the sync fails. Deleting any lower segment leaves a higher file as the record, so only emptying a store pays the fsync, under `writeMutex`. A filesystem that rejects directory fsync keeps that last segment, as before this rule. The record stays an 8-byte in-place overwrite at offset 0, inside one sector, so the flush path takes no extra fsync.

A backup captures `txn.state` before it copies segments, outside the store's locks. `pinRetention()` holds off ordinary purges for that window: a purge reads a newer flushed position and could delete entries the captured one still needs replayed. A stalled backup stream therefore suspends retention for its store until the stream ends.

# Conditional close

`tryClose()` (the only caller is `purgeLogs({ destroy: true })`) may set `isClosing` only when no transaction is bound-but-unwritten (`pendingTransactionCount`) or written-but-uncommitted (a non-sentinel uncommitted position). The two are read in separate critical sections — the count under `transactionBindMutex`, the positions under `writeMutex` + `dataSetsMutex` — and a transaction hands off from one to the other inside `writeBatch()`, so one that binds and writes between the reads is in neither. `transactionBindGeneration` closes that gap: every bind goes through `tryBindTransaction()`, which bumps it, and the latch refuses if it moved since the count was read. A bind that is later abandoned also refuses that attempt; the store stays registered and the next destroy closes it.

Two tighter alternatives are deliberately not used. Holding `transactionBindMutex` while waiting for `writeMutex` would make JS-thread binds wait on retention's file I/O. Try-locking `writeMutex`/`dataSetsMutex` under it would refuse to destroy an idle store whenever a reader, a flush callback, a backup snapshot or a stats call holds `dataSetsMutex`.

# A failed sync freezes the flushed state

`databaseFlushBegin()` syncs every segment before RocksDB flushes, and `txn.state` then records how far the log is covered by flushed data. If any segment sync fails, the store latches `logSyncFailed` and `txn.state` stops advancing for the life of that store instance, reported once through `log.warn`. A later successful sync does not clear it: after a failed `fsync` the kernel may mark the dirty pages clean, so a later success proves nothing about the bytes it lost. The latch is per instance, not persisted: an in-process close and reopen builds a fresh store whose new file descriptors no longer report the earlier error, so the first sync after a reopen can advance `txn.state` again. Holding `txn.state` back has three costs: a longer replay on restart, retention that cannot purge past it, and a lower position for `recoverTail()` to protect (invariant 14), so a crash after the failed sync can discard an unclosed batch's surviving prefix that a later completed flush already put in SST. Advancing it instead could let a restart skip log entries that were never durable.

# Exact-start lookups

`query({ start, exactStart: true })` returns everything from the first entry whose timestamp equals `start`. Timestamps are not ordered (`setTimestamp()` adopts origin and replay timestamps), so a larger timestamp ends nothing. The running-maximum index gives only a lower bound, and a miss used to scan to end-of-log.

`TransactionLogStore::findPositionByTimestamp(timestamp, &span)` also returns a span `[stop, tail)` that holds no entry with a timestamp at or below `start`; the reader jumps over it once. Per segment, the index walk keeps two scalars for entries below the running maximum: their minimum timestamp and the end of the last one. The last possible candidate in a segment is then the later of the next maximum key above `start` and, when that minimum is at or below `start`, the last such entry. That gives the same yes/no answer per segment as a full suffix-minimum index. Precision is lost only inside the one segment holding the newest candidate. Memory stays constant per segment, where a suffix-minimum stack grows with every entry below a high outlier.

`tail` is the newest examined byte and moves down to anything the scan must still see. Keep these, each covered by a test:

- **Live tail.** `tail` names the newest _visited_ segment's indexed extent, never `currentSequenceNumber`, which can name a segment not yet created. A match appended later lands past it.
- **Batch boundary.** `tail` is always the start of a batch: the end of a complete walk is the written extent, which `writeEntriesV1` advances only by whole batches, and an incomplete walk or a framing break falls back to the end of the last indexed entry carrying `TRANSACTION_LOG_ENTRY_LAST_FLAG`. A reader that jumps therefore reads the first entry of the next batch, and its flags, before any other entry of it. Logs written before batches were kept within one segment can hold a batch split across a rotation, and a segment with no flagged entry falls back to its header; there the jump lands where a sequential reader crossing the rotation would, at a segment start.
- **Unindexed suffix, framing break, missing sequence.** `tail` stops below each, so the reader still raises `CorruptFrameError` and `TransactionLogGapError` as before.
- **Purge after positioning.** A jump into another segment maps the tail, then re-checks `TransactionLogStore::segmentRemovals` (process-wide, bumped and read under `dataSetsMutex`). If it moved, the reader scans and reports the gap. A start that is already past the stop jumps inside `query()`, so a purge after `query()` returns is ordered after that jump.

The first lookup in a process still indexes every segment from the newest down to the start. The index then persists per segment, so later lookups pay only the walk.

# Aborted batches

`TRANSACTION_LOG_ENTRY_ABORTED_FLAG` (0x02) on a batch's first entry marks a transaction that did not commit. The flag is only on the first entry, so the reader has to find batch starts: an entry starts a transaction after an entry carrying `LAST_FLAG`, at a timestamp change (every entry of a batch carries the batch's timestamp, which recovery also relies on), or when it carries the abort flag itself. An aborted transaction is skipped until the next start, and the state resets at every segment boundary and corrupt-frame resync. So a legacy run that never closed cannot expose an aborted batch, and an aborted batch that lost its last entry hides at most a following batch with the same timestamp in the same segment, which takes corruption plus a reused origin key. A reader that enters an aborted batch past its first entry, or resyncs inside one (the decision is not carried across a resync), reads the rest as live, like the rest of any transaction torn by corruption; on an uncorrupted log no query starts or resyncs there. The validators use the same start rule minus the abort flag itself, and report the flag on an entry that does not otherwise start a transaction. This release reads the flag but never writes it, so a later release can set it without leaving this one to read aborted transactions as live (the reader-first half of HarperFast/harper#3253).
