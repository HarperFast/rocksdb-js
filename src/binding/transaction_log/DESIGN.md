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
- Purge syncs it, and its directory, before unlinking the highest registered segment (`syncFlushedStateForPurge()`), and keeps that segment if the sync fails. Deleting any lower segment leaves a higher file as the record, so only emptying a store pays the fsync, under `writeMutex`. The record stays an 8-byte in-place overwrite at offset 0, inside one sector, so the flush path takes no extra fsync.

A backup captures `txn.state` before it copies segments, outside the store's locks. `pinRetention()` holds off ordinary purges for that window: a purge reads a newer flushed position and could delete entries the captured one still needs replayed. A stalled backup stream therefore suspends retention for its store until the stream ends.
