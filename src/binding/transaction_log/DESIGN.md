# Transaction-log mapping capacity

A reader's external buffer has a fixed length. `TransactionLogFile::getMemoryMap()` records the smallest exposed capacity under `fileMutex`; `writeEntriesV1()` refuses a batch whose end would exceed it. The store rotates on no progress, preserving each complete transaction in one segment. The capacity remains recorded after readers release their views.

`writeEntries()` holds `fileMutex` through lazy open, header creation, and the first append, so readers cannot force repeated rotations before an oversized first batch lands. Recovered header-only segments remain readable; an oversized batch rotates away if their capacity was already exposed.

Internal index maps may be replaced before the first reader handout. After exposure, an active map remains pinned so the POSIX writer continues extending its file overlay. Windows releases an undersized internal view before extending the file for a reader's first full-capacity mapping. A requested capacity is always raised to the written extent under the file lock. `transactionLogMaxSize: 0` uses the default 16 MiB limit.

The JS reader needs no remapping; its filtered-entry transition still refreshes the final extent before leaving a rotated segment, and traversal skips empty retired segments.
