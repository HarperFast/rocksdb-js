# Tiered storage

Spreading one database across volumes with different cost and performance — typically a fast
local NVMe plus a larger, slower attached volume.

Two independent knobs do this, and they cover different files:

| Option      | Governs    | Placement rule                          |
| ----------- | ---------- | --------------------------------------- |
| `paths`     | SST files  | By LSM level, across the listed volumes |
| `blobs.dir` | Blob files | All of them, in one directory           |

Everything else — the MANIFEST, `CURRENT`, `OPTIONS`, the WAL, the informational `LOG` — stays in
the database directory you pass to `open()`. Transaction logs have their own `transactionLogsPath`.

## `paths` — spreading SST files

```js
const db = RocksDatabase.open('/nvme/mydb', {
	paths: [
		{ path: '/nvme/mydb-sst', targetSize: 200 * 1024 ** 3 },
		{ path: '/mnt/attached/mydb-sst', targetSize: 4 * 1024 ** 4 },
	],
});
```

RocksDB walks the list in order, assigning levels to a path until the running total of that
level's estimated size exceeds the path's `targetSize`, then moving to the next. The last path is
the fallback and absorbs everything that doesn't fit earlier.

Three things about this are easy to get wrong:

**Placement is static.** `GetPathId()` is a pure function of the target sizes,
`max_bytes_for_level_base`, the level multiplier, and the level number. It never looks at actual
disk usage, so `targetSize` really means "how many levels fit here" — it will not react to a
volume genuinely filling up.

**Flushes always go to the first path.** RocksDB's `FlushJob` hardcodes `path_id` 0, so every L0
file lands on `paths[0]` regardless of its target size. Only compaction output is distributed. The
same is true of a manual `compact()`/`compactSync()`, whose `target_path_id` is 0.

**Entries may only be appended.** A file's _path index_ is what gets recorded in the MANIFEST, not
its directory. Appending a path to grow onto a new volume is safe — existing files keep index 0 and
stay put. Reordering or removing an entry makes RocksDB look for existing files in the wrong
directory.

Also note that supplying more than one path disables `level_compaction_dynamic_level_bytes`
(RocksDB's `SanitizeCfOptions` logs a warning and turns it off), so level sizing becomes static and
`max_bytes_for_level_base` has to be sized by hand.

## Directories must not be shared between databases

This applies to both `paths` entries and `blobs.dir`, and it is not checked for you.

SST and blob file numbers come from a per-database counter, and RocksDB's `LOCK` file protects only
the database directory. Point two databases at one `/mnt/attached/data` — which is the natural
reading of "put the big stuff on the attached volume" — and both mint `000042.sst` / `000042.blob`
in it, and each one's obsolete-file scan deletes the other's live files. Silent corruption, no
error at open. Give every database its own subdirectory.

Column families of the _same_ database are fine: they share one counter, so one `blobs.dir` for all
of a database's tables is the intended arrangement.

## Backups do not preserve the layout

`db.backup()` and `backups.restore` copy files into a single directory. RocksDB has no notion of
re-scattering them across `paths` or back into a `blobs.dir`, so a restored database is flat:
every SST that had a path index above 0, and every blob file, lands in the database directory.

Reopening that database with the original `paths` fails to find the SSTs. For blobs it is quieter
and worse — the backup carries the `OPTIONS` file, so the persisted `blobs.dir` still matches the
request, the mismatch check passes, and reads of large values fail with "No such file or
directory".

Until restore learns to relocate, treat a tiered database as needing manual placement after a
restore: restore it flat, then either open it without `paths`/`blobs.dir`, or move the `.sst` and
`.blob` files back to the volumes they belong on before reopening (`blobs.allowDirChange` covers
the blob half).

## `blobs.dir` — putting large values on their own volume

`paths` cannot tier large values: stock RocksDB derives every blob file path from the _first_
configured path, so blob files never move. `blobs.dir` decouples them.

```js
const db = RocksDatabase.open('/nvme/mydb', {
	blobs: { dir: '/mnt/attached/mydb-blobs', minSize: 2048 },
});
```

This puts the whole LSM tree — including all the compaction write traffic — on the fast volume,
while values at or above `minSize` live on the slow one. That trade is usually the right way
round for network-attached storage: blob files are never rewritten by compaction (avoiding that
rewrite is the entire point of blob separation), so the slow volume absorbs no compaction I/O. The
cost is one extra random read on the hot path of every record at or above `minSize`.

Because RocksDB does **not** put blob values in the block cache, that extra read is real I/O
unless a blob cache is configured. When blob files are on slower storage this matters a lot:

```js
RocksDatabase.config({ blobCacheSize: 512 * 1024 ** 2 });
```

### `blobs.dir` requires a patched RocksDB

The option is a downstream addition (`AdvancedColumnFamilyOptions::blob_dir`, guarded by
`ROCKSDB_HAS_CF_BLOB_DIR`), carried as a patch in `HarperFast/rocksdb-prebuilds`. Opening with
`blobs.dir` set against an unpatched build throws.

### Changing it strands existing blob files

Unlike an SST's path index, a blob file's directory is **not** recorded per file — it is derived
from `blobs.dir` every time the file is opened. Reopening with a different directory therefore
does not move the blob files, it orphans them, and every value at or above `minSize` becomes
unreadable.

`open()` rejects that: it compares the requested directory against the one recorded in the
database's `OPTIONS` file and throws rather than opening a database whose large values have gone
missing.

To migrate, move the `.blob` files while the database is closed, then reopen with
`allowDirChange` to acknowledge it:

```js
// database closed; mv /old/blobs/*.blob /new/blobs/
const db = RocksDatabase.open('/nvme/mydb', {
	blobs: { dir: '/new/blobs', allowDirChange: true },
});
```

Nothing is moved for you — `allowDirChange` only suppresses the check, so setting it _without_
relocating the files is exactly the failure the check exists to prevent. It is only needed for the
open that performs the switch: that open records the new directory, so subsequent plain opens
succeed on their own.

## Turning blob files off

`blobs.enabled: false` stores values inline in SST files regardless of size. Existing blob files
stay readable, and with `blobs.garbageCollection` on, compaction gradually pulls their values back
inline — which is the only way to drain blob files into SST files (and therefore onto `paths`).

Two caveats if you use this to reclaim a filling volume:

- It temporarily **increases** usage. A blob file is deleted only once no SST references it, so
  mid-drain the value exists in both places.
- The drain is partial by default. `blobs.garbageCollectionAgeCutoff` defaults to `0.25`, so only
  the oldest quarter of blob files are eligible per compaction, and
  `blobs.garbageCollectionForceThreshold` defaults to `1` so nothing is ever force-scheduled.
  Raise the cutoff toward `1` to actually drain them.

Inlining also gives up the write-amplification win permanently: large values get rewritten at
every level again.
