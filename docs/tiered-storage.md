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

`path` and `blobs.dir` are resolved to absolute paths (against the process working directory) when
the database is opened, so the same configuration cannot silently resolve to a different volume in
a process started from somewhere else.

Three things about this are easy to get wrong:

**Placement is static.** `GetPathId()` is a pure function of the target sizes,
`max_bytes_for_level_base`, the level multiplier, and the level number. It never looks at actual
disk usage, so `targetSize` really means "how many levels fit here" — it will not react to a
volume genuinely filling up.

**Flushes always go to the first path.** RocksDB's `FlushJob` hardcodes `path_id` 0, so every L0
file lands on `paths[0]` regardless of its target size. Only compaction output is distributed. The
same is true of a manual `compact()`/`compactSync()`, whose `target_path_id` is 0.

**Entries may only be appended.** A file's _path index_ is what gets recorded in the MANIFEST, not
its directory. Appending a path to grow onto a new volume is safe — existing files keep their index
and stay put. Reordering or removing an entry makes RocksDB look for existing files in the wrong
directory, and nothing on disk records which index a file came from — so across a restart that one
is not checked for you.

That includes **deleting the option entirely**, which is the natural thing to try when tiering turns
out to be a mistake. `db_paths` is sanitized back to `[{ <database directory> }]`, index 0 stops
meaning the fast volume, and the open fails.

Once _this process_ has opened the database, it does check: the list is kept as the destroy layout
(below), and a writable open that neither matches nor appends to it is rejected before RocksDB is
handed it.

```
Cannot open "/var/lib/mydb" with ["/var/lib/mydb", "/mnt/other"]: this process has
already recorded ["/var/lib/mydb", "/mnt/cold"] as where this database keeps its SST
files, and destroy() deletes them from that record. The requested list neither matches
it nor appends to it, so RocksDB would place new files on a volume destroy() never
sweeps, and look for existing ones on a volume they were never written to. Storage
paths are append-only: reopen with the recorded list, adding any new volume to the end
of it.
```

That record does not survive a restart — `db_paths` is not persisted, so a fresh process has
nothing to compare a shrinking or reordered list against, and the first sign of it is RocksDB's own
failure. That one is annotated with what to do about it:

```
Corruption: IO error: No such file or directory: /var/lib/mydb/000009.sst ...
MANIFEST-000012 may be corrupted

If this database has ever been opened with `paths`, that same list must be supplied
again: RocksDB stores each SST file's location as an index into the list given at open
and does not record the list itself, so removing or reordering an entry points it at the
wrong volume for files that are still where they were left. Entries may only be
appended. Otherwise the files named above are genuinely missing.
```

Nothing is lost: putting the list back opens the database. To undo tiering for real, keep the
`paths` list intact and move the data with a compaction to path 0, or rebuild the database from a
snapshot taken before tiering.

Also note that supplying more than one path disables `level_compaction_dynamic_level_bytes`
(RocksDB's `SanitizeCfOptions` logs a warning and turns it off), so level sizing becomes static and
`max_bytes_for_level_base` has to be sized by hand.

### Adding `paths` to a database that does not have it

This is the migration people actually attempt, and the naive form of it breaks the database.

With no `paths`, RocksDB sanitizes `db_paths` to `[{ <database directory>, ... }]`, so every SST
file the database has ever written carries path index 0 meaning _the database directory_. Supplying
`paths` for the first time redefines index 0 as `paths[0]`, and RocksDB then looks for all of those
files on the wrong volume:

```
Corruption: IO error: No such file or directory: /nvme/mydb-sst/000009.sst
The file /nvme/mydb/MANIFEST-000005 may be corrupted.
```

Nothing is actually corrupt — reverting the config line opens it again — but the error names the
MANIFEST, which is how an operator ends up reaching for a backup restore instead.

The supported form is to list the database directory itself as the first entry, so index 0 keeps
meaning what it already meant:

```js
const db = RocksDatabase.open('/nvme/mydb', {
	paths: [
		{ path: '/nvme/mydb', targetSize: 200 * 1024 ** 3 },
		{ path: '/mnt/attached/mydb-sst', targetSize: 4 * 1024 ** 4 },
	],
});
```

`open()` rejects the naive form with a message naming the real cause: it checks whether the
database directory holds SST files that are not reachable under `paths[0]`.

## Directories must not be shared between databases

This applies to both `paths` entries and `blobs.dir`, and it is not checked for you.

SST and blob file numbers come from a per-database counter, and RocksDB's `LOCK` file protects only
the database directory. Point two databases at one `/mnt/attached/data` — which is the natural
reading of "put the big stuff on the attached volume" — and both mint `000042.sst` / `000042.blob`
in it, and each one's obsolete-file scan deletes the other's live files. Silent corruption, no
error at open. Give every database its own subdirectory.

Column families of the _same_ database are fine: they share one counter, so one `blobs.dir` for all
of a database's tables is the intended arrangement.

## `destroy()` follows the layout

`db.destroy()` deletes the tiered SST files and the blob files along with the database directory,
using the database's actual `db_paths` and per-column-family `blob_dir` rather than assuming
everything is under the database directory.

That holds for `close()` followed by `destroy()` too, which is the call order most likely to lose
the layout: closing the last handle takes the descriptor with it, and `db_paths` is not written to
the OPTIONS file, so the process registry keeps a path-keyed copy for exactly this.

Only a successful writable cold open can establish or append storage paths in that copy. A
read-only open may use a shorter or different list while all files it needs are still at path index
0, but its list never becomes a destroy target. Default placement is retained explicitly too, so an
empty path list or `blob_dir` cannot be replaced by a stale reader's external directory.

Because that copy is what `destroy()` acts on, a writable open cannot quietly disagree with it: a
shorter or divergent list would leave compaction writing SSTs to a volume `destroy()` never visits,
and `destroy()` sweeping one this open disowned — which some other database may since have been
given. Such an open fails, before RocksDB sees the list.

After a non-default column family is successfully dropped, its former `blobs.dir` is removed from
that destroy layout. RocksDB owns cleanup of the dropped family's files; wait for any remaining
handles to close and verify that no blob files remain before reusing the directory. Once reassigned,
`destroy()` will not revisit it and delete the new owner's blobs.

## Backups and checkpoints

**`paths` disables both.** RocksDB's `GetLiveFilesStorageInfo` — which `BackupEngine` and
`Checkpoint` both go through — refuses when `db_paths` is set, so `db.backup()` and
`db.createCheckpoint()` fail with:

```
Not implemented: db_paths / cf_paths not supported for Checkpoint nor BackupEngine
```

This is not "the copy comes out flat" — there is no copy. It applies to **any** non-empty `paths`,
including the one-entry `[{ path: <the database directory> }]` form the migration section above
recommends. A database using `paths` has to be backed up some other way (a filesystem/volume
snapshot covering every path, taken with the database closed or quiesced).

**`blobs.dir` copies, but flattens.** Blob placement is not `db_paths`, so a backup or checkpoint
succeeds — but everything lands in one directory, and RocksDB has no notion of re-scattering it on
restore. The copy carries the `OPTIONS` file, so the persisted `blobs.dir` still names the old
directory while the files are now beside the SSTs.

A plain open of that copy is rejected (the mismatch guard fires), which is the good case — it
rejects instead of reading large values that are not there. To open the copy as a flat database,
acknowledge the new (empty) directory once:

```js
const db = RocksDatabase.open('/nvme/restored', { blobs: { allowDirChange: true } });
```

Note the omitted `dir`. That is what says "the whole database is flat now", and it re-points
**every** column family, not just the one the open names. It is what makes restoring beside a live
source safe: scoped to the named family, the others would keep the `blob_dir` in the restored
`OPTIONS` file — the _source_ database's live directory — and the two databases would then mint
colliding `NNNNNN.blob` numbers there while each one's obsolete-file scan deleted the other's live
files.

That open must be writable so RocksDB can persist the flat layout; a read-only open rejects the
acknowledgement. After it succeeds, later opens of the restored database need nothing. The
alternative is to move the `.blob` files back to the original directory before opening with the
original configuration.

> **Open a restored copy with `allowDirChange` before anything else touches it.** The guard can only
> fire on the family an open names, so a restored database whose _first_ family happens to have no
> external blob directory of its own (`default`, typically) opens cleanly while its other families
> still point at the source. Nothing in the `OPTIONS` file distinguishes a restored copy from the
> original, so this one is a procedure, not a check.

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
unless a blob cache is configured. When `blockCacheSize` is configured and `blobCacheSize` has
never been set explicitly, rocksdb-js assigns the blob cache an additional 10% of the block-cache
capacity — so an existing `config({ blockCacheSize })` call raises this process's memory ceiling by
10% with no code change. Set `blobCacheSize` explicitly (`0` disables it) to state the budget
yourself; once you have, later calls that supply only `blockCacheSize` leave it alone. When blob
files are on slower storage, a larger explicit cache can be useful:

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
missing. The check — and the request — apply to the column family being opened, not to the whole
database: `blobs` is a per-column-family option, and opening one family never restamps another's
settings (the same rule `compression` follows). A family whose `blobs` you omit entirely keeps
whatever it persisted.

`dir` is the one exception to that inheritance: omitting it means "alongside the SST files", not
"whatever this family had", so **every** open of a family with an external blob directory must keep
supplying the same `dir`. Dropping it from a configuration is what the guard rejects.

"Alongside the SST files" is `paths[0]` when the database also sets `paths`, not the database
directory: RocksDB derives a blob file's path from `cf_paths.front()`, which falls back to
`db_paths.front()`. So a tiered database keeps its blob files on its first storage volume unless
`dir` moves them, and that is the directory the relocation checks below read.

To migrate, move the `.blob` files while the database is closed, then reopen with
`allowDirChange` to acknowledge it:

```js
// database closed; mv /old/blobs/*.blob /new/blobs/
const db = RocksDatabase.open('/nvme/mydb', {
	blobs: { dir: '/new/blobs', allowDirChange: true },
});
```

Nothing is moved for you — `allowDirChange` only records where the files went, so setting it
_without_ relocating them is exactly the failure the check exists to prevent. It is only needed for
the open that performs the switch: that open records the new directory, so later opens can drop
`allowDirChange` — but they still have to supply the same `dir`.

A read-only open may keep a leftover `allowDirChange` flag when every directory already matches.
An open that would actually change a directory requires write access so the new location can be
persisted in the OPTIONS file.

One open relocates one directory's worth of files. The `mv` above moved every blob file that was in
`/old/blobs`, so `allowDirChange` re-points every column family whose blobs were in `/old/blobs` —
not just the one the open names, and not families whose blobs were somewhere else (`default`, in the
common Harper layout, keeps its blobs beside the SSTs and is left alone). Reaching the other
families matters because it cannot be done afterwards: a second open in the same process is a _warm_
one, which cannot change a live family's directory, so it would take one process restart per table
with the database broken in between.

A database using several distinct blob directories therefore needs one open per directory — `dir`
names a single destination, so a single open cannot describe more than one move. Families are
grouped by the directory string they persisted, so **spell one directory identically across every
family that uses it**: `/mnt/blobs` and a symlinked `/data/blobs` are two groups, and an
acknowledged move naming one spelling leaves the other family behind.

Omitting `dir` entirely is the exception: it says the whole database was flattened into its own
directory, and re-points every family.

Either form's claim is checked rather than trusted, because it is a claim about files on disk. One
rule covers both: a family whose recorded directory still holds `.blob` files has not moved, and the
open is refused naming that family. That includes a family whose blobs are still beside its SST
files — moving a flat database's blobs out to a volume for the first time is the migration most
people make, and "the database directory" is a recorded directory like any other. So finish the move — a half-copied directory is refused, and so
is running the open before the `mv`. For a restored copy the same rule says the source's directory
is still in use and must not be shared. Neither the destination existing nor its holding some files
is evidence: the open creates it, and a partial copy leaves both populated.

If the source cannot be inspected, the acknowledgement is refused. Restore access and finish the
move, or make the old path definitively absent, then retry. Confirm the old volume's mount state
first: a failed mount can leave an empty mount point that looks exactly like a successfully emptied
directory, and no source-side scan can distinguish those cases. Removing an absent mount point is
also an operator assertion that the old volume is no longer the source. Every accepted relocation
is recorded after a successful open in RocksDB's info `LOG` with the previous and new directory
when the configured logger is available.

A persisted directory that remains selected but has gone missing entirely — an unmounted volume,
for example — fails a normal open naming the family and the directory, rather than being discovered
when the first read returns nothing and the first flush errors the database read-only.

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
