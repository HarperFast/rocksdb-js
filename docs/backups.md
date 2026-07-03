# Backups

## Overview

A backup is a **whole-database** copy: every column family, the manifest, and (by default) the
write-ahead log are captured as a consistent point-in-time snapshot. Backups are not scoped to an
individual store or column family.

There are two ways to produce a backup, plus a closely related third mechanism for fast local
copies:

|                     | Directory backup                       | Stream backup                          | Checkpoint                             |
| ------------------- | -------------------------------------- | -------------------------------------- | -------------------------------------- |
| **API**             | `db.backup(dir)`                       | `db.backup(stream)`                    | `db.createCheckpoint(path)`            |
| **Output**          | a backup directory                     | a tar (optionally gzipped) byte stream | an independent database directory      |
| **Local disk copy** | full copy into the directory           | none — streamed out                    | hard links (same filesystem) or a copy |
| **Incremental**     | yes — files shared across backups      | no — always a full snapshot            | n/a — each is independent              |
| **Restore**         | `backups.restore()`                    | extract the tar, then open             | open the directory directly            |
| **Resumable**       | n/a                                    | no                                     | n/a                                    |
| **Returns**         | a numeric backup id                    | `void`                                 | `void`                                 |
| **Management API**  | list / verify / delete / purge         | none                                   | none                                   |
| **Best for**        | scheduled local backups with retention | off-host upload with no scratch disk   | fast branching / snapshots             |

Directory and stream backups flush the memtable before copying **by default only when the database
was opened with `disableWAL`** (otherwise unflushed data would be missing); when the WAL is enabled
it is captured in the snapshot and replayed on restore, so committed data is never lost either way.
Checkpoints always flush the memtable.

---

## Directory backups

```typescript
const id = await db.backup('/path/to/backups');
```

### How it works

Directory backups use RocksDB's `BackupEngine`. The live SST/blob files, manifest, and WAL are
**copied** into the backup directory. Parent directories are created as needed, and the call
resolves with a monotonically increasing **backup id**.

Multiple backups can live in the same directory. With file sharing enabled (the default), unchanged
SST/blob files are stored **once** and referenced by later backups — so the second and subsequent
backups into the same directory are effectively **incremental** and only copy what changed. Shared
files are reference-counted and removed only when no remaining backup references them.

### Options

```typescript
await db.backup('/path/to/backups', {
	flushBeforeBackup: true, // default: true iff opened with disableWAL
	metadata: 'nightly-2026-06-26', // arbitrary string returned by backups.list()
	shareTableFiles: true, // default: true — enables incremental backups
	shareFilesWithChecksum: true, // default: true — dedup shared files by checksum
	backupLogFiles: true, // default: true — include the WAL
	sync: true, // default: true — fsync backup files
	maxBackgroundOperations: 1, // default: 1 — copy threads
});
```

### Managing backups

The `backups` namespace operates on a backup directory and does **not** require an open database:

```typescript
import { backups } from '@harperfast/rocksdb-js';

await backups.list('/path/to/backups'); // BackupInfo[]
await backups.restore('/path/to/backups', '/restored'); // restore latest (or { backupId })
await backups.verify('/path/to/backups', id, { verifyWithChecksum: true });
await backups.delete('/path/to/backups', id); // remove one backup
await backups.purge('/path/to/backups', 3); // keep the newest 3
```

### Restoring

```typescript
await backups.restore('/path/to/backups', '/path/to/restored-db', {
	backupId: 2, // default: the latest non-corrupt backup
	walDir: '/restored-db', // default: the database directory
	keepLogFiles: false,
	mode: 'purgeAllFiles', // default — see below
});

const restored = new RocksDatabase('/path/to/restored-db');
restored.open();
```

Restore modes:

- **`purgeAllFiles`** (default): purge the destination directory and restore everything. Destructive
  but always correct.
- **`keepLatestDbSessionIdFiles`**: efficiently restore over a healthy database, reusing existing
  files that match the backup.
- **`verifyChecksum`**: reuse existing destination files whose checksums match the backup, replacing
  only mismatched/corrupt files.

### Caveats

- **The data is duplicated onto local disk.** A backup is a real, separate copy in the backup
  directory; it is not a hard link to the live database. Budget disk for it.
- **Incremental sharing is per-directory.** Files are only deduplicated against other backups in the
  same backup directory.
- **Restore is destructive by default** (`purgeAllFiles`). Never restore into a directory you cannot
  afford to have purged, and never restore into the backup directory itself (this is rejected, but
  do not rely on it).
- **It needs a writable local directory.** To get a backup off the host you still have to copy the
  directory afterward.

---

## Stream backups

```typescript
import { createWriteStream } from 'node:fs';
import { Writable } from 'node:stream';

// Any WHATWG WritableStream<Uint8Array> works. A Node stream adapts via Writable.toWeb():
const out = Writable.toWeb(createWriteStream('/path/to/backup.tar'));
await db.backup(out);
```

### How it works

Stream backups produce a **tar archive** of the database's live files and write it to a
`WritableStream` — with **no intermediate copy on disk**. The native layer enumerates the consistent
set of live files (via RocksDB's `GetLiveFilesStorageInfo`), reads each one, and the bytes are framed
into a [USTAR](<https://en.wikipedia.org/wiki/Tar_(computing)#UStar_format>) archive and written to the
stream. The result is byte-for-byte a snapshot of the database directory, so it unpacks with any tar
tool into a directory that opens directly as a RocksDB database.

**Backpressure is honored end to end.** Exactly one chunk is in flight at a time, and the native
producer waits for each `stream.write()` to drain before reading the next chunk. A slow consumer
(e.g. a network or object-store upload) therefore _paces_ the backup rather than buffering it in
memory — peak memory stays flat regardless of database size.

### Options

```typescript
await db.backup(stream, {
	flushBeforeBackup: true, // default: true iff opened with disableWAL
	gzip: false, // default: false — set true for a .tar.gz stream
});
```

Set `gzip: true` to compress the archive on the fly (via the runtime's `CompressionStream`),
producing a `.tar.gz` stream. Compression sits downstream of the tar encoder, so backpressure is
preserved. Note that RocksDB SST files are usually already block-compressed, so the extra savings
depend on your data and column-family compression settings.

### Restoring

There is no dedicated restore call — the archive _is_ the database. Unpack it with any tar tool and
open the directory:

```sh
mkdir /path/to/restored-db
tar -xf backup.tar -C /path/to/restored-db
# For a gzipped stream (gzip: true), use -xzf backup.tar.gz instead.
```

```typescript
const restored = new RocksDatabase('/path/to/restored-db');
restored.open();
```

The archive contains the standard RocksDB files for the snapshot (`CURRENT`, a `MANIFEST-*`, the
`OPTIONS-*` file, the SST/blob files, and — unless flushed away — the WAL).

### Caveats

- **Not resumable.** The stream is a single, ordered pass over a live snapshot; there is no offset or
  manifest to resume against, and the source snapshot is not retained after the call. If the
  connection drops or the consumer errors partway through, the backup is incomplete and must be
  **restarted from the beginning**. Treat a stream backup as valid only once it has been written and
  closed in full. (For interrupted-transfer detection, the archive ends with the standard tar
  end-of-archive marker — a truncated stream is missing it.)
- **Always a full snapshot.** Unlike directory backups, there is no file sharing or incremental
  mode; every stream copies the entire database.
- **No management API.** `backups.list` / `verify` / `delete` / `purge` and `backups.restore`
  operate on the `BackupEngine` directory format and do **not** understand tar streams. Listing,
  verification, and retention are the caller's responsibility (e.g. verify the extracted directory
  opens, or carry a checksum alongside the archive).
- **Compaction cleanup is deferred for the duration of the stream.** File deletions are disabled
  while the snapshot is being read so a compaction cannot delete a file mid-stream. A _very_ slow
  consumer therefore lets obsolete SST files accumulate (transient disk pressure) until the stream
  finishes. This is the cost of streaming with no scratch copy; a fast consumer is unaffected.
- **The database must stay open for the whole stream.** Closing or destroying the database while a
  stream is in flight aborts it: the backup promise rejects, and `destroy()` throws rather than
  tearing the database down underneath the copy.
- **A consumer error aborts the backup.** If `stream.write()` rejects (or the stream is aborted), the
  backup promise rejects and the native producer stops.

---

## Checkpoints (related)

```typescript
await db.createCheckpoint('/path/to/checkpoint');
const branch = new RocksDatabase('/path/to/checkpoint');
branch.open(); // an independent, writable database
```

A checkpoint is not a backup in the `BackupEngine` sense — it is a point-in-time, fully independent
**sibling database**. It is included here because it is the fastest way to make a local copy and
shares the same "copy the whole database" shape.

### How it works

RocksDB **hard-links** the SST and blob files into the target directory when that directory is on the
**same filesystem** as the database, and **copies** them otherwise. Other files (the manifest, etc.)
are always copied. On the same filesystem this makes a checkpoint near-instant and near-zero extra
space — the hard links share the underlying data blocks until compaction diverges them.

### Caveats

- **Hard linking requires the same filesystem and filesystem/OS support.** When the target is on a
  different filesystem or volume, RocksDB falls back to a full **copy** (slower, full space). The
  same fallback applies on platforms/filesystems that don't support hard links — notably **Windows**,
  where checkpoints generally **copy** rather than link. Do not assume a checkpoint is cheap unless
  you know it lands on the same Unix filesystem as the database.
- **The target path must not already exist.** RocksDB creates the checkpoint directory; an existing
  path is rejected with `Create checkpoint failed: target path exists`. Parent directories are
  created for you, and cleanup of the directory is the caller's responsibility.
- **It writes to local disk.** Unlike a stream backup, a checkpoint is materialized on disk (as links
  or copies); it is not a way to avoid touching the filesystem.

---

## Choosing a mechanism

- **Scheduled, retained, local backups** → directory backups. You get incremental storage, a
  listing/verify/purge API, and a restore command.
- **Send a backup off-host without scratch disk** (upload to object storage, pipe over a socket) →
  stream backups. Just remember they are full snapshots and cannot be resumed.
- **A fast local copy to branch from or snapshot before a risky operation** → checkpoints, ideally on
  the same filesystem.
