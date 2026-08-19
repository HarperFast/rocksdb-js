This file provides guidance to AI codign agents like Claude Code (claude.ai/code), Cursor AI, Codex,
GitHub Copilot, and other AI coding assistants when working with code in this repository.

## Development Commands

### Building

- `pnpm build` - Full production build (TypeScript bundle + native C++ binding)
- `pnpm build:binding` - Incremental build C++ binding only (production)
- `pnpm build:binding:debug` - Incremental build C++ binding only (debug)
- `pnpm build:bundle` - TypeScript only (unminified)
- `pnpm build:bundle:minify` - TypeScript only (minified)
- `pnpm rebuild` - Configure and build C++ binding only (production)
- `pnpm rebuild:debug` - Native C++ binding only (with debug logging and coverage)

### Testing

- `pnpm test` - Run all tests with Vitest using Node.js
- `pnpm coverage` - Run all tests with Vitest and coverage report
- `pnpm coverage:native` - Native tests with gcov/lcov report in `coverage/native/html/` (Unix only)
- `node --expose-gc ./node_modules/vitest/vitest.mjs test/specific.test.ts` - Run single test file
- `pnpm test:bun` - Run all tests with Vitest using Bun
- `pnpm test:deno` - Run all tests with Vitest using Deno
- `pnpm test:stress` - Run all stress tests with Vitest using Node.js
- `pnpm test:native` - Build and run C++ GoogleTest unit tests (no Node runtime in test binary)
- `pnpm bench` - Run all benchmarks with Vitest using Node.js

### Code Quality

- `pnpm check` - Run type-check, lint, and format checking
- `pnpm fmt` - Format code with oxfmt
- `pnpm fmt:check` - Check code formatting with oxfmt
- `pnpm lint` - Code linting with oxlint
- `pnpm type-check` - TypeScript type checking only

### Development Workflow

- `pnpm clean` - Clean native build artifacts
- `pnpm build:bundle && pnpm rebuild:debug` - Full debug build for development

## Architecture Overview

This is a Node.js binding for RocksDB that provides both TypeScript and C++ layers:

### TypeScript Layer (`src/`)

- **`database.ts`** - Main `RocksDatabase` class extending `DBI` with transaction support
- **`backup.ts`** - `backups` namespace (restore/list/delete/purge/verify) over RocksDB's `BackupEngine`; backup creation is `RocksDatabase.backup()`
- **`store.ts`** - Core `Store` class wrapping native database with encoding/decoding
- **`transaction.ts`** - Transaction implementation for atomic operations
- **`dbi.ts` & `dbi-iterator.ts`** - Database interface and iteration logic
- **`encoding.ts`** - Key/value encoding with msgpack and ordered-binary support
- **`load-binding.ts`** - Native module loading and configuration
- **`parse-transaction-log.ts`** - Utility for reading raw transaction log files
- **`transaction-log.ts`** - Transaction log implementation for storing transaction related data
- **`transaction.ts`** - Transaction-specific context for transactional operations
- **`util.ts`** - Various helpers
- **`validate-transaction-log.ts`** - `validateTransactionLogStore()` over the native validator
  (`src/binding/transaction_log/transaction_log_validation.cpp`); also used by `backups.verify()`
  (strict mode) and the CLI `verify-logs` command

### C++ Native Layer (`src/binding/`)

Layout (include via `src/binding` root, e.g. `#include "core/encoding.h"`):

- **`core/`** - No `node_api.h`: encoding, `DBException`, platform helpers, debug logging
- **`napi/`** - N-API helpers, macros, async work (`BaseAsyncState`), module `binding.h`
- **`database/`**, **`transaction/`**, **`iterator/`**, **`transaction_log/`**, **`stats/`** -
  domain code and JS bridge classes
- **`binding.cpp`** - `NAPI_MODULE_INIT` entry point
- **`options/db_options.h`** - Parsed open options (plain C++)

`core/` and `transaction_log/` store/file code are suitable for **GoogleTest** without Node.
N-API surface remains covered by Vitest (`test/*.test.ts`). Native tests live in `test/native/`.

### Key Design Patterns

1. **Hybrid Sync/Async**: Operations return promises for disk I/O or immediate values for cached
   data
2. **Encoding Strategy**: Keys use ordered-binary encoding, values default to msgpack
3. **Store Pattern**: `Store` class encapsulates database instance and encoding logic, shared
   between `RocksDatabase` and `Transaction`
4. **Native Binding**: Uses node-gyp with C++20, links against prebuilt RocksDB libraries
5. **Backups**: Whole-database (all column families) via RocksDB's `BackupEngine`
   (`src/binding/database/backup.cpp`). Creating a backup is the `Database::Backup` instance
   method (needs the open DB); restore/list/delete/purge/verify are module-level functions
   operating on a backup directory with no open DB.
6. **Compression**: a **per-column-family** open option (`compression`), normalized in the TS Store
   layer (`normalizeCompression` — string | `{algorithm, level}` → native string + validated int32
   level) and applied to the target CF's options. Non-obvious points:
   - The algorithm is applied to **both** `ColumnFamilyOptions::compression` (SST blocks) **and**
     `blob_compression_type` — this codebase enables blob files for values ≥ 2KB, and blob
     compression defaults to none, so setting only block compression leaves large values
     uncompressed. `compression_opts.level` carries an optional level.
   - **Per-CF preservation is load-bearing and easy to get wrong.** RocksDB requires opening _every_
     CF at once with the options you pass, and does **not** restore persisted per-CF options on its
     own. So `DBDescriptor::open` calls `LoadLatestOptions` (`rocksdb/utilities/options_util.h`) to
     read each existing CF's persisted compression, opens each CF with _its own_ value, and applies
     the caller's request **only** to the target CF (`options.name`) — and only when it was
     _explicitly_ requested. A cold open of one CF must never restamp the others (that was the bug
     Kris caught: first-open order used to dictate every CF's algorithm). New CFs
     (`createRocksDBColumnFamily`) get the request/default. Because the OPTIONS file is the _only_
     authoritative source, a non-OK `LoadLatestOptions` for an **existing** DB (missing/corrupt
     OPTIONS) **fails the open** rather than falling back to defaults — falling back would reopen the
     non-target CFs with the base default and silently restamp them. Applying an explicit algorithm
     **without** a level resets `compression_opts.level` to `kDefaultCompressionLevel` (it must not
     inherit the target CF's persisted level, e.g. cold-reopening a zstd-level-19 CF as zlib).
   - The default is **LZ4** (overriding RocksDB's Snappy default), applied natively in
     `Database::Open` when unset; build-dependent, so it falls back to RocksDB's default when LZ4
     isn't linked. The default is marked **non-explicit** (`DBOptions::compressionExplicit`) so it
     never overrides an existing CF and a plain reopen inherits the live value.
   - A second in-process open of an already-open CF (the `DBDescriptor` is process-global, shared
     across handles/`worker_threads`) with an **explicitly different** algorithm, blob algorithm, _or_
     level is **rejected** (throws in `DBRegistry::OpenDB`, comparing all three against the live
     `GetOptions`). The level compared is the _effective_ request — omitting a level means
     `kDefaultCompressionLevel`, not "inherit" — and the blob check catches a legacy CF opened plainly
     with `block=snappy`/`blob=none` that a later explicit `snappy` open would otherwise leave with
     uncompressed blobs.
   - `supportedCompression` (module constant from `rocksdb::GetSupportedCompressions()`) is the
     source of truth; name↔enum mapping lives in the Node-free `core/compression.{h,cpp}`
     (GoogleTest-covered). The `db.compression` getter returns `{ algorithm, level? }` read live via
     `DB::GetOptions` (level omitted when it is the default sentinel).
   - `scripts/configure-rocksdb.mjs` (run by `binding.gyp` at configure) provisions the pinned
     prebuild then emits the compression libs to link as **whitespace-free** `-l` flags / `.lib`
     names (never absolute paths), resolved via a single `library_dirs` entry — so a repo checked
     out under a path with spaces still links (gyp `<!@()` splits output on whitespace).

### Transaction Architecture

- Optimistic (default): Conflicts detected at commit time
- Pessimistic: Conflicts throw immediately on detection
- Both modes support async/sync APIs with automatic commit/rollback

### Iterator Design

Uses `ExtendedIterable` wrapper around native iterators for array-like methods (map, filter, etc.)
with lazy evaluation.

### Event Emitters

The codebase has **two** event surfaces backed by the same `EventEmitter` class in
`napi/event_emitter.h`:

- **Per-database**: instance methods on `RocksDatabase` (`db.addListener`, `db.notify`,
  `db.listeners`, `db.removeListener`). Listeners are scoped to a `DBDescriptor` and cleaned
  up when the owning `DBHandle` closes. Native exports live on the `Database` class prototype.
- **Process-global**: static methods on `RocksDatabase` (`RocksDatabase.on`, `.addListener`,
  `.off`, `.removeListener`, `.listenerCount`, `.notify`). Used for events that have no
  natural database context — e.g. warnings from the transaction log layer. Native exports
  live on the binding module root (`binding.addListener`, etc.), wired via `GlobalEvents::Init`.
  The underlying `EventEmitter` is a C++ magic-static singleton — it is **shared across
  every Node env that loads this .node binary in the same process**, so listeners
  registered on the main thread will receive events emitted from `worker_threads`
  workers and vice versa. When an env is torn down (e.g. a worker exits), its
  cleanup hook calls `EventEmitter::removeListenersByEnv(env)` so that env's
  tsfns / napi_refs are released and the singleton is left with no dangling pointers.

When wiring a new listener-related export from TypeScript, pick the right one: the binding
module's `addListener` is **global**; the per-DB `addListener` is on the `Database` class.
`load-binding.ts` renames the global exports to `addGlobalListener` / `removeGlobalListener`
/ `globalListenerCount` / `globalNotify` to make the distinction explicit in TS.

C++ code that needs to emit to JS without a database context should call
`emitGlobalEvent(key, data)` from `napi/global_events.h`. Use namespaced keys
(`'transactionLog:warning'`) for internal events to avoid collisions with user-defined ones.

### Commit execution

Async `Transaction.commit()` does not use the libuv threadpool: each database
has a dedicated commit thread (`CommitWorker`, owned by the shared
`DBDescriptor`) that runs the txn-log write + RocksDB commit in dispatch order,
so slow commits cannot starve fs/dns/crypto/async-get work sharing the libuv
pool. `ROCKSDB_JS_COMMIT_THREAD` selects the mode (`0`/`false` = legacy libuv
path, default = single lane, `2` = experimental two-lane txnlog→commit
pipeline). Completions are marshalled back to the originating env via per-env
tsfns held on the descriptor under `commitMutex`; the commit thread calls them
under that mutex and a dying env's tsfn is released from the module env-cleanup
hook (`DBRegistry::ReleaseCommitCompletionsByEnv`) — the same env-teardown
discipline as `EventEmitter::notify` above. A per-commit tsfn acquire is NOT
sufficient (env teardown does not honor tsfn acquire counts); see
`test/commit-teardown.test.ts` and the `ROCKSDB_JS_COMMIT_DELAY_MS` test seam.

## Environment Variables

- `ROCKSDB_VERSION` - Override RocksDB version (default from package.json, or 'latest')
- `ROCKSDB_PATH` - Build from local RocksDB source instead of prebuilt
- `MINIFY=1` - Enable minification of TypeScript bundle
- `KEEP_FILES=1` - Don't delete temporary test databases for debugging purposes
- `ROCKSDB_JS_COMMIT_THREAD` - Async-commit execution mode: `0`/`false` = legacy
  libuv threadpool, unset = dedicated per-database commit thread (default),
  `2` = experimental two-lane pipeline
- `ROCKSDB_JS_COMMIT_DELAY_MS` - Test-only: delay on the commit thread before
  each completion callback (widens teardown race windows)

## Test Structure

- **Vitest** (`test/*.test.ts`): TypeScript integration tests; `pnpm test` / `pnpm coverage`
- **GoogleTest** (`test/native/*.cc`): C++ unit tests; `pnpm test:native` /
  `pnpm coverage:native` (lcov on Unix)
- `test/lib/util.ts` contains Vitest utilities
- Coverage: TypeScript in `coverage/`; native GTest in `coverage/native/`
- **No `tsx`**: `.ts`/`.mts` scripts, worker files, and `src` itself run under Node's native type
  stripping (the `engines` floor `^22.18.0 || >=24.0.0` is where it's unflagged). Rules for code Node
  loads directly (i.e. outside Vitest/tsdown, which do their own resolution):
  - **Real file extensions in every relative import.** `src` imports siblings as `./foo.ts`, not
    extensionless or `.js` — native strip does **not** remap `.js`→`.ts`. `allowImportingTsExtensions`
    - `noEmit` in `tsconfig.json` let `tsc` accept the `.ts` specifiers (tsdown ignores `noEmit` and
      still emits `dist`). This is why the unit-test worker `.mts` (`test/workers/`) and spawned-child
      `.mts` (`test/fixtures/`) import `../../src/index.ts` directly — so those tests need **no build
      step** (Vitest resolves its own `.js` specifiers; only the native-loaded files must use `.ts`).
      **Stress tests and benchmarks are the deliberate exception**: their workers import the built
      `dist` (`stress-test/workers/`, `benchmark/setup.ts`) because they exercise the shipped artifact,
      not source — so `pnpm test:stress` / `pnpm bench` build first (both CI workflows do).
  - **Mark type-only imports with `type`.** `verbatimModuleSyntax` is enabled: native strip can only
    erase `import type` / `import { type X }`, not a value-style `import { X }` that happens to be a
    type — that would survive to runtime and fail against a module (e.g. `dist`) that never exported
    the type. `tsc` enforces this.
  - **Fixture helpers must be `src`-free** only where they'd otherwise pull a heavier graph — e.g.
    `createWorkerBootstrapScript` lives in `test/lib/worker-bootstrap.ts` (no `src` import), separate
    from `test/lib/util.ts` (which imports `src`); every call site imports it directly.
- **GC is not exposed to Deno's test workers** (#770): tests that force collection run in Vitest's
  worker, not the process you launched. Node's `threads` pool inherits `--expose-gc` through
  `execArgv`, and Bun exposes `Bun.gc()`, but Deno uses the `forks` pool and
  `--v8-flags=--expose-gc` applies only to the CLI process it was passed to — so `globalThis.gc` is
  undefined in every Deno worker and each `skipIf(!globalThis.gc)` test silently skips there. Guard
  GC-dependent tests with `skipIf`, never with a throw. `DENO_V8_FLAGS=--expose-gc` is the fix (the
  environment is inherited by children) but cannot land until #771 is fixed: the restored coverage
  makes `test/lock.test.ts` and one macOS `verification-table.test.ts` case fail.

## Important Implementation Notes

1. **Key Encoding Order**: Always encode values before keys when using `sharedStructuresKey` to
   avoid overwriting shared key buffer
2. **Buffer Management**: Store uses reusable buffers for performance (`keyBuffer`, `encodeBuffer`)
3. **Memory Management**: Native layer handles RocksDB memory, TypeScript layer manages encoding
   buffers
4. **Error Handling**: C++ errors are translated to JavaScript exceptions via N-API
5. **Transaction log size is append-owned**: `TransactionLogFile::size` is the authoritative written
   extent, mutated only by the append path (and the one-time reopen correction before the first
   append). Read/index paths (e.g. `findPositionByTimestamp`) must never truncate it — a zero
   timestamp seen mid-index during concurrent appends is a not-yet-visible memory-map artifact, not
   EOF. Reads during writes are bounded by the committed position, not `size` (see
   `hasAppendedSinceOpen`; HarperFast/harper#1148).
6. **Shared DBDescriptor teardown is cross-env**: a `DBDescriptor` is process-global and shared by
   every env that opens the same path (`worker_threads` workers included), so multiple threads can
   reach `DBRegistry::CloseDB` for one descriptor at the same time — e.g. several worker envs tearing
   down at once, each via its own `Database` finalizer. The purge decision (refcount check),
   `descriptor->close()`, and the registry-map erase must therefore be coordinated under
   `databasesMutex` and must never dereference a raw pointer/iterator into the map across an unlocked
   region — a concurrent erase frees that node and the survivor closes a freed descriptor (locking a
   destroyed mutex; surfaces on glibc as "malloc(): unaligned tcache chunk detected"). The current
   design takes a `shared_ptr` copy of the descriptor under the lock as a single-purge claim (the copy
   pushes `use_count` past the purge threshold so a racing `CloseDB` skips) while leaving the entry in
   the map — descriptor non-null and `isClosing()` — until `close()` finishes, so a concurrent
   `OpenDB` keeps waiting on the entry's condition instead of re-opening the path mid-close. This
   purge tail lives in `DBRegistry::PurgeIfUnreferenced`. Async ops that pin the descriptor with
   their own `shared_ptr` for the duration of a copy (backup, backup stream, checkpoint) make a
   racing close skip the purge (`use_count > 1`), so their state destructors re-run
   `PurgeIfUnreferenced` after releasing the ref — without that retry the skipped purge is permanent
   and the entry (plus the open RocksDB) leaks (HarperFast/rocksdb-js#672).
7. **One writable BackupEngine per backup directory (kernel advisory lock)**: each backup op opens its
   own short-lived `rocksdb::BackupEngine`/`BackupEngineReadOnly` (`src/binding/database/backup.cpp`), and
   RocksDB only serializes work _within_ a single engine — it has no cross-engine lock on the directory.
   Two writers on the same directory (two `db.backup()` calls, or a `backup` racing a `delete`/`purge`),
   in the same process or different ones, collide on the per-backup staging dir and both fail,
   potentially leaving zero usable backups. A single writer is enforced by holding a non-blocking
   exclusive OS advisory lock on the `.backup.lock` file at the directory root — `flock` on POSIX,
   `LockFileEx` on Windows. Backup creation acquires it natively inside `Database::Backup`
   (`runCreateBackup` in `src/binding/database/backup.cpp`), which first creates the backup directory
   (with missing parents); `backups.delete` and `backups.purge` acquire the same lock from JS via
   `withBackupDirLock` in `src/backup.ts`, which first rejects a missing backup directory with a
   clear error — `tryFileLock` itself creates missing parents, so without that explicit check a
   `delete` on a typo'd path would conjure an empty directory. The lock is taken **entirely in
   native code** (`tryAcquireFileLock` / `releaseFileLock` in `src/binding/core/file_lock.cpp`,
   exposed generically as the binding's `tryFileLock`/`fileLockRelease` — a public utility API, not
   backup-specific): native opens the file,
   locks it, and later closes its OS handle, returning only an opaque uint32 token to JS. **No descriptor
   crosses the JS boundary** — this is deliberate: the addon
   statically links its own C runtime (`binding.gyp` `RuntimeLibrary: 0` = `/MT`), so a Node/libuv fd is
   not resolvable here and `_get_osfhandle` on such an fd fast-fails the process (`0xC0000409` on Windows).
   The kernel owns the lock, so there is **no staleness heuristic**: it is released when the handle closes —
   normal release, crash, `kill -9`, container exit — and a dead holder can never wedge the directory. (An
   earlier pidfile design broke in containers: pid liveness is meaningless across pid namespaces — every
   container has a pid 1 — and pidfile reclaim races are only fully eliminated by OS locks.) The lock
   conflicts per _open file description_, so it excludes across processes, containers sharing a volume
   (same kernel), and `worker_threads` — an in-memory lock cannot. It does **not** coordinate across
   hosts: `flock` on many network filesystems (NFS `local_lock`, CIFS, 9p) is node-local, so two hosts
   sharing a backup volume can both acquire — a caller-managed hazard the old pidfile also could not
   prevent (its reclaim used host-local pid liveness). On filesystems that don't implement `flock` at
   all (`EOPNOTSUPP`/`ENOTSUP` — e.g. the FUSE/9p mounts behind Docker Desktop bind mounts on
   macOS/Windows), native **degrades to a no-op "acquired"** rather than making backups impossible:
   cross-writer protection is forfeited only where it was unattainable. Native opens the handle with
   `O_CLOEXEC` (POSIX) / non-inheritable (Windows) so a spawned child can't inherit it and hold the lock
   past release. On Windows the locked byte sits far past EOF because Windows range locks are mandatory and
   would otherwise block a contender from reading the file. The file is **never unlinked** — unlink-on-
   release races a concurrent acquirer holding a handle to the removed inode (two "winners" on different
   inodes); an unlocked, empty `.backup.lock` is the steady state. Contention **rejects**; it does not queue, so a caller issuing
   overlapping backups to one directory must handle the "locked" error (e.g. retry).
   `backups.restore` holds the same lock in **shared** mode (`tryFileLock(file, true)` → `flock`
   `LOCK_SH` / `LockFileEx` without `LOCKFILE_EXCLUSIVE_LOCK`) for its source read: concurrent
   restores coexist, but a writer racing a restore rejects instead of deleting the files the restore
   is copying — the asymmetry matters because the default `purgeAllFiles` restore mode wipes the
   destination before copying, so a restore failed mid-purge leaves no usable database while a
   rejected writer just retries. A restore is pure-read (`BackupEngineReadOnly` + copy out), so the
   shared path must not require write access to the backup directory: it opens the lock file
   **read-only** and never creates it (the exclusive path still opens read-write / creates), letting
   a restore lock an existing `.backup.lock` on an immutable/WORM or read-only-mounted backup store.
   If even the read-only open fails because the media is read-only for **every** process (`EROFS` on
   POSIX, `ERROR_WRITE_PROTECT` on Windows), the shared lock **degrades to a no-op "acquired"** rather
   than hard-failing the restore — the same reasoning as the `flock`-unsupported degrade: no writer
   can exist on a directory nothing can write, so the lock would protect nothing there. Permission
   denial (`EACCES`/`EPERM`, `ERROR_ACCESS_DENIED`) is deliberately **not** degraded: it means only
   the _calling_ identity is blocked, so a more-privileged writer (e.g. a `purge` running as the
   service account that created the backup) could still hold a real exclusive lock — degrading there
   would let a lesser-privileged restore silently read a directory mid-purge. Those cases hard-fail.
   (Exclusive acquisition has no degrade either — writers legitimately need write access and hard-fail.)
   The remaining read-only ops (`list`, `verify`) are not locked since
   concurrent readers are safe (and locking them would make cheap listings reject during a long
   backup); a `list`/`verify` racing a `delete`/`purge` is a caller-managed hazard. Different
   directories are independent (separate lock files) and run fully in parallel.
8. **Backup disk-space preflight is conservative and best-effort**: directory-target `db.backup()`
   preflights the destination volume before taking the writer lock (`checkBackupDiskSpace` in
   `src/binding/database/backup_disk_space.cpp`, defaulted on via the `checkDiskSpace` option). It is
   extracted into a Node-free translation unit **so a GoogleTest can exercise it** — N-API TUs can't
   link into the native-test target — with a `rocksdb::Env*` param a fake env overrides in tests. The
   required size is deliberately the _full_ live-file footprint (`GetLiveFilesStorageInfo`, summed) plus
   the transaction-log snapshot bytes when `transactionLogs` is set (those write to the same volume but
   aren't RocksDB live files) plus the current memtable when flushing — never the incremental delta,
   because a backup only ever _copies_ files, so full size can't under-estimate the bytes written. It
   therefore over-rejects incrementals to tight volumes (opt out with `checkDiskSpace:false`) and, like
   the backup lock, **degrades to a skip** where the answer is untrustworthy: `GetFreeSpace`
   unsupported/errored or reporting 0 (which also lets a genuinely-full local volume through — a real 0
   is indistinguishable from the spurious 0 some network filesystems report). The stream-target backup
   path never opens a `BackupEngine` against a volume and is not checked.
9. **Transactional reads are database-wide, but async column-family pins end before teardown**:
   a RocksDB transaction can read any column family in its database, so a read issued through another
   `RocksDatabase` must use that caller's `ColumnFamilyDescriptor`, not the transaction's original
   `DBHandle`. Register setup briefly against the caller's `DBHandle` before copying its descriptor,
   so a cross-environment close cannot reset the descriptor between the open check and the pin. The
   transaction itself must be registered before inspecting `txn`/state, then that registration is
   transferred to the queued async state; failed N-API setup must release refs/work/state without
   double-unregistering or retaining the descriptor.
   cold-cache async path then pins that descriptor in `AsyncGetState` while the worker uses its native
   column-family handle, and resets the pin **before** `signalExecuteCompleted()`; that
   signal can unblock transaction/database close, which destroys column families before the RocksDB
   database. Do not retain a raw/native column-family handle into the N-API completion callback or
   inspect a concurrently closing `DBHandle` from the worker. Transactional count iterators must also
   pass the transaction snapshot through `ReadOptions`; `disableSnapshot` intentionally leaves it null
   so counts observe the latest committed state.
10. **Retained memtable history must fit the WriteBufferManager budget**: `max_write_buffer_size_to_maintain`
    is a floor, not a cap — RocksDB trims history back down to it and never below — and that memory is
    charged to the process-wide WriteBufferManager. A target above the manager's budget therefore fills
    the budget with memory that is never released, and a manager built with `allowStall` stalls every
    write to that database permanently rather than until a flush catches up. `buildColumnFamilyOptions`
    resolves the derived (`-1`) default to 0 whenever a stalling manager is configured for exactly this
    reason (`resolveMaxWriteBufferSizeToMaintain`); an explicit caller value is honored as given, and
    sizing it against the budget and the column-family count is then the caller's job. The general trap:
    `DBOptions` defaults that derive a large value were sized when they reached one column family, so
    widening where an option applies means re-checking its default against every shared budget it
    competes for.
11. **A corrupt transaction-log frame ends an entry, not the log**: framing breaks come in two
    shapes and the reader must not conflate them. A **torn tail** has nothing valid behind it, so
    the break is genuinely end-of-log — that is what `recoverTail()` truncates at open. A **mid-log
    break** (a partial `ENOSPC`/`EDQUOT` append the process survived) has intact, already-committed
    entries appended _after_ it; `recoverTail()` deliberately leaves such a file alone rather than
    discard them, and rotated files are never rescanned at all. `query()` therefore reports the
    break as a `CorruptFrameError` carrying `resyncPosition` (where framing resumes, per the same
    heuristic as `validFramingResumes()`) and **leaves iteration positioned there**, so a caller
    that calls `next()` again recovers the entries past it. Treating the throw as terminal amputates
    every later entry in the file permanently — each drain restarts from the same resume cursor and
    re-throws at the same offset, which is how HarperFast/harper#2016 lost 2.2 days of acknowledged
    writes and #2063 starved a replication stream for 11 days. Keep `RESYNC_MIN_FRAMES` in
    `transaction-log-reader.ts` and `transaction_log_recovery.cpp` in step.

    The resync scan must be bounded by the **written extent** (`getLogFileSize`, which returns the
    append-owned `TransactionLogFile::size` — see invariant 5 — not the physical or mapped size).
    An uncommitted read's own limit is the pre-extended memory map, and every offset in that zero
    fill reads as an end-of-entries marker: scanning against it both loses the exact-end signal and,
    if a zero were taken as a terminator, would let a chain "end" anywhere in megabytes of padding.
    Resolve it only on a break — `getLogFileSize` crosses into native and takes the store mutex, so
    a per-frame call would tax every healthy read.

12. **A dropped transaction must release itself**: `DBDescriptor::transactionAdd` holds a **strong**
    `shared_ptr` (the parallel `closables` entry is weak), so the registry alone keeps a
    `TransactionHandle` alive and `~TransactionHandle` — hence `close()`, the only `ClearSnapshot()`
    path — is unreachable while it is registered. The `NativeTransaction` finalizer therefore calls
    `onWrapperCollected()` before dropping its reference: once V8 has collected the wrapper, no JS
    code can commit, abort, retry, or read through that handle again, so it is closed. The one
    exception is `state == Committing`, where `TransactionCommitState` still owns the handle and
    closing would cancel a commit mid-flight; the commit-completion paths close it instead — success
    always closes, and the failure paths (which deliberately leave the handle open for a caller that
    may retry) check `wrapperCollected`, because there is no caller left. Without this a transaction
    dropped without `commit()`/`abort()` pinned `rocksdb.oldest-snapshot-time` for the life of the
    process, so RocksDB could never discard obsolete versions for that database — restart was the
    only recovery (HarperFast/harper#2107; `test/transaction-orphan-gc.test.ts`). Two constraints on
    any redesign here: the registry reference cannot simply be made weak, because an async `get`
    holds a raw `TransactionHandle*` (`AsyncGetState<TransactionHandle*>`) and relies on `close()`'s
    `cancelAllAsyncWork()`/`waitForAsyncWorkCompletion()`, which a plain destructor race would skip;
    and `registryStatus()` may only report handle fields that are fixed before the handle is
    published to the registry (`id`, `createdAt`), because `txnsMutex` covers the map's membership
    while the mutable fields' writers — the owning thread's read paths, the commit-completion
    callback — hold no lock at all.

## Debugging native heap corruption

AddressSanitizer is the first choice (`ROCKSDB_ASAN=1 node-gyp rebuild` toggles `-fsanitize=address`
on the binding via `binding.gyp`). On Linux, `LD_PRELOAD` the libasan shared object to run the
instrumented `.node` under stock node; `.github/workflows/benchmark-asan.yml` does this and loops the
worker benchmarks. **ASan does not work locally on recent macOS** — the runtime deadlocks at init even
for a trivial binary, and Node additionally hangs under a DYLD-injected ASan runtime. Use Apple's
**Guard Malloc** there instead (no rebuild needed): `DYLD_INSERT_LIBRARIES=/usr/lib/libgmalloc.dylib
MallocScribble=1 node ...` faults immediately on an out-of-bounds access or use-after-free (it works
with `worker_threads`). To reproduce a teardown/lifecycle race, drive the relevant workers in a tight
loop under Guard Malloc and capture the stack with `lldb -b -o 'break set -n __cxa_throw' -o run -o bt`.
