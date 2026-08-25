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

**Run `pnpm fmt` before every commit** (or `pnpm check` to also type-check and lint) — CI runs
`pnpm fmt:check` and fails the build on unformatted code. Note the scope: oxfmt formats **TS/JS/JSON
only**. It does **not** touch C++ or Markdown, so changes to `src/binding/**` and to docs like this
file (`AGENTS.md`) are not auto-formatted and must be checked by hand — a mis-numbered invariant or a
stray C++ indent will pass `fmt:check` untouched.

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
- `ROCKSDB_JS_TXN_GET_DELAY_MS` - Test-only: delay a transaction's cold-cache async get before
  it reads (exercises orphan cleanup past the async-work wait timeout)
- `ROCKSDB_JS_PARK_TIMEOUT_MS` - Bounded wait (default `5000`) before a
  coordinated-retry commit parked on a conflicting holder's VT lock resolves
  RETRY_NOW unconditionally, in case the holder never releases (see
  "Coordinated retry" note below). Read once per process (a function-local
  `static`, like the other two above — `::getenv` is not safe against a
  concurrent `::setenv` from a `process.env` write, and a park runs on whichever
  env's JS thread owns the transaction), so it must be set in the environment a
  process is started with. Values below `50` are clamped up to it, and `0` (an
  ambiguous "disable the bound") falls back to the default like any malformed
  value. There is no opt-out: a deployment that would rather wait than fail a
  legitimately slow holder raises the value instead
- `ROCKSDB_JS_WRITE_STALL_DEBOUNCE_MS` - Rate-limit window (default `1000`) for the
  per-database `'writeStall'` event. The event is rising-edge only (fires when a
  column family enters a stall); during a sustained oscillating stall it re-emits
  at most once per window. Recovery is not pushed (see `isWriteStalled()`). Resolved
  once at `DBDescriptor` construction on the JS thread (the emit path runs on a
  RocksDB background thread; this avoids `::getenv` there — same
  `::getenv`-vs-`process.env` caveat as `ROCKSDB_JS_PARK_TIMEOUT_MS`), so it must be
  set in the environment a process is started with. `0` disables the window (every
  rising edge emits); malformed/negative falls back to the default

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
   `hasAppendedSinceOpen`; HarperFast/harper#1148). The other half of that contract is that
   an append that fails part-way (ENOSPC, a short write on a full volume) retires the segment
   without truncating it: `writeBatchToFile` reports the landed extent, `writeEntriesV1` marks
   any positive or unknown extent unappendable, and `TransactionLogStore::writeBatch` rotates
   it before propagating the error. The rotation is allowed only after the last safe logical
   extent has been written and synced to that segment's preallocated marker under
   `transaction_logs/.append-boundaries/<store>/`; retirement overwrites that fixed extent rather
   than extending it, and a filesystem that still cannot persist the overwrite fails closed instead
   of rotating. Initial creation writes and syncs a temporary marker before atomically publishing the
   final name, so neither a crash nor a concurrent opener can observe a short initialization. The
   marker carries a token and complemented boundary so a torn/corrupt marker fails load closed. On
   restart, registered files, readers, purge counting, backup snapshots, and strict validation all
   use the marked logical prefix and never expose the orphaned physical tail. The marker is not copied
   into backups: the copied prefix is already a clean canonical `.txnlog`.
   A known-zero-byte failure leaves the segment and its zero marker reusable.
   Only the **active** segment may have a marker _created_ for it (a retired segment keeps the one it
   already earned), and startup discovery cannot know which segment that is until the whole directory
   has been scanned — directory iteration order is unspecified, so any segment can briefly hold the
   highest sequence. `registerLogFile()` therefore registers without opening, and `load()`
   marker-enables and opens the surviving current file after discovery. Promoting eagerly minted a
   marker for a segment that is never appended to and, on Windows (where the handle is opened without
   `FILE_SHARE_DELETE`), left every superseded segment undeletable by anything outside the process for
   the life of the store.
   **The physical extent tracks `size` on POSIX only.** There the fd is `O_APPEND`,
   so writes go to physical EOF, not to `size`, and leaving orphaned bytes makes every later
   append land after a partial entry: a mid-file framing break that `recoverTail()` deliberately
   will not repair, so every entry after it is unreachable (HarperFast/rocksdb-js#748). On
   Windows `size` is the logical end of entries only — an active segment is pre-extended to
   `maxFileSize` with `SetEndOfFile` so it can be mapped (`getMemoryMapLocked`), its physical
   size stays `maxFileSize` for its whole life with a zero-padded tail, and end-of-entries is
   found by the zero-timestamp convention instead. Windows appends seek to `size` first, so an
   orphan is overwritten rather than skipped past — but a _shorter_ next batch would leave the
   orphan's stale bytes past its own end, reading as an entry instead of the marker, so retirement
   is the rule on both platforms. Transactions are never split across segments: a batch that does
   not fit rotates before writing, while one batch may exceed `transactionLogMaxSize` in an empty
   segment. This keeps a failed append from stranding an unflagged transaction prefix in an earlier
   file. Initialization owes the same discipline:
   a header write that lands short removes the file, since a size in `(0, HEADER_SIZE)` fails
   `open()`'s validity check on every future open and freeing disk space would not heal it.
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

12. **Coordinated retry parks on a lock, bounded by a descriptor-owned timeout**: a `coordinatedRetry`
    commit that loses a conflict (`IsBusy`) parks instead of rejecting immediately —
    `completeCommitWork` (`src/binding/transaction/transaction.cpp`) registers a wake callback on the
    conflicting VT slot's `LockTracker` (via `addWakeCallback`) and resolves `RETRY_NOW` only when
    that lock's last holder releases (`VerificationTable::releaseWriteIntent` → `LockTracker::wake`).
    A holder that never releases — a leaked/abandoned transaction, or a wake lost to a bug elsewhere —
    would otherwise park forever (harper#2001: a worker's write path disabled for 5+ hours until
    restart). `ParkTimeoutRegistry` (`db_descriptor.{h,cpp}`) bounds this with
    `ROCKSDB_JS_PARK_TIMEOUT_MS` (default `5000` — the top of this fix's requested 2-5s range, to
    leave maximum headroom for a holder that is merely slow rather than abandoned, since a timeout
    consumes a `coordinatedRetry` attempt exactly like a real wake does and `maxRetries` is finite):
    one registry, and one lazily-started timeout thread, **per descriptor** — joined at
    `finishClose()` (and again, idempotently, from the destructor as a safety net, matching
    `commitWorker`) — tracks every outstanding deadline instead of spawning a thread per park (the
    contention path is exactly where an abandoned holder makes parks dense, so per-park threads would
    be a resource cliff, not a fix). Deliberately a plain `std::thread`, not a `uv_timer_t`: this addon
    ships one prebuilt binary across Node ABI versions via N-API, and libuv's struct layout is not part
    of that stable surface.

    **A `LockTracker` wake callback runs under the process-global VT `writerMutex_`, so it must not
    block and must not re-enter the VT or `DBRegistry`.** `LockTracker::wake()` invokes its callbacks
    inline and both callers (`releaseWriteIntent`, `cancelForDB`) hold that mutex across the whole
    function. Re-entering the registry from there self-deadlocks: `DBRegistry::PurgeIfUnreferenced`
    can claim the purge and call `finishClose()` → `cancelForDB()` → a second lock of the same
    non-recursive `writerMutex_`, wedging every database's write-intent path process-wide — the exact
    symptom this note exists to fix. It is also an AB-BA against `finishClose`'s `txnsMutex` →
    `writerMutex_` order, and it would run a flush, a manual compaction, `WaitForCompact` and thread
    joins under the global VT lock. That is why `ParkTimeoutRegistry` is a standalone object owned by
    the descriptor through a `shared_ptr` rather than state on the descriptor itself: the wake closure
    captures a **`std::weak_ptr<ParkTimeoutRegistry>`** and calls only `fire(id)`, which touches one
    mutex and one map. Weak, not raw, because a park can end up registered on a tracker installed by a
    _different_ database on a colliding VT slot (`VerificationTable::lockSlotForWrite` joins an existing
    tracker without retagging its `dbId`), so that lock's eventual release wakes a park whose own
    database may already have closed — `cancelForDB()` only wakes trackers tagged with _its own_
    `vtEpoch`, so it cannot be relied on to have resolved a foreign-`dbId` park first. Weak **to the
    registry and not to the descriptor** because a `weak_ptr<DBDescriptor>::lock()` is a transient extra
    reference, and `PurgeIfUnreferenced` decides on `use_count() <= 1`: a racing close would see the
    inflated count, skip the purge, and leak the registry entry plus the open RocksDB — the
    HarperFast/rocksdb-js#672 hazard, which the wake path cannot repair by retrying the purge (that is
    the re-entrancy above). `.lock()` failing is the expected outcome once the owning database closes:
    `ParkTimeoutRegistry::shutdown()` (called from `finishClose()` right after `cancelForDB`, before
    the descriptor can be destroyed) unconditionally resolves every park it still holds regardless of
    whether the real holder ever wakes it, so by the time the weak reference can fail, the park has
    already settled.

    Each park is identified by a monotonic `uint64_t id`, not its entry's address: `LockTracker::wakeCallbacks`
    has no removal API (see the gap noted below), so a stale closure can outlive its entry, and an
    address-keyed lookup risks resolving a _different_, later park that reused the same freed heap
    address. The timeout thread and the LockTracker wake callback race through one heap-allocated
    `std::atomic<bool>` per park (independent of the per-park `RetryNowContext`, whose refs/TSFN the
    winning side's release eventually frees) — whichever fires first calls+releases the TSFN under the
    registry's `mutex` and erases the entry; the loser finds it already gone and touches nothing. That
    same mutex is what a dying env's `releaseByEnv` (wired into the module env-cleanup
    hook next to `ReleaseCommitCompletionsByEnv`) takes to cancel — release without calling — that
    env's pending parks before Node frees their tsfns; `retryNowCallJs` also guards `env == nullptr`
    like `commitCompletionCallJs` does, for the same tsfn-queue-drained-during-teardown reason. Parks
    are indexed twice, by id and by deadline (`std::multimap`): `fire()` needs an O(1) lookup because it
    runs under the global VT mutex, and the timeout thread needs the earliest deadline on every wakeup
    without an O(N) scan on that same lock. Known gap: `LockTracker::wakeCallbacks`
    itself has no removal API. Before this change an abandoned holder accrued one inert callback per
    waiter and then everything hung; now each waiter re-parks (and re-registers) every
    `ROCKSDB_JS_PARK_TIMEOUT_MS` up to `maxRetries`, so registrations accumulate per _retry_ rather than
    per incident for as long as it lasts (each is inert once its own park resolves, so this is a
    memory-growth concern, not a correctness one) — deferred rather than risking an unreviewed change to
    `verification_table.cpp`'s concurrency invariants under this fix's scope.

13. **A dropped transaction must release itself**: `DBDescriptor::transactionAdd` holds a **strong**
    `shared_ptr` (the parallel `closables` entry is weak), so the registry alone keeps a
    `TransactionHandle` alive and `~TransactionHandle` — hence `close()`, the only `ClearSnapshot()`
    path — is unreachable while it is registered. The `NativeTransaction` finalizer therefore calls
    `onWrapperCollected()` before dropping its reference: once V8 has collected the wrapper, no JS
    code can commit, abort, retry, or read through that handle again, so it is closed. The one
    exception is `state == Committing`, where `TransactionCommitState` still owns the handle and
    closing would cancel a commit mid-flight; the commit-completion paths close it instead — success
    always closes, and the failure paths (which deliberately leave the handle open for a caller that
    may retry) check `wrapperCollected`, because there is no caller left. Other dependents defer the
    orphan close without blocking the V8 finalizer: a cold-cache async get owns a
    `shared_ptr<TransactionHandle>` and retries after its async registration is released, while a
    transaction-backed `DBIteratorHandle` owns the handle and keeps `activeIteratorCount` nonzero
    until the RocksDB iterator is reset. The last dependent closes the orphan. Without this a dropped
    transaction either pinned `rocksdb.oldest-snapshot-time` for the life of the process or, after
    orphan cleanup was added, could be destroyed under an async read/live iterator
    (HarperFast/harper#2107; `test/transaction-orphan-gc.test.ts`). Two constraints on any redesign
    here: the registry reference cannot simply be made weak, because dependents need the coordinated
    cancellation and transaction-destruction path in `close()`; and `registryStatus()` may only report
    handle fields that are fixed before publication (`id`, `createdAt`), because `txnsMutex` covers
    map membership while mutable-field writers hold no lock.

14. **A recovered active transaction-log file ends on a transaction boundary when recovery can
    prove one**: only a batch's final entry
    carries `TRANSACTION_LOG_ENTRY_LAST_FLAG`, so a crash mid-batch leaves whole, well-framed
    entries that are a _prefix_ of a transaction. `recoverTail()` discards them
    (`discardUnclosedTransaction`) rather than leaving them for the committed watermark to step
    around: kept bytes are only invisible until the next commit moves the watermark past them, and
    then that batch's flag closes the phantom group — two source transactions merged into one for
    anything grouping on the flag. Discarding is safe because `writeBatch()` completes before
    `Transaction::Commit()` in every commit path and both commit-thread lanes preserve dispatch
    order, so an interrupted log write is always the newest thing in the log and its RocksDB commit
    never ran. Recovery walks entry headers via positional reads (never a whole-file buffer);
    payload bytes are skipped. Discarding is gated on proof that the writer sets the flag — a
    boundary earlier in the same file — plus a single timestamp across the trailing run. Callers can
    assign repeated timestamps,
    but an earlier transaction would still carry its own flag and reset the run. Without that proof
    the bytes are kept and warned about: a legacy batch split across a rotation has no boundary in
    the active file, and a log written before the flag existed would otherwise be truncated wholesale.
    Recovery reads `txn.state` before repairing the active file and never truncates below its
    same-file flushed offset: a missing flag can be media corruption on a batch RocksDB already
    absorbed, not proof that the commit never ran.
    `TransactionLogStore::load()` seeds from the latest proved boundary, walking backward across
    legacy rotation-spanning batches until it reaches a boundary or the `txn.state` floor, so recovery never
    hides entries already absorbed by RocksDB. Both platforms truncate; Windows first drops the cached
    mapping because mapped ranges prevent `SetEndOfFile` from shrinking the file. Windows uses the same
    physical truncation when the scan detects a torn tail, but its pre-extended zero padding makes an
    entry with a durable header and partially durable payload look complete; detecting that case needs
    a payload checksum. Recovery runs before mappings can be handed to readers.
15. **A callback-style native method owes its caller exactly one settled callback on every path**:
    `Flush` and `Compact` take `resolve`/`reject` and used to `return` on a read-only database
    without invoking either, so `await db.flush()` there never resumed (#774). The sync siblings can
    early-return — for a promise, "return" is not a no-op, it is a permanent hang with no error, no
    log line and a fully live event loop. Any new early return (guard, unsupported mode, cancelled
    work) added ahead of the `napi_create_async_work` call must settle first. The `napi_cancelled`
    branch in each `complete` callback is the same shape and is only unreachable because nothing
    calls `napi_cancel_async_work`.
16. **`FlushOptions::allow_write_stall` defaults to the waiting behavior, and the name reads
    backwards**: false (the RocksDB default, and what `flush()`/`flushSync()` still use unless a
    caller opts out) means the flush _waits_ until it can run without causing a write stall. The
    wait is unbounded and is taken on the calling thread — a libuv worker for the async `flush()` —
    so a database in a stall condition (immutable-memtable backlog, L0 stop trigger,
    pending-compaction-bytes limit, an exhausted WriteBufferManager budget, see invariant 10) yields
    a promise that never settles while the event loop stays alive — and parks the whole libuv
    worker, not just that promise: the threadpool defaults to 4 threads, so a handful of
    concurrently stalled flushes exhausts it and stalls every unrelated `fs`/`dns`/`crypto` call and
    cold-cache `get()` in the process. Do not confuse it with the
    `writeBufferManagerAllowStall` config: that decides whether the WriteBufferManager may stall
    writers at all, this decides whether one manual flush is willing to cause a stall rather than
    wait one out. The general trap is the same as invariant 10's — a default that encodes "wait for
    a good moment" is a hang whenever the good moment never arrives. `DBDescriptor::close()` is the
    obvious candidate to opt in — it has stopped accepting work, so a stall costs it nothing — and
    it is **not** opted in, on purpose: opting in makes the flush switch memtables immediately
    rather than waiting, which fires `OnFlushBegin`/`OnFlushCompleted` into transaction log stores
    that a concurrent `purgeLogs({destroy:true})` may be destroying, and that crashed a vitest
    worker on Bun/Windows (`transaction-log.test.ts`, "should write to same log from multiple
    workers"). So close can still wedge on a stall; fixing that has to happen without flushing into
    the teardown race. Still
    uncovered: `flushBeforeBackup` (`src/binding/database/backup.cpp`) flushes inside RocksDB's
    `BackupEngine`, which builds its own default `FlushOptions` we cannot reach — and that wait is
    taken _after_ the exclusive `.backup.lock` is acquired, so a stalled database turns a backup
    into an indefinite hang that also blocks every other backup/delete/purge on that directory
    until the process dies. Opting a flush in is also database-wide: it covers every column family
    on a process-global descriptor shared across `worker_threads`, so the stall reaches every
    handle on that path, not just the caller's. It is not a rescue for a flush already in flight —
    there is no cancellation — and forcing the memtable switch can itself prolong an L0 stop-trigger
    condition rather than clear it, so opt in up front rather than reaching for it mid-hang. It
    relocates the hang rather than removing it, too:
    a stalled `db->Write()` blocks whichever thread calls it, and for a committing transaction that
    is the descriptor's single `CommitWorker` thread (see "Commit execution" above), which dispatches
    every `Transaction.commit()` in order — so opting a flush into a stall queues up every commit
    behind it, including ones from callers that never touched flush.
17. **File placement is recorded two different ways, and only one survives a config change**: `paths`
    (RocksDB `db_paths`) records a _path index_ per SST file in the MANIFEST, so entries may only be
    appended — reordering or removing one points existing files at the wrong directory. `blobs.dir`
    records nothing: a blob file's directory is re-derived from the option on every open, delete, and
    report, so changing it strands the existing blob files and makes every value >= `min_blob_size`
    unreadable. `DBDescriptor::open` enforces the blob half by comparing the request against the
    `blob_dir` persisted in the OPTIONS file (`loadPersistedCFOptions`), which is why that struct
    carries more than compression; `blobs.allowDirChange` is the acknowledgement for a completed
    offline relocation, and it is the **one blob setting that reaches past the open's target family**:
    blob files sitting in one directory move together, so every family that shared the target's
    persisted directory is repointed with it; an omitted `dir` (the whole database flattened, as a
    backup restore produces) repoints every family. Scoped to the target, untargeted families keep
    the source database's `blob_dir` after a restore; independent file-number allocation then lets
    each database's obsolete-file scan delete the other's live blobs, and a multi-table migration is
    unreachable because the second family opens warm and cannot move a live directory. Applying the
    change to every family unconditionally strands families whose files did not move — the usual
    layout is heterogeneous (`default` flat and a named table on its own volume). Flattening
    (`allowDirChange` with no `dir`) is checked rather than trusted: a family whose recorded
    directory still holds `.blob` files refuses the open, avoiding an erroneous path that targets
    the live original. Grouping is by persisted directory string, so two spellings form two groups.
    A restored copy opened without acknowledgement whose target family has no external blob directory remains a
    documented procedure: OPTIONS cannot distinguish copy from original. A missing persisted
    `blob_dir` is caught at open for every family, rather than when a flush makes the whole database
    read-only. The `paths` half is **documented but not enforced** — `db_paths`/`cf_paths`
    sit in RocksDB's "not yet supported" serialization block, so there is nothing persisted to
    compare against without inventing a rocksdb-js-owned marker file. The zero-to-one transition is
    caught before open because it is detectable from the files themselves. Removing or reordering
    an entry cannot be detected up front; `explainOpenFailure` conditionally appends guidance to a
    `Corruption` naming an `.sst` file because genuine corruption reports the same status. With no
    detectable from the files themselves: with no `db_paths`, RocksDB sanitizes it to `[{dbname,
    ...}]`, so existing SST files sit at index 0 = the database directory, and supplying `paths`
    redefines that index. `assertStoragePathsUsable` rejects it by asking whether the database
    directory's SST files are reachable under `paths[0]`, rather than comparing directory strings,
    so RocksDB does not report the MANIFEST as corrupt and send an operator to backup restore. Two
    RocksDB behaviours routinely surprise people here: flush output is hardcoded to `path_id` 0
    (`FlushJob`), and manual `CompactRange` defaults to `target_path_id` 0, so only _automatic_
    compaction distributes across paths. `blobs.dir` needs a RocksDB carrying the downstream
    `blob_dir` patch (`ROCKSDB_HAS_CF_BLOB_DIR`, in `HarperFast/rocksdb-prebuilds`); every use must
    stay compilable without it. Setting `paths` at all disables `db.backup()` and
    `db.createCheckpoint()`: `GetLiveFilesStorageInfo`, which both use, returns
    `Status::NotSupported` whenever `db_paths`/`cf_paths` is non-empty, so a tiered database has no
    in-process copy path, only a volume snapshot. `destroy()` has to receive the real layout
    (`db_paths` from the live `DB`, per-CF `blob_dir`) — a default `rocksdb::Options` means
    "everything under the database directory", which orphans exactly the files tiering moved away.
    The layout must survive the descriptor: `destroy()` accepts a closed handle, and closing the last
    one purges the registry entry, so `DBHandle::close` copies a `DBFileLayout` onto the handle and
    `Database::Destroy` passes it down. `blob_dir` can be recovered from OPTIONS; `db_paths` cannot.
    See [docs/tiered-storage.md](docs/tiered-storage.md).
18. **Every per-column-family option belongs in `buildColumnFamilyOptions`**: families listed on disk
    are opened by `DBDescriptor::open`, but a _new_ family is created by `createRocksDBColumnFamily`,
    reached from both the cold path and `DBRegistry::OpenDB`'s warm reuse. Both must pass options
    built by that one function — an option set only on the open path silently keeps its default on
    every named family, and Harper maps every table to a named family, so that is the normal path
    rather than an edge. A test for a per-CF option must therefore cover a named family, not just
    `default`.

    Reaching every family is only half of it: a per-CF option must also NOT reach families this open
    is not targeting. RocksDB requires opening all of them at once and restores none of their per-CF
    options, so whatever `buildColumnFamilyOptions` puts on the base options restamps every family
    unless the cold-open loop restores the persisted value (`loadPersistedCFOptions` →
    `restorePersistedBlobOptions`, alongside the compression fields). `BlobOptions` is a struct of
    `std::optional`s so an omitted field means "leave this family alone"; only supplied fields apply
    to `options.name` (`applyExplicitBlobOptions`). Add a per-CF option in the builder's create-time
    default, persisted struct, and explicit-apply, or whichever table opens first silently decides it
    for all tables on restart.

    A field inert in the current process is still unsafe to drop on restore. RocksDB rewrites the
    OPTIONS file on every open, so a value `restorePersistedBlobOptions` skips is erased, not merely
    unused. `prepopulate_blob_cache` was gated on an attached blob cache and therefore turned itself
    off permanently when a CLI tool or `noBlockCache` script opened the database. Restore persisted
    fields unconditionally; gate on the live resource only where the field is used.

    Every field the builder sets must also be assigned in both directions, never only when requested
    is truthy. On `DBRegistry::OpenDB`'s warm path its base is the descriptor's retained
    `cfOptions`, so a one-sided assignment leaves the first opener's value in later families and then
    persists it. `value_or(default)` with an explicit else branch prevents this leak; test persisted
    OPTIONS rather than behavior after a reopen, which otherwise rewrites the correct default.

19. **An env's pending transactions are reaped by its cleanup hook — never by `DBHandle::close()`**:
    `transactionAdd` stores a strong `shared_ptr<TransactionHandle>` in the process-global
    `DBDescriptor`, and only commit/abort call `transactionRemove` (the JS wrap finalizer drops
    the JS-side ref and, per invariant 13, `onWrapperCollected()` reaps a transaction whose
    _wrapper_ was GC'd while its env is still alive). That finalizer path does not cover the
    distinct case this invariant addresses: a worker **env that exits with a transaction still
    pending** — the wrapper was never collected, the env is dying. Such a handle — holding a live
    RocksDB transaction + snapshot, with its owning `DBHandle`'s `env` about to dangle — leaked
    into the shared descriptor, and the last env's
    `DBRegistry::Shutdown → finishClose → close()` then walked those corpses and corrupted the
    glibc heap (production signatures: `corrupted size vs. prev_size`,
    `corrupted double-linked list`, `free(): invalid pointer`; HarperFast/rocksdb-js#741 —
    reproduced 10/10 on Linux/glibc by `test/lingering-txn-shutdown.test.ts`, 10/10 clean with
    the fix). The reap is
    `DBRegistry::CloseTransactionsByEnv` → `DBDescriptor::closeTransactionsByEnv`, wired into the
    module's per-env cleanup hook (binding.cpp) so it runs on the dying env's own thread while the
    env is still valid. Both reap paths funnel through the same idempotent `close()`
    (`closed` gate) + idempotent `transactionRemove`, so a handle reachable by both the finalizer
    and this hook is closed exactly once.

    **Do not "simplify" this into `DBHandle::close()`.** A user-called `db.close()` runs with live
    microtasks: `db.transaction()` awaits its callback before committing, so a legitimate commit
    is routinely one microtask behind the close and must still reach the native layer (that
    close-then-commit overlap is exactly what `test/txn-close-commit-uaf.test.ts` exercises).
    Reaping at handle close rejects those commits with "Database not open" — and under Deno's
    scheduling it stranded the caller's commit promise entirely (CI hang on all three Deno
    platforms). At env teardown no such continuation can exist, so the hook is the only safe reap
    point. Between a user `db.close()` and env death, an open transaction's handle intentionally
    lingers (bounded, cleaned at env exit).

    Relatedly, `TransactionHandle::close()` is deliberately **napi-free** — the transaction holds
    no `napi_env`/`napi_ref` fields (the JS database is passed to `UseLog` per-call; its former
    weak `jsDatabaseRef` plus a recycled-pthread `std::thread::id` collision in close()'s
    thread-identity guard was the Linux corrupting write:
    `napi_delete_reference(dead env, dead ref)` from Shutdown). Close is therefore safe from any
    thread and any teardown phase; do not
    reintroduce napi calls into close paths. Known macOS-only artifact: under Guard Malloc the
    leaker repro still faults in Node's second-pass napi finalizer drain even with the fix; it
    never reproduces natively or on glibc, so the repro test is `skipIf(darwin)` (and, like the
    repo's other teardown repros, gated to Node).

20. **A transaction timestamp freezes when native state captures it**: `setTimestamp()` may adopt an
    origin timestamp for replication or replay only while the transaction is pending and before any
    database write or transaction-log entry is staged. The log batch snapshots the timestamp at the
    first `addLogEntry`; `committedPosition` survives coordinated-retry resets, so a batch already
    written remains frozen across retries, though reapplying the same timestamp is idempotent while
    the transaction remains pending. rocksdb-js does not define record value layouts: a producer
    that copies `getTimestamp()` into record bytes must call `setTimestamp()` first.

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
