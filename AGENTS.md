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
10. **`TransactionHandle::stateMutex` serializes `txn`/VT-lock state against cross-env `close()`**: a
    `TransactionHandle` is normally single-owner (bound to the JS thread that created it), but
    `DBDescriptor::finishClose()` (worker-env teardown, e.g. via `DBRegistry::Shutdown()`) closes
    **every** transaction registered on the shared descriptor — including ones owned by a different,
    still-live env that may be mid-commit, mid-put, or mid-abort at that exact moment
    (HarperFast/rocksdb-js#741). Before the fix, `close()`'s `waitForAsyncWorkCompletion()` only
    waited for async work _already registered_ on `activeAsyncWorkCount` — a TOCTOU let a racing
    `Transaction::Commit()`/`CommitSync`/`Abort` register (or start touching `txn`) _after_ `close()`
    had already observed zero in-flight work and moved on to `delete this->txn` and
    `releaseIntent()`'s vector mutation, corrupting the heap (`txn` UAF/double-free) and/or
    double-releasing a shared `LockTracker` (`releaseWriteIntent` reaching "last holder" twice for the
    same tracker — `holders`/`refcount` underflow, now asserted in
    `VerificationTable::releaseWriteIntent`/`unrefTracker`). Every path that touches `txn` or
    `lockedVTSlots`/`heldTrackers` — `lockVTSlot()`, `releaseIntent()`, `resetTransaction()`, `close()`,
    and `Commit()`/`CommitSync`/`Abort()`/`get()`'s entry via `tryRegisterAsyncWork()` — now takes
    `stateMutex` for a short critical section (never across `txn->Commit()` or
    `waitForAsyncWorkCompletion()`, which would deadlock `close()` against the very work it is
    waiting to drain). `close()` flips `closed` under `stateMutex` _before_ calling
    `waitForAsyncWorkCompletion()`, so by the time it waits, no new work can have registered — the
    wait only has to drain work that already committed to running. Known gaps deliberately left open
    (each needs its own dedicated investigation, not a bolt-on to this fix): **(a)** `getSync`/
    `putSync`/`removeSync`/`CommitSync`/`getCount` still read/write `txn` without going through
    `tryRegisterAsyncWork()`. `getSync`/`putSync`/`removeSync` are the hottest paths in the binding
    (every read and write) with a narrow, synchronous exposure window — the mutex-per-call cost
    wasn't accepted without a dedicated hot-path review. `CommitSync` and `getCount` are not
    hot-path-sensitive the same way, but an attempt to add `tryRegisterAsyncWork()` +
    `AsyncWorkGuard`/an inline RAII guard to `CommitSync` reproduced a heap-corruption regression
    under worker-churn testing (bisected: removing that registration made the same test suite pass
    5/5 across two Node majors; adding it back reproduced glibc heap-corruption aborts in ~50% of
    runs) that was not root-caused within the time available — the exact mechanism is still unknown,
    so `getCount` (identical new-code shape, untested) was reverted alongside it out of caution
    rather than ship an unverified instance of the same pattern. **(b)** `waitForAsyncWorkCompletion()`'s
    5-second timeout is pre-existing and unchanged: on timeout `close()` proceeds anyway, so a commit
    that is genuinely still executing past 5s (not parked — parking happens after
    `signalExecuteCompleted()`, so it doesn't interact with this wait) can still race a
    `delete this->txn`. Removing the timeout trades this for a shutdown-hang risk if a commit truly
    never completes.
11. **A coordinated-retry parked wake-callback's TSFN can outlive the env that created it**: an
    `IsBusy` commit under `coordinatedRetry` parks its `RETRY_NOW` resolution on the _winning_
    holder's `LockTracker` (`completeCommitWork`'s park loop in transaction.cpp) by registering a
    wake callback that captures a `napi_threadsafe_function` by value. `LockTracker` is
    process-global VT state (not owned by any env), so that callback can fire on **any** thread —
    whichever thread eventually releases the winning holder — an arbitrary time later, including
    after the _parking_ transaction's own env has already torn down. Node reclaims a tsfn as part of
    destroying the env that created it, so calling `napi_call_threadsafe_function` /
    `napi_release_threadsafe_function` on it past that point is a use-after-free — this was the
    actual mechanism behind the `uv_mutex_lock` abort inside `napi_release_threadsafe_function` in
    HarperFast/rocksdb-js#741's crash trace (confirmed via a gdb backtrace off the real repro; item 10
    above closes a _different_, also-real race in the same issue, not this one). Fixed the same way as
    `commitCompletions`: `ParkedFlagRegistry` (transaction.cpp) tracks a `ParkedFlag` per registered
    wake callback, keyed by the owning env; `Transaction::ReleaseParkedFlagsByEnv`, wired into the
    module's per-env cleanup hook, flips each of that env's flags before Node frees its tsfns, and the
    wake callback checks the flag under the _same_ per-flag mutex before touching the tsfn — so
    whichever of "env teardown" or "wake fires" happens first wins, and the other is a no-op instead of
    a race.
12. **CRITICAL, UNRESOLVED as of the #741 fix: closing items 10+11's races introduces or exposes a
    separate, much more frequent heap-corruption crash, root cause NOT found despite extensive
    bisection.** Measured on `HarperFast/rocksdb-js`'s own `repro-crossthread.mjs` (a `coordinatedRetry`
    - materialized-VT + worker-churn stress script that pre-dates this fix), same settings
      (`GRACEFUL=1`, 4 workers, one worker recycled every 2s): the commit immediately before items 10/11's
      fix (`ea83ff46`) is consistently clean (15/15, plus 20/20 more with an artificial delay injected
      into `close()` — see below); every tested version of the fix crashes 14/15 to 93% of runs with
      glibc heap corruption (`corrupted size vs. prev_size`, `free(): invalid size`, `corrupted
double-linked list`, `double free or corruption (!prev)` — multiple distinct messages, i.e.
      multiple corruption _detection_ sites, not necessarily multiple root causes). gdb backtraces land
      inside `node::CleanupQueue::Drain()` → `DBRegistry::Shutdown()` → `DBDescriptor::finishClose()`'s
      pre-existing `closables` teardown loop (`db_descriptor.cpp` ~line 304, unmodified by this fix),
      sometimes one level deeper inside `~OptimisticTransaction`/`~TransactionBaseImpl`/
      `~WriteBatchWithIndex`/`~Arena`.

    Hypotheses tested and **ruled out**:
    - `writerMutex_` held across the N-API calls item 11's wake callbacks make (in
      `VerificationTable::releaseWriteIntent`/`cancelForDB`) — moving `wake()` outside `writerMutex_`
      in both did not reduce the crash rate (7/20 vs. 3/14 without the change; both far above
      baseline's 0/18).
    - Un-closed/leaked `TransactionHandle`s accumulating over a run's lifetime and inflating
      `finishClose()`'s `closables` backlog. This accumulation is **real** and worth its own note (see
      below) — debug-log correlation (`pnpm rebuild:debug`, `DEBUG_LOG`) showed thousands of distinct,
      never-before-closed `TransactionHandle` addresses being closed for the first time inside one
      `finishClose()` call, immediately before a crash, because `completeCommitWork`'s normal
      error-rejection path (`transaction.cpp`, the final `else` branch, non-`IsBusy`) never calls
      `close()` — only a caller-side `abort()` does, and `repro-crossthread.mjs`'s racer role doesn't
      call it on a non-`RETRY_NOW` rejection. But patching the repro to `abort()` on every rejection
      did **not** reduce the crash rate (17/20, same order as unpatched) — so this leak is a real,
      separate finding, not (by itself) the corruption's cause.
    - Raw `TransactionHandle::close()` latency, tested directly and decisively: baseline (`ea83ff46`,
      zero code changes from this fix) with `ROCKSDB_JS_TXN_CLOSE_DELAY_MS` set to 10ms and then 100ms
      (an artificial sleep inside `close()`, via the pre-existing test seam used by
      `txn-close-commit-uaf.test.ts`) stayed **20/20 clean** at both settings. This rules out "the fix
      just makes `close()` slower, which widens some pre-existing timing window" as an explanation —
      whatever the fix changes, it is not reducible to `close()` taking longer.

    Given the above, the cause is something specific to items 10/11's actual new logic (the `stateMutex`
    critical sections spread across `resetTransaction`/`lockVTSlot`/`releaseIntent`/`close`/
    `tryRegisterAsyncWork`, or the `ParkedFlagRegistry` mechanism itself), not simply added latency.
    ThreadSanitizer was tried and is **not currently practical** here: running the full repro under it
    (via a `ROCKSDB_TSAN=1` `binding.gyp` toggle added alongside the existing `ROCKSDB_ASAN` one) reports
    100-150+ races per run, but across several runs and multiple suppression passes for known-noisy V8
    subsystems (Scavenger, Maglev, baseline/concurrent compilation, concurrent marking, the sweeper,
    tracing), **every single one** resolves (confirmed via `addr2line` against the built `.node`, using
    `TSAN_OPTIONS=external_symbolizer_path=/usr/bin/addr2line`) to pure V8-internal GC/JIT machinery with
    zero `rocksdb_js::` frames on either side. This is a known limitation of running vanilla Node under
    TSan without Node's own build-time instrumentation (which Node does not ship) — V8's internal
    lock-free/relaxed-atomics patterns are invisible to TSan without it, and the noise categories are
    numerous enough (new ones appear run to run) that suppression-file whack-a-mole doesn't converge in a
    reasonable time. A real attempt would need Node built from source with `-fsanitize=thread` (a 30-60+
    minute compile), not attempted this round. **Do not merge a fix for #741 without resolving this** —
    see the #741 dispatch's Findings for the full reproduction recipe, saved backtraces, and debug logs.

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
