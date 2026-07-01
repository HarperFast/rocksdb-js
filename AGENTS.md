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

## Environment Variables

- `ROCKSDB_VERSION` - Override RocksDB version (default from package.json, or 'latest')
- `ROCKSDB_PATH` - Build from local RocksDB source instead of prebuilt
- `MINIFY=1` - Enable minification of TypeScript bundle
- `KEEP_FILES=1` - Don't delete temporary test databases for debugging purposes

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
   `OpenDB` keeps waiting on the entry's condition instead of re-opening the path mid-close.
7. **One writable BackupEngine per backup directory (on-disk lock)**: each backup op opens its own
   short-lived `rocksdb::BackupEngine`/`BackupEngineReadOnly` (`src/binding/database/backup.cpp`), and
   RocksDB only serializes work _within_ a single engine — it has no cross-engine lock on the directory.
   Two writers on the same directory (two `db.backup()` calls, or a `backup` racing a `delete`/`purge`),
   in the same process or different ones, collide on the per-backup staging dir and both fail,
   potentially leaving zero usable backups. `withBackupDirLock` in `src/backup.ts` (used by
   `Store.backup`, `backups.delete`, `backups.purge`) enforces a single writer via a `.backup.lock`
   pidfile at the directory root: it reads any existing lock, **throws** if the named pid is still
   running (a live lock — note the current process's own pid always reads as running), and otherwise
   reclaims a stale lock and claims the directory by writing its pid to a `temp_<rand>` file and
   `link()`-ing it into place (`link`, not `rename`: POSIX `rename` overwrites the target, so two racers
   would both "win"; `link` fails if it exists). The lock is on disk precisely so it coordinates across
   processes and `worker_threads` — an in-memory lock cannot. Contention **rejects**; it does not queue,
   so a caller issuing overlapping backups to one directory must handle the "locked" error (e.g. retry).
   Read-only ops (`list`, `verify`, a restore's source read) are not locked since concurrent readers are
   safe; a reader racing a `delete`/`purge` is a caller-managed hazard. Different directories are
   independent (separate lock files) and run fully in parallel.

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
