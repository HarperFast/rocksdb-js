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

### Transaction Architecture

- Optimistic (default): Conflicts detected at commit time
- Pessimistic: Conflicts throw immediately on detection
- Both modes support async/sync APIs with automatic commit/rollback

### Iterator Design

Uses `ExtendedIterable` wrapper around native iterators for array-like methods (map, filter, etc.)
with lazy evaluation.

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
