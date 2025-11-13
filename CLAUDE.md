# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Building
- `pnpm build` - Full production build (TypeScript bundle + native C++ binding)
- `pnpm build:binding` - Incremental build C++ binding only (production)
- `pnpm build:binding:debug` - Incremental build C++ binding only (debug)
- `pnpm build:bundle` - TypeScript only (minified)
- `pnpm build:debug` - TypeScript only (unminified)
- `pnpm rebuild` - Configure and build C++ binding only (production)
- `pnpm rebuild:debug` - Native C++ binding only (with debug logging and coverage)

### Testing
- `pnpm test` - Run all tests with Vitest
- `pnpm coverage` - Run all tests with Vitest and coverage report
- `node --expose-gc ./node_modules/vitest/vitest.mjs test/specific.test.ts` - Run single test file

### Code Quality
- `pnpm check` - Run both type-check and lint
- `pnpm type-check` - TypeScript type checking only
- `pnpm lint` - Code linting with oxlint

### Development Workflow
- `pnpm clean` - Clean native build artifacts
- `pnpm build:debug && pnpm rebuild:debug` - Full debug build for development

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
- **`util.ts`** - Various helpers

### C++ Native Layer (`src/binding/`)
- **`binding.cpp/h`** - Main N-API module entry point
- **`database.cpp/h`** - Native database operations and async work handling
- **`db_handle.cpp/h`** - Database handle management
- **`transaction.cpp/h` && `transaction_handle.cpp/h`** - Native transaction implementations
- - **`transaction_log*.cpp/h`** - Native transaction log store and file implementations
- **`db_iterator*.cpp/h`** - Iterator implementations for range queries
- **`util.cpp/h`** - Utility functions and error handling

### Key Design Patterns

1. **Hybrid Sync/Async**: Operations return promises for disk I/O or immediate values for cached data
2. **Encoding Strategy**: Keys use ordered-binary encoding, values default to msgpack
3. **Store Pattern**: `Store` class encapsulates database instance and encoding logic, shared between `RocksDatabase` and `Transaction`
4. **Native Binding**: Uses node-gyp with C++20, links against prebuilt RocksDB libraries

### Transaction Architecture
- Optimistic (default): Conflicts detected at commit time
- Pessimistic: Conflicts throw immediately on detection
- Both modes support async/sync APIs with automatic commit/rollback

### Iterator Design
Uses `ExtendedIterable` wrapper around native iterators for array-like methods (map, filter, etc.) with lazy evaluation.

## Environment Variables

- `ROCKSDB_VERSION` - Override RocksDB version (default from package.json, or 'latest')
- `ROCKSDB_PATH` - Build from local RocksDB source instead of prebuilt
- `SKIP_MINIFY=1` - Disable minification for debug builds
- `KEEP_FILES=1` - Don't delete temporary test databases for debugging purposes

## Test Structure

Tests are in `test/` directory using Vitest:
- Individual feature tests (transactions, ranges, encoding, etc.)
- `test/lib/util.ts` contains test utilities
- Coverage reports generated to `coverage/` directory

## Important Implementation Notes

1. **Key Encoding Order**: Always encode values before keys when using `sharedStructuresKey` to avoid overwriting shared key buffer
2. **Buffer Management**: Store uses reusable buffers for performance (`keyBuffer`, `encodeBuffer`)
3. **Memory Management**: Native layer handles RocksDB memory, TypeScript layer manages encoding buffers
4. **Error Handling**: C++ errors are translated to JavaScript exceptions via N-API