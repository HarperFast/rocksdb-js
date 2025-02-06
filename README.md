# rocksdb-js

A Node.js binding for the RocksDB library.

## Installation

```bash
npm i --save rocksdb-js
```

## Usage

```ts
import { RocksDatabase } from 'rocksdb-js';

const db = new RocksDatabase('path/to/db');
await db.open();
```

## API

?

## Development

This package requires Node.js 18 or higher, pnpm, and a C++ compiler.

### Building the Native Binding

To compile everything including the native binding and the TypeScript source, run:

```bash
pnpm build
```

To compile only the native binding, run:

```bash
pnpm rebuild
```

When building the native binding, it will download the appropriate prebuilt
RocksDB library for your platform and architecture from the
[rocksdb-prebuilds](https://github.com/HarperDB/rocksdb-prebuilds) GitHub
repository. It defaults to the pinned version in the `package.json` file. You
can override this by setting the `ROCKSDB_VERSION` environment variable. For
example:

```bash
ROCKSDB_VERSION=9.10.0 pnpm build
```

You may also specify `latest` to use the latest prebuilt version.

```bash
ROCKSDB_VERSION=latest pnpm build
```

Optionally, you may also create a `.env` file in the root of the project
to specify various settings. For example:

```bash
echo "ROCKSDB_VERSION=9.10.0" >> .env
```

### Building RocksDB from Source

To build RocksDB from source, simply set the `ROCKSDB_PATH` environment
variable to the path of the local `rocksdb` repo:

```bash
git clone https://github.com/facebook/rocksdb.git /path/to/rocksdb
echo "ROCKSDB_PATH=/path/to/rocksdb" >> .env
pnpm rebuild
```

### Testing

To run the tests, run:

```bash
pnpm coverage
```

