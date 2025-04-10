# rocksdb-js

A Node.js binding for the RocksDB library.

## Installation

```bash
npm i --save @harperdb/rocksdb-js
```

## Usage

### `new RocksDatabase(path, options?)`

Creates a new database instance.

- `path: string` The path to write the database files to. This path does not
  need to exist, but the parent directories do.
- `options: object` [optional]
  - `blockCacheSize: number` The amount of memory in bytes to use to cache uncompressed blocks. Defaults to 100MB. Set to `0` (zero) disables block cache. Negative values throw error.
  - `name:string` The column family name. Defaults to `"default"`.
  - `parallelismThreads: number` The number of background threads to use for flush and compaction. Defaults to `1`.
  - `pessimistic: boolean` When `true`, throws conflict errors when they occur instead of waiting until commit. Defaults to `false`.

### `db.open(): Promise<RocksDatabase>`

Opens the database at the given path. This must be called before performing
any data operations.

```typescript
import { RocksDatabase } from '@harperdb/rocksdb-js';

const db = new RocksDatabase('path/to/db');
await db.open();
```

There's also a static `open()` method for convenience that performs the same thing:

```typescript
const db = await RocksDatabase.open('path/to/db');
```

### `db.close()`

Closes a database. A database instance can be reopened once its closed.

### `db.get(key, options?): Promise<any>`

Retreives the value for a given key. If the key does not exist, it will return
`undefined`.

```typescript
const foo = await db.get('foo');
assert.equal(foo, 'foo');
```

### `db.put(key, value, options?): Promise`

Stores a value for a given key.

```typescript
await db.put('foo', 'bar');
```

### `db.remove(key): Promise`

Removes the value for a given key.

```typescript
await db.remove('foo');
```

### `db.transaction(async (txn: Transaction) => Promise): Promise`

Executes all database operations within the specified callback within a single
transaction. If the callback completes without error, the database operations
are automatically committed. However, if an error is thrown during the
callback, all database operations will be rolled back.

```typescript
import type { Transaction } from '@harperdb/rocksdb-js';
await db.transaction(async (txn: Transaction) => {
	await txn.put('foo', 'baz');
});
```

Additionally, you may pass the transaction into any database data method:

```typescript
await db.transaction(async (transaction: Transaction) => {
	await db.put('foo', 'baz', { transaction });
});
```

## Development

This package requires Node.js 18 or higher, pnpm, and a C++ compiler.

> [!TIP]
> Enable pnpm log streaming to see full build output:
> ```
> pnpm config set stream true
> ```

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

