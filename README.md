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
  - `noBlockCache: boolean` When `true`, disables the block cache. Block caching is enabled by default and the cache is shared across all database instances.
  - `name:string` The column family name. Defaults to `"default"`.
  - `parallelismThreads: number` The number of background threads to use for flush and compaction. Defaults to `1`.
  - `pessimistic: boolean` When `true`, throws conflict errors when they occur instead of waiting until commit. Defaults to `false`.

### `db.config(options)`

Sets global database settings.

- `options: object`
  - `blockCacheSize: number` The amount of memory in bytes to use to cache uncompressed blocks. Defaults to 32MB. Set to `0` (zero) disables block cache for future opened databases. Existing block cache for any opened databases is resized immediately. Negative values throw an error.

```typescript
RocksDatabase.config({
  blockCacheSize: 100 * 1024 * 1024 // 100MB
})
```

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

### `db.get(key, options?): MaybePromise<any>`

Retreives the value for a given key. If the key does not exist, it will resolve
`undefined`.

```typescript
const result = await db.get('foo');
assert.equal(result, 'foo');
```

If the value is in the memtable or block cache, `get()` will immediately return
the value synchronously instead of returning a promise.

```typescript
const result = db.get('foo');
const value = result instanceof Promise ? (await result) : result;
assert.equal(result, 'foo');
```

Note that all errors are returned as rejected promises.

### `db.getSync(key, options?): any`

Synchronous version of `get()`.

### `db.put(key, value, options?): Promise`

Stores a value for a given key.

```typescript
await db.put('foo', 'bar');
```

### `db.putSync(key, value, options?): void`

Synchronous version of `put()`.

### `db.remove(key): Promise`

Removes the value for a given key.

```typescript
await db.remove('foo');
```

### `db.removeSync(key): void`

Synchronous version of `remove()`.

### `db.transaction(async (txn: Transaction) => Promise<any>): Promise<any>`

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

Note that `db.transaction()` resolves whatever value the transaction callback
resolves:

```typescript
const isBar = await db.transaction(async (txn: Transaction) => {
  const foo = await txn.get('foo');
  return foo === 'bar';
});
console.log(isBar ? 'Foo is bar' : 'Foo is not bar');
```

### `db.transactionSync((txn: Transaction) => any): any`

Executes a transaction callback and commits synchronously. Once the transaction
callback returns, the commit is executed synchronously and blocks the current
thread until finished.

Inside a synchronous transaction, use `getSync()`, `putSync()`, and `removeSync()`.

```typescript
import type { Transaction } from '@harperdb/rocksdb-js';
db.transactionSync((txn: Transaction) => {
	txn.putSync('foo', 'baz');
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

