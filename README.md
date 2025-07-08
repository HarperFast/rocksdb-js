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
  - `name:string` The column family name. Defaults to `"default"`.
  - `noBlockCache: boolean` When `true`, disables the block cache. Block caching is enabled by default and the cache is shared across all database instances.
  - `parallelismThreads: number` The number of background threads to use for flush and compaction. Defaults to `1`.
  - `pessimistic: boolean` When `true`, throws conflict errors when they occur instead of waiting until commit. Defaults to `false`.
  - `store: Store` A custom store that handles all interaction between the `RocksDatabase` or `Transaction` instances and the native database interface. See [Custom Store](#custom-store) for more information.

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

### `db.getEntry(key): MaybePromise`

Retrieves a value for the given key as an "entry" object.

```typescript
const { value } = await db.getEntry('foo');
```

### `db.getKeys(options?: IteratorOptions): RangeIterable`

Retrieves all keys within a range.

```typescript
for (const { key, value } of db.getKeys()) {
  console.log({ key, value });
}
```

### `db.getKeysCount(options?: RangeOptions): number`

Retrieves the number of keys within a range.

```typescript
const total = db.getKeysCount();
const range = db.getKeysCount({ start: 'a', end: 'z' });
```

### `db.getRange(options?: IteratorOptions): RangeIterable`

Retrieves a range of keys and their values. Supports both synchronous and asynchronous iteration.

```typescript
// sync
for (const { key, value } of db.getRange()) {
  console.log({ key, value });
}

// async
for await (const { key, value } of db.getRange()) {
  console.log({ key, value });
}

// key range
for (const { key, value } of db.getRange({ start: 'a', end: 'z' })) {
  console.log({ key, value });
}
```

### `db.getValues(key: Key, options?: IteratorOptions): RangeIterable`

Retrieves all values for the given key.

```typescript
for (const { value } of db.getValues('a')) {
  console.log({ value });
}
```

### `db.getValuesCount(key: Key, options?: RangeOptions): number`

Retrieves the number of values for the given key.

```typescript
const count = db.getValuesCount('a');
```

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

### `class RangeIterable`

An iterable that provides a rich set of methods for working with ranges of items.

#### `.asArray: any[] | Promise<any[]>`

Collects the iterator results in an array and returns it.

```typescript
const array = db.getRange().asArray;
```

If the iterable is asynchronous, then it will return a promise.

```typescript
const array = await db.getRange().asArray;
```

#### `.at(index: number): any`

Returns the item at the given index.

```typescript
const item = db.getRange().at(0);
```

#### `.concat(iterable: Iterable): RangeIterable`

Concatenates the iterable with another iterable.

```typescript
const concatenated = db.getRange().concat(db.getRange());
```

#### `.drop(limit: number): RangeIterable`

Returns a new iterable with the first `limit` items removed.

```typescript
for (const { key, value } of db.getRange().drop(10)) {
  console.log({ key, value });
}
```

#### `.every(callback: (value, index) => boolean): boolean`

Returns `true` if the callback returns `true` for every item of the iterable.

```typescript
const isAllValid = db.getRange().every(item => item.value.length > 0);
```

#### `.filter(callback: (value, index) => boolean): RangeIterable`

Returns a new iterable containing only the values for which the callback returns `true`.

```typescript
const filtered = db.getRange().filter(item => item.value.length > 0);
```

#### `.find(callback: (value, index) => boolean): any`

Returns the first item of the iterable for which the callback returns `true`.

```typescript
const found = db.getRange().find(item => item.value.length > 0);
```

#### `.flatMap(callback: (value, index) => any): RangeIterable`

Returns a new iterable with the results of a callback function, then flattens the results.

```typescript
const flattened = db.getRange().flatMap(item => [item, item]);
```

#### `.forEach(callback: (value, index) => void): void`

Calls a function for each item of the iterable.

```typescript
db.getRange().forEach(item => console.log(item));
```

#### `.map(callback: (value, index) => any): RangeIterable`

Returns a new iterable with the results of calling a callback function.

```typescript
const mapped = db.getRange().map(item => item.value.length);
```

#### `.mapError(catchCallback: (error) => Error): RangeIterable`

Catch errors thrown during iteration and allow iteration to continue.

```typescript
const mapped = db.getRange().mapError(error => new Error('Error: ' + error.message));
```

#### `.reduce(callback: (prev, current, index) => any): any`

Reduces the iterable to a single value.

```typescript
const sum = db.getRange().reduce((acc, item) => acc + item.value.length, 0);
```

#### `.slice(start: number, end?: number): RangeIterable`

Returns a new iterable with the items between the start and end indices.

```typescript
const sliced = db.getRange().slice(10, 20);
```

#### `.some(callback: (value, index) => boolean): boolean`

Returns `true` if the callback returns `true` for any item of the iterable.

```typescript
const hasEven = db.getRange().some(item => item.value.length % 2 === 0);
```

#### `.take(limit: number): RangeIterable`

Returns a new iterable with the first `limit` items.

```typescript
for (const { key, value } of db.getRange().take(10)) {
  console.log({ key, value });
}
```

## Custom Store

The store is a class that sits between the `RocksDatabase` or `Transaction`
instance and the native RocksDB interface. It owns the native RocksDB instance
along with various settings including encoding and the db name. It handles all interactions with the native RocksDB instance.

The default `Store` contains the following methods which can be overridden:

- `constructor(path, options?)`
- `close()`
- `decodeKey(key)`
- `decodeValue(value)`
- `encodeKey(key)`
- `encodeValue(value)`
- `get(context, key, resolve, reject, txnId?)`
- `getCount(context, options?, txnId?)`
- `getRange(context, options?)`
- `getSync(context, key, options?)`
- `getValuesCount(context, key, options?)`
- `isOpen()`
- `open()`
- `putSync(context, key, value, options?)`
- `removeSync(context, key, options?)`

To use it, extend the default `Store` and pass in an instance of your store
into the `RocksDatabase` constructor.

```typescript
import { RocksDatabase, Store } from '@harperdb/rocksdb-js';

class MyStore extends Store {
  get(context, key, resolve, reject, txnId) {
    console.log('Getting:' key);
    return super.get(context, key, resolve, reject, txnId);
  }

  putSync(context, key, value, options) {
    console.log('Putting:', key);
    return super.putSync(context, key, value, options);
  }
}

const myStore = new MyStore('path/to/db');
const db = await RocksDatabase.open(myStore);
await db.put('foo', 'bar');
console.log(await db.get('foo'));
```

## Interfaces

### `RocksDBOptions`

- `options: object`
  - `adaptiveReadahead: boolean` When `true`, RocksDB will do some enhancements for prefetching the data. Defaults to `true`. Note that RocksDB defaults this to `false`.
  - `asyncIO: boolean` When `true`, RocksDB will prefetch some data async and apply it if reads are sequential and its internal automatic prefetching. Defaults to `true`. Note that RocksDB defaults this to `false`.
  - `autoReadaheadSize: boolean` When `true`, RocksDB will auto-tune the readahead size during scans internally based on the block cache data when block caching is enabled, an end key (e.g. upper bound) is set, and prefix is the same as the start key. Defaults to `true`.
  - `backgroundPurgeOnIteratorCleanup: boolean` When `true`, after the iterator is closed, a background job is scheduled to flush the job queue and delete obsolete files. Defaults to `true`. Note that RocksDB defaults this to `false`.
  - `fillCache: boolean` When `true`, the iterator will fill the block cache. Filling the block cache is not desirable for bulk scans and could impact eviction order. Defaults to `false`. Note that RocksDB defaults this to `true`.
  - `readaheadSize: number` The RocksDB readahead size. RocksDB does auto-readahead for iterators when there is more than two reads for a table file. The readahead starts at 8KB and doubles on every additional read up to 256KB. This option can help if most of the range scans are large and if a larger readahead than that enabled by auto-readahead is needed. Using a large readahead size (> 2MB) can typically improve the performance of forward iteration on spinning disks. Defaults to `0`.
  - `tailing: boolean` When `true`, creates a "tailing iterator" which is a special iterator that has a view of the complete database including newly added data and
  is optimized for sequential reads. This will return records that were inserted into the database after the creation of the iterator. Defaults to `false`.

### `RangeOptions`

Extends `RocksDBOptions`.

- `options: object`
  - `end: Key | Uint8Array` The range end key, otherwise known as the "upper bound". Defaults to the last key in the database.
  - `exclusiveStart: boolean` When `true`, the iterator will exclude the first key if it matches the start key. Defaults to `false`.
  - `inclusiveEnd: boolean` When `true`, the iterator will include the last key if it matches the end key. Defaults to `false`.
  - `start: Key | Uint8Array` The range start key, otherwise known as the "lower bound". Defaults to the first key in the database.

### `IteratorOptions`

Extends `RangeOptions`.

- `options: object`
  - `reverse: boolean` When `true`, the iterator will iterate in reverse order. Defaults to `false`.

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

To configure and compile only the native binding, run:

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

