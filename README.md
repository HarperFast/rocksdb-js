# rocksdb-js

A Node.js binding for the RocksDB library.

## Features

- Supports optimistic and pessimistic transactions
- Hybrid sync/async data retrieval
- Range queries return an iterable with array-like methods and lazy evaluation
- Custom stores provide ability to override default database interactions
- Efficient binary key and value encoding
- Designed for Node.js and Bun on Linux, macOS, and Windows

## Example

```typescript
const db = RocksDatabase.open('/path/to/db');

for (const key of ['a', 'b', 'c', 'd', 'e']) {
  await db.put(key, `value ${key}`);
}

console.log(await db.get('b')); // `value b`

for (const { key, value } of db.getRange({ start: 'b', end: 'd' })) {
  console.log(`${key} = ${value}`);
}

await db.transaction(async (txn: Transaction) => {
  await txn.put('f', 'value f');
  await txn.remove('c');
});
```

## Usage

### `new RocksDatabase(path, options?)`

Creates a new database instance.

- `path: string` The path to write the database files to. This path does not
  need to exist, but the parent directories do.
- `options: object` [optional]
  - `disableWAL: boolean` Whether to disable the RocksDB write ahead log.
  - `name: string` The column family name. Defaults to `"default"`.
  - `noBlockCache: boolean` When `true`, disables the block cache. Block
     caching is enabled by default and the cache is shared across all database
     instances.
  - `parallelismThreads: number` The number of background threads to use for
    flush and compaction. Defaults to `1`.
  - `pessimistic: boolean` When `true`, throws conflict errors when they occur
    instead of waiting until commit. Defaults to `false`.
  - `store: Store` A custom store that handles all interaction between the
    `RocksDatabase` or `Transaction` instances and the native database
    interface. See [Custom Store](#custom-store) for more information.
  - `transactionLogRetention: string | number` The number of minutes to retain
    transaction logs before purging. Defaults to `'3d'` (3 days).

### `db.close()`

Closes a database. This function can be called multiple times and will only
close an opened database. A database instance can be reopened once its closed.

```typescript
const db = RocksDatabase.open('foo');
db.close();
```

### `db.config(options)`

Sets global database settings.

- `options: object`
  - `blockCacheSize: number` The amount of memory in bytes to use to cache
    uncompressed blocks. Defaults to 32MB. Set to `0` (zero) disables block
    cache for future opened databases. Existing block cache for any opened
    databases is resized immediately. Negative values throw an error.

```typescript
RocksDatabase.config({
  blockCacheSize: 100 * 1024 * 1024 // 100MB
})
```

### `db.open(): RocksDatabase`

Opens the database at the given path. This must be called before performing
any data operations.

```typescript
import { RocksDatabase } from '@harperdb/rocksdb-js';

const db = new RocksDatabase('path/to/db');
db.open();
```

There's also a static `open()` method for convenience that performs the same
thing:

```typescript
const db = RocksDatabase.open('path/to/db');
```

## Data Operations

### `db.clear(options?): Promise<number>`

Asychronously removes all data in the current database.

- `options: object`
  - `batchSize?: number` The number of records to remove at once. Defaults to `10000`.

Returns the number of entries that were removed.

Note: This does not remove data from other column families within the same
database path.

```typescript
for (let i = 0; i < 10; i++) {
  db.putSync(`key${i}`, `value${i}`);
}
const entriesRemoved = await db.clear();
console.log(entriesRemoved); // 10
```

### `db.clearSync(options?): number`

Synchronous version of `db.clear()`.

- `options: object`
  - `batchSize?: number` The number of records to remove at once. Defaults to `10000`.

```typescript
for (let i = 0; i < 10; i++) {
  db.putSync(`key${i}`, `value${i}`);
}
const entriesRemoved = db.clearSync();
console.log(entriesRemoved); // 10
```

### `db.get(key: Key, options?: GetOptions): MaybePromise<any>`

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

### `db.getSync(key: Key, options?: GetOptions): any`

Synchronous version of `get()`.

### `db.getKeys(options?: IteratorOptions): ExtendedIterable`

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

### `db.getOldestSnapshotTimestamp(): number`

Returns a number representing a unix timestamp of the oldest unreleased
snapshot.

Snapshots are only created during transactions. When the database is opened in
optimistic mode (the default), the snapshot will be created on the first
read. When the database is opened in pessimistic mode, the snapshot will be
created on the first read or write.

```typescript
console.log(db.getOldestSnapshotTimestamp()); // returns `0`, no snapshots

const promise = db.transaction(async (txn) => {
  // perform a write to create a snapshot
  await txn.get('foo');
  await setTimeout(100);
});

console.log(db.getOldestSnapshotTimestamp()); // returns `1752102248558`

await promise;
// transaction completes, snapshot released

console.log(db.getOldestSnapshotTimestamp()); // returns `0`, no snapshots
```

### `db.getRange(options?: IteratorOptions): ExtendedIterable`

Retrieves a range of keys and their values. Supports both synchronous and
asynchronous iteration.

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

### `db.getUserSharedBuffer(key: Key, defaultBuffer: ArrayBuffer, options?)`

Creates a new buffer with the contents of `defaultBuffer` that can be accessed
across threads. This is useful for storing data such as flags, counters, or
any ArrayBuffer-based data.

- `options?: object`
  - `callback?: () => void` A optional callback is called when `notify()`
	  on the returned buffer is called.

Returns a new `ArrayBuffer` with two additional methods:

- `notify()` - Invokes the `options.callback`, if specified.
- `cancel()` - Removes the callback; future `notify()` calls do nothing

Note: If a shared buffer already exists for the given `key`, the returned
`ArrayBuffer` will reference this existing shared buffer. Once all
`ArrayBuffer` instances have gone out of scope and garbage collected, the
underlying memory and notify callback will be freed.

```typescript
const buffer = new Uint8Array(
  db.getUserSharedBuffer('isDone', new ArrayBuffer(1))
);
done[0] = 0;

if (done[0] !== 1) {
  done[1] = 1;
}
```

```typescript
const incrementer = new BigInt64Array(
  db.getUserSharedBuffer('next-id', new BigInt64Array(1).buffer)
);
incrementer[0] = 1n;

function getNextId() {
  return Atomics.add(incrementer, 0, 1n);
}
```

### `db.put(key: Key, value: any, options?: PutOptions): Promise`

Stores a value for a given key.

```typescript
await db.put('foo', 'bar');
```

### `db.putSync(key: Key, value: any, options?: PutOptions): void`

Synchronous version of `put()`.

### `db.remove(key: Key): Promise`

Removes the value for a given key.

```typescript
await db.remove('foo');
```

### `db.removeSync(key: Key): void`

Synchronous version of `remove()`.

## Transactions

### `db.transaction(async (txn: Transaction) => void | Promise<any>): Promise<any>`

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

Inside a synchronous transaction, use `getSync()`, `putSync()`, and
`removeSync()`.

```typescript
import type { Transaction } from '@harperdb/rocksdb-js';
db.transactionSync((txn: Transaction) => {
	txn.putSync('foo', 'baz');
});
```

## Events

### Event: `'aftercommit'`

The `'aftercommit'` event is emitted after a transaction has been committed and
the transaction has completed including waiting for the async worker thread to
finish.

 - `result: object`
   - `next: null`
   - `last: null`
   - `txnId: number` The id of the transaction that was just committed.

### Event: `'beforecommit'`

The `'beforecommit'` event is emitted before a transaction is about to be
committed.

### Event: `'begin-transaction'`

The `'begin-transaction'` event is emitted right before the transaction function
is executed.

### Event: `'committed'`

The `'committed'` event is emitted after the transaction has been written. When
this event is emitted, the transaction is still cleaning up. If you need to know
when the transaction is fully complete, use the `'aftercommit'` event.

## Event API

`rocksdb-js` provides a EventEmitter-like API that lets you asynchronously
notify events to one or more synchronous listener callbacks. Events are scoped
by database path.

Unlike `EventEmitter`, events are emitted asynchronously, but in the same order
that the listeners were added.

```typescript
const callback = (name) => console.log(`Hi from ${name}`);
db.addListener('foo', callback);
db.notify('foo');
db.notify('foo', 'bar');
db.removeListener('foo', callback);
```

### `addListener(event: string, callback: () => void): void`

Adds a listener callback for the specific key.

```typescript
db.addListener('foo', () => {
  // this callback will be executed asynchronously
});

db.addListener(1234, (...args) => {
  console.log(args);
});
```

### `listeners(event: string): number`

Gets the number of listeners for the given key.

```typescript
db.listeners('foo'); // 0
db.addListener('foo', () => {});
db.listeners('foo'); // 1
```

### `on(event: string, callback: () => void): void`

Alias for `addListener()`.

### `once(event: string, callback: () => void): void`

Adds a one-time listener, then automatically removes it.

```typescript
db.once('foo', () => {
  console.log('This will only ever be called once');
});
```

### `removeListener(event: string, callback: () => void): boolean`

Removes an event listener. You must specify the exact same callback that was
used in `addListener()`.

```typescript
const callback = () => {};
db.addListener('foo', callback);

db.removeListener('foo', callback); // return `true`
db.removeListener('foo', callback); // return `false`, callback not found
```

### `off(event: string, callback: () => void): boolean`

Alias for `removeListener()`.

### `notify(event: string, ...args?): boolean`

Call all listeners for the given key. Returns `true` if any callbacks were
found, otherwise `false`.

Unlike `EventEmitter`, events are emitted asynchronously, but in the same order
that the listeners were added.

You can optionally emit one or more arguments. Note that the arguments must be
serializable. In other words, `undefined`, `null`, strings, booleans, numbers,
arrays, and objects are supported.

```typescript
db.notify('foo');
db.notify(1234);
db.notify({ key: 'bar' }, { value: 'baz' });
```

## Exclusive Locking

`rocksdb-js` includes a handful of functions for executing thread-safe mutually
exclusive functions.

### `db.hasLock(key: Key): boolean`

Returns `true` if the database has a lock for the given key, otherwise `false`.

```typescript
db.hasLock('foo'); // false
db.tryLock('foo'); // true
db.hasLock('foo'); // true
```

### `db.tryLock(key: Key, onUnlocked?: () => void): boolean`

Attempts to acquire a lock for a given key. If the lock is available, the
function returns `true` and the optional `onUnlocked` callback is never called.
If the lock is not available, the function returns `false` and the `onUnlocked`
callback is queued until the lock is released.

When a database is closed, all locks associated to it will be unlocked.

```typescript
db.tryLock('foo', () => {
  console.log('never fired');
}); // true, callback ignored

db.tryLock('foo', () => {
  console.log('hello world');
}); // false, already locked, callback queued

db.unlock('foo'); // fires second lock callback
```

The `onUnlocked` callback function can be used to signal to retry acquiring the
lock:

```typescript
function doSomethingExclusively() {
  // if lock is unavailable, queue up callback to recursively retry
  if (db.tryLock('foo', () => doSomethingExclusively())) {

    // lock acquired, do something exclusive

    db.unlock('foo');
  }
}
```

### `db.unlock(key): boolean`

Releases the lock on the given key and calls any queued `onUnlocked` callback
handlers. Returns `true` if the lock was released or `false` if the lock did
not exist.

```typescript
db.tryLock('foo');
db.unlock('foo'); // true
db.unlock('foo'); // false, already unlocked
```

### `db.withLock(key: Key, callback: () => void | Promise<void>): Promise<void>`

Runs a function with guaranteed exclusive access across all threads.

```typescript
await db.withLock('key', async () => {
  // do something exclusive
  console.log(db.hasLock('key')); // true
});
```

If there are more than one simultaneous lock requests, it will block them until
the lock is available.

```typescript
await Promise.all([
  db.withLock('key', () => {
    console.log('first lock blocking for 100ms');
    return new Promise(resolve => setTimeout(resolve, 100));
  }),
  db.withLock('key', () => {
    console.log('second lock blocking for 100ms');
    return new Promise(resolve => setTimeout(resolve, 100));
  }),
  db.withLock('key', () => {
    console.log('third lock acquired');
  })
]);
```

Note: If the `callback` throws an error, Node.js suppress the error. Node.js
18.3.0 introduced a `--force-node-api-uncaught-exceptions-policy` flag which
will cause errors to emit the `'uncaughtException'` event. Future Node.js
releases will enable this flag by default.

## Custom Store

The store is a class that sits between the `RocksDatabase` or `Transaction`
instance and the native RocksDB interface. It owns the native RocksDB instance
along with various settings including encoding and the db name. It handles all
interactions with the native RocksDB instance.

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
- `getUserSharedBuffer(key, defaultBuffer?)`
- `hasLock(key)`
- `isOpen()`
- `open()`
- `putSync(context, key, value, options?)`
- `removeSync(context, key, options?)`
- `tryLock(key, onUnlocked?)`
- `unlock(key)`
- `withLock(key, callback?)`

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
const db = RocksDatabase.open(myStore);
await db.put('foo', 'bar');
console.log(await db.get('foo'));
```

> [!IMPORTANT]
> If your custom store overrides `putSync()` without calling `super.putSync()`
> and it performs its own `this.encodeKey(key)`, then you MUST encode the VALUE
> before you encode the KEY.
>
> Keys are encoded into a shared buffer. If the database is opened with the
> `sharedStructuresKey` option, encoding the value will load and save the
> structures which encodes the `sharedStructuresKey` overwriting the encoded
> key in the shared key buffer, so it's ultra important that you encode the
> value first!

## Interfaces

### `RocksDBOptions`

- `options: object`
  - `adaptiveReadahead: boolean` When `true`, RocksDB will do some enhancements
    for prefetching the data. Defaults to `true`. Note that RocksDB defaults
    this to `false`.
  - `asyncIO: boolean` When `true`, RocksDB will prefetch some data async and
    apply it if reads are sequential and its internal automatic prefetching.
    Defaults to `true`. Note that RocksDB defaults this to `false`.
  - `autoReadaheadSize: boolean` When `true`, RocksDB will auto-tune the
    readahead size during scans internally based on the block cache data when
    block caching is enabled, an end key (e.g. upper bound) is set, and prefix
    is the same as the start key. Defaults to `true`.
  - `backgroundPurgeOnIteratorCleanup: boolean` When `true`, after the iterator
    is closed, a background job is scheduled to flush the job queue and delete
    obsolete files. Defaults to `true`. Note that RocksDB defaults this to
    `false`.
  - `fillCache: boolean` When `true`, the iterator will fill the block cache.
    Filling the block cache is not desirable for bulk scans and could impact
    eviction order. Defaults to `false`. Note that RocksDB defaults this to
    `true`.
  - `readaheadSize: number` The RocksDB readahead size. RocksDB does
    auto-readahead for iterators when there is more than two reads for a table
    file. The readahead starts at 8KB and doubles on every additional read up
    to 256KB. This option can help if most of the range scans are large and if a
    larger readahead than that enabled by auto-readahead is needed. Using a
    large readahead size (> 2MB) can typically improve the performance of
    forward iteration on spinning disks. Defaults to `0`.
  - `tailing: boolean` When `true`, creates a "tailing iterator" which is a
    special iterator that has a view of the complete database including newly
    added data and is optimized for sequential reads. This will return records
    that were inserted into the database after the creation of the iterator.
    Defaults to `false`.

### `RangeOptions`

Extends `RocksDBOptions`.

- `options: object`
  - `end: Key | Uint8Array` The range end key, otherwise known as the "upper
    bound". Defaults to the last key in the database.
  - `exclusiveStart: boolean` When `true`, the iterator will exclude the first
    key if it matches the start key. Defaults to `false`.
  - `inclusiveEnd: boolean` When `true`, the iterator will include the last key
    if it matches the end key. Defaults to `false`.
  - `start: Key | Uint8Array` The range start key, otherwise known as the "lower
    bound". Defaults to the first key in the database.

### `IteratorOptions`

Extends `RangeOptions`.

- `options: object`
  - `reverse: boolean` When `true`, the iterator will iterate in reverse order.
    Defaults to `false`.

## Development

This package requires Node.js 18 or higher, pnpm, and a C++ compiler.

> [!TIP]
> Enable pnpm log streaming to see full build output:
> ```
> pnpm config set stream true
> ```

### Building

There are two things being built: the native binding and the TypeScript code.
Each of those can be built to be debug friendly.

| Description | Command |
| --- | --- |
| Production build (minified + native binding) | `pnpm build` |
| TypeScript only (minified) | `pnpm build:bundle` |
| TypeScript only (unminified) | `pnpm build:debug` |
| Native binding only (prod) | `pnpm rebuild` |
| Native binding only (with debug logging) | `pnpm rebuild:debug` |
| Debug build everything | `pnpm build:debug && pnpm rebuild:debug` |

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

To run the tests without code coverage, run:

```bash
pnpm test
```

To run a specific test suite, for example `"ranges"`, run:

```bash
pnpm test ranges
# or
pnpm test test/ranges
```

To run a specific unit test, for example all tests that mention
`"column family"`, run:

```bash
pnpm test -t "column family"
```

Vitest's terminal renderer will often overwrite the debug log output, so it's
highly recommended to specify the `CI=1` environment variable to prevent Vitest
from erasing log output:

```bash
CI=1 pnpm test
```
