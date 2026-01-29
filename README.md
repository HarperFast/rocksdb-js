# rocksdb-js

A Node.js binding for the RocksDB library.

## Features

- Supports optimistic and pessimistic transactions
- Hybrid sync/async data retrieval
- Range queries return an iterable with array-like methods and lazy evaluation
- Transaction log system for recording transaction related data
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

- `path: string` The path to write the database files to. This path does not need to exist, but the
  parent directories do.
- `options: object` [optional]
  - `disableWAL: boolean` Whether to disable the RocksDB write ahead log.
  - `name: string` The column family name. Defaults to `"default"`.
  - `noBlockCache: boolean` When `true`, disables the block cache. Block caching is enabled by
    default and the cache is shared across all database instances.
  - `parallelismThreads: number` The number of background threads to use for flush and compaction.
    Defaults to `1`.
  - `pessimistic: boolean` When `true`, throws conflict errors when they occur instead of waiting
    until commit. Defaults to `false`.
  - `store: Store` A custom store that handles all interaction between the `RocksDatabase` or
    `Transaction` instances and the native database interface. See [Custom Store](#custom-store) for
    more information.
  - `transactionLogMaxAgeThreshold: number` The threshold for the transaction log file's last
    modified time to be older than the retention period before it is rotated to the next sequence
    number. Value must be between `0.0` and `1.0`. A threshold of `0.0` means ignore age check.
    Defaults to `0.75`.
  - `transactionLogMaxSize: number` The maximum size of a transaction log file. If a log file is
    empty, the first log entry will always be added regardless if it's larger than the max size. If
    a log file is not empty and the entry is larger than the space available, the log file is
    rotated to the next sequence number. Defaults to 16 MB.
  - `transactionLogRetention: string | number` The number of minutes to retain transaction logs
    before purging. Defaults to `'3d'` (3 days).
  - `transactionLogsPath: string` The path to store transaction logs. Defaults to
    `"${db.path}/transaction_logs"`.

### `db.close()`

Closes a database. This function can be called multiple times and will only close an opened
database. A database instance can be reopened once its closed.

```typescript
const db = RocksDatabase.open('foo');
db.close();
```

### `db.config(options)`

Sets global database settings.

- `options: object`
  - `blockCacheSize: number` The amount of memory in bytes to use to cache uncompressed blocks.
    Defaults to 32MB. Set to `0` (zero) disables block cache for future opened databases. Existing
    block cache for any opened databases is resized immediately. Negative values throw an error.

```typescript
RocksDatabase.config({
	blockCacheSize: 100 * 1024 * 1024, // 100MB
});
```

### `db.open(): RocksDatabase`

Opens the database at the given path. This must be called before performing any data operations.

```typescript
import { RocksDatabase } from '@harperfast/rocksdb-js';

const db = new RocksDatabase('path/to/db');
db.open();
```

There's also a static `open()` method for convenience that performs the same thing:

```typescript
const db = RocksDatabase.open('path/to/db');
```

### `db.name: string`

Returns the database column family's name.

```typescript
const db = new RocksDatabase('path/to/db');
console.log(db.name); // 'default'

const db2 = new RocksDatabase('path/to/db', { name: 'users' });
console.log(db.name); // 'users'
```

## Data Operations

### `db.clear(options?): Promise<number>`

Asychronously removes all data in the current database.

- `options: object`
  - `batchSize?: number` The number of records to remove at once. Defaults to `10000`.

Returns the number of entries that were removed.

Note: This does not remove data from other column families within the same database path.

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

### `db.drop(): Promise<void>`

Removes all entries in the database. If the database was opened with a `name`, the database will be
deleted on close.

```typescript
const db = RocksDatabase.open('path/to/db', { name: 'users' });
await db.drop();
db.close();
```

### `db.dropSync(): void`

Synchronous version of `db.drop()`.

```typescript
const db = RocksDatabase.open('path/to/db');
db.dropSync();
db.close();
```

### `db.get(key: Key, options?: GetOptions): MaybePromise<any>`

Retreives the value for a given key. If the key does not exist, it will resolve `undefined`.

```typescript
const result = await db.get('foo');
assert.equal(result, 'foo');
```

If the value is in the memtable or block cache, `get()` will immediately return the value
synchronously instead of returning a promise.

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
for (const key of db.getKeys()) {
	console.log(key);
}
```

### `db.getKeysCount(options?: RangeOptions): number`

Retrieves the number of keys within a range.

```typescript
const total = db.getKeysCount();
const range = db.getKeysCount({ start: 'a', end: 'z' });
```

### `db.getMonotonicTimestamp(): number`

Returns the current timestamp as a monotonically increasing timestamp in milliseconds represented as
a decimal number.

```typescript
const ts = db.getMonotonicTimestamp();
console.log(ts); // 1764307857213.739
```

### `db.getOldestSnapshotTimestamp(): number`

Returns a number representing a unix timestamp of the oldest unreleased snapshot.

Snapshots are only created during transactions. When the database is opened in optimistic mode (the
default), the snapshot will be created on the first read. When the database is opened in pessimistic
mode, the snapshot will be created on the first read or write.

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

### `db.getDBProperty(propertyName: string): string`

Gets a RocksDB database property as a string.

- `propertyName: string` The name of the property to retrieve (e.g., ) `'rocksdb.levelstats'`.

```typescript
const db = RocksDatabase.open('/path/to/database');
const levelStats = db.getDBProperty('rocksdb.levelstats');
const stats = db.getDBProperty('rocksdb.stats');
```

### `db.getDBIntProperty(propertyName: string): number`

Gets a RocksDB database property as an integer.

- `propertyName: string` The name of the property to retrieve (e.g., ) `'rocksdb.num-blob-files'`.

```typescript
const db = RocksDatabase.open('/path/to/database');
const blobFiles = db.getDBIntProperty('rocksdb.num-blob-files');
const numKeys = db.getDBIntProperty('rocksdb.estimate-num-keys');
```

### `db.getRange(options?: IteratorOptions): ExtendedIterable`

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

### `db.getUserSharedBuffer(key: Key, defaultBuffer: ArrayBuffer, options?)`

Creates a new buffer with the contents of `defaultBuffer` that can be accessed across threads. This
is useful for storing data such as flags, counters, or any ArrayBuffer-based data.

- `options?: object`
  - `callback?: () => void` A optional callback is called when `notify()` on the returned buffer is
    called.

Returns a new `ArrayBuffer` with two additional methods:

- `notify()` - Invokes the `options.callback`, if specified.
- `cancel()` - Removes the callback; future `notify()` calls do nothing

Note: If a shared buffer already exists for the given `key`, the returned `ArrayBuffer` will
reference this existing shared buffer. Once all `ArrayBuffer` instances have gone out of scope and
garbage collected, the underlying memory and notify callback will be freed.

```typescript
const buffer = new Uint8Array(db.getUserSharedBuffer('isDone', new ArrayBuffer(1)));
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

Executes all database operations within the specified callback within a single transaction. If the
callback completes without error, the database operations are automatically committed. However, if
an error is thrown during the callback, all database operations will be rolled back.

```typescript
import type { Transaction } from '@harperfast/rocksdb-js';
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

Note that `db.transaction()` returns whatever value the transaction callback returns:

```typescript
const isBar = await db.transaction(async (txn: Transaction) => {
	const foo = await txn.get('foo');
	return foo === 'bar';
});
console.log(isBar ? 'Foo is bar' : 'Foo is not bar');
```

### `db.transactionSync((txn: Transaction) => any): any`

Executes a transaction callback and commits synchronously. Once the transaction callback returns,
the commit is executed synchronously and blocks the current thread until finished.

Inside a synchronous transaction, use `getSync()`, `putSync()`, and `removeSync()`.

```typescript
import type { Transaction } from '@harperfast/rocksdb-js';
db.transactionSync((txn: Transaction) => {
	txn.putSync('foo', 'baz');
});
```

### Class: `Transaction`

The transaction callback is passed in a `Transaction` instance which contains all of the same data
operations methods as the `RocksDatabase` instance plus:

- `txn.abort()`
- `txn.commit()`
- `txn.commitSync()`
- `txn.getTimestamp()`
- `txn.id`
- `txn.setTimestamp(ts)`

#### `txn.abort(): void`

Rolls back and closes the transaction. This method is automatically called after the transaction
callback returns, so you shouldn't need to call it, but it's ok to do so. Once called, no further
transaction operations are permitted.

#### `txn.commit(): Promise<void>`

Commits and closes the transaction. This is a non-blocking operation and runs on a background
thread. Once called, no further transaction operations are permitted.

#### `txn.commitSync(): void`

Synchronously commits and closes the transaction. This is a blocking operation on the main thread.
Once called, no further transaction operations are permitted.

#### `txn.getTimestamp(): number`

Retrieves the transaction start timestamp in seconds as a decimal. It defaults to the time at which
the transaction was created.

#### `txn.id`

Type: `number`

The transaction ID represented as a 32-bit unsigned integer. Transaction IDs are unique to the
RocksDB database path, regardless the database name/column family.

#### `txn.setTimestamp(ts: number?): void`

Overrides the transaction start timestamp. If called without a timestamp, it will set the timestamp
to the current time. The value must be in seconds with higher precision in the decimal.

```typescript
await db.transaction(async (txn) => {
	txn.setTimestamp(Date.now() / 1000);
});
```

## Events

### Event: `'aftercommit'`

The `'aftercommit'` event is emitted after a transaction has been committed and the transaction has
completed including waiting for the async worker thread to finish.

- `result: object`
  - `next: null`
  - `last: null`
  - `txnId: number` The id of the transaction that was just committed.

### Event: `'beforecommit'`

The `'beforecommit'` event is emitted before a transaction is about to be committed.

### Event: `'begin-transaction'`

The `'begin-transaction'` event is emitted right before the transaction function is executed.

### Event: `'committed'`

The `'committed'` event is emitted after the transaction has been written. When this event is
emitted, the transaction is still cleaning up. If you need to know when the transaction is fully
complete, use the `'aftercommit'` event.

## Event API

`rocksdb-js` provides a EventEmitter-like API that lets you asynchronously notify events to one or
more synchronous listener callbacks. Events are scoped by database path.

Unlike `EventEmitter`, events are emitted asynchronously, but in the same order that the listeners
were added.

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

Removes an event listener. You must specify the exact same callback that was used in
`addListener()`.

```typescript
const callback = () => {};
db.addListener('foo', callback);

db.removeListener('foo', callback); // return `true`
db.removeListener('foo', callback); // return `false`, callback not found
```

### `off(event: string, callback: () => void): boolean`

Alias for `removeListener()`.

### `notify(event: string, ...args?): boolean`

Call all listeners for the given key. Returns `true` if any callbacks were found, otherwise `false`.

Unlike `EventEmitter`, events are emitted asynchronously, but in the same order that the listeners
were added.

You can optionally emit one or more arguments. Note that the arguments must be serializable. In
other words, `undefined`, `null`, strings, booleans, numbers, arrays, and objects are supported.

```typescript
db.notify('foo');
db.notify(1234);
db.notify({ key: 'bar' }, { value: 'baz' });
```

## Exclusive Locking

`rocksdb-js` includes a handful of functions for executing thread-safe mutually exclusive functions.

### `db.hasLock(key: Key): boolean`

Returns `true` if the database has a lock for the given key, otherwise `false`.

```typescript
db.hasLock('foo'); // false
db.tryLock('foo'); // true
db.hasLock('foo'); // true
```

### `db.tryLock(key: Key, onUnlocked?: () => void): boolean`

Attempts to acquire a lock for a given key. If the lock is available, the function returns `true`
and the optional `onUnlocked` callback is never called. If the lock is not available, the function
returns `false` and the `onUnlocked` callback is queued until the lock is released.

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

The `onUnlocked` callback function can be used to signal to retry acquiring the lock:

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

Releases the lock on the given key and calls any queued `onUnlocked` callback handlers. Returns
`true` if the lock was released or `false` if the lock did not exist.

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

If there are more than one simultaneous lock requests, it will block them until the lock is
available.

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
	}),
]);
```

Note: If the `callback` throws an error, Node.js suppress the error. Node.js 18.3.0 introduced a
`--force-node-api-uncaught-exceptions-policy` flag which will cause errors to emit the
`'uncaughtException'` event. Future Node.js releases will enable this flag by default.

### `db.flush(): Promise<void>`

Flushes all in-memory data to disk asynchronously.

```typescript
await db.flush();
```

### `db.flushSync(): void`

Flushes all in-memory data to disk synchronously. Note that this can be an expensive operation, so
it is recommended to use `flush()` if you want to keep the event loop free.

```typescript
db.flushSync();
```

## Transaction Log

A user controlled API for logging transactions. This API is designed to be generic so that you can
log gets, puts, and deletes, but also arbitrary entries.

### `db.listLogs(): string[]`

Returns an array of log store names.

```typescript
const names = db.listLogs();
```

### `db.purgeLogs(options?): string[]`

Deletes transaction log files older than the `transactionLogRetention` (defaults to 3 days).

- `options: object`
  - `destroy?: boolean` When `true`, deletes transaction log stores including all log sequence files
    on disk.
  - `name?: string` The name of a store to limit the purging to.

Returns an array with the full path of each log file deleted.

```typescript
const removed = db.purgeLogs();
console.log(`Removed ${removed.length} log files`);
```

### `db.useLog(name): TransactionLog`

Gets or creates a `TransactionLog` instance. Internally, the `TransactionLog` interfaces with a
shared transaction log store that is used by all threads. Multiple worker threads can use the same
log at the same time.

- `name: string | number` The name of the log. Numeric log names are converted to a string.

```typescript
const log1 = db.useLog('foo');
const log2 = db.useLog('foo'); // gets existing instance (e.g. log1 === log2)
const log3 = db.useLog(123);
```

`Transaction` instances also provide a `useLog()` method that binds the returned transaction log to
the transaction so you don't need to pass in the transaction id every time you add an entry.

```typescript
await db.transaction(async (txn) => {
	const log = txn.useLog('foo');
	log.addEntry(Buffer.from('hello'));
});
```

### Class: `TransactionLog`

A `TransactionLog` lets you add arbitrary data bound to a transaction that is automatically written
to disk right before the transaction is committed. You may add multiple enties per transaction. The
underlying architecture is thread safe.

- `log.addEntry()`
- `log.query()`

#### `log.addEntry(data, transactionId): void`

Adds an entry to the transaction log.

- `data: Buffer | UInt8Array` The entry data to store. There is no inherent limit beyond what
  Node.js can handle.
- `transactionId: Number` A related transaction used to batch entries on commit.

```typescript
const log = db.useLog('foo');
await db.transaction(async (txn) => {
	log.addEntry(Buffer.from('hello'), txn.id);
});
```

If using `txn.useLog()` (instead of `db.useLog()`), you can omit the transaction id from
`addEntry()` calls.

```typescript
await db.transaction(async (txn) => {
	const log = txn.useLog('foo');
	log.addEntry(Buffer.from('hello'));
});
```

Note that the `TransactionLog` class also has internal methods `_getMemoryMapOfFile`,
`_findPosition`, and `_getLastCommittedPosition` that should not be used directly and may change in
any version.

#### `log.query(options?): IterableIterator<TransactionLogEntry>`

Returns an iterable/iterator that streams all log entries for the given filter.

- `options: object`
  - `start?: number` The transaction start timestamp.
  - `end?: string` The transction end timestamp.
  - `exclusiveStart?: boolean` When `true`, this will only match transactions with timestamps after
    the start timestamp.
  - `exactStart?: boolean` When `true`, this will only match and iterate starting from a transaction
    with the given start timestamp. Once the specified transaction is found, all subsequent
    transactions will be returned (regardless of whether their timestamp comes before the `start`
    time). This can be combined with `exactStart`, finding the specified transaction, and returning
    all transactions that follow. By default, all transactions equal to or greater than the start
    timestamp will be included.
  - `readUncommitted?: boolean` When `true`, this will include uncommitted transaction entries.
    Normally transaction entries that haven't finished committed are not included. This is
    particularly useful for replaying transaction logs on startup where many entries may have been
    written to the log but are no longer considered committed if they were not flushed to disk.
  - `startFromLastFlushed?: boolean` When `true`, this will only match transactions that have been
    flushed from RocksDB's memtables to disk (and are within any provided `start` and `end` filters,
    if included). This is useful for replaying transaction logs on startup where many entries may
    have been written to the log but are no longer considered committed if they were not flushed to
    disk.

The iterator produces an object with the log entry timestamp and data.

- `object`
  - `data: Buffer` The entry data.
  - `timestamp: number` The entry timestamp used to collate entries by transaction.
  - `endTxn: boolean` This is `true` when the entry is the last entry in a transaction.

```typescript
const log = db.useLog('foo');
const iter = log.query({});
for (const entry of iter) {
	console.log(entry);
}

const lastHour = Date.now() - (60 * 60 * 1000);
const rangeIter = log.query({ start: lastHour, end: Date.now() });
for (const entry of rangeIter) {
	console.log(entry.timestamp, entry.data);
}
```

#### `log.getLogFileSize(sequenceNumber?: number): number`

Returns the size of the given transaction log sequence file in bytes. Omit the sequence number to
get the total size of all the transaction log sequence files for this log.

### Transaction Log Parser

#### `parseTransactionLog(file)`

In general, you should use `log.query()` to query the transaction log, however, if you need to load
an entire transaction log into memory and want detailed information about entries, you can use the
`parseTransactionLog()` utility function.

```typescript
const everything = parseTransactionLog('/path/to/file.txnlog');
console.log(everything);
```

Returns an object containing all of the information in the log file.

- `size: number` The size of the file.
- `version: number` The log file format version.
- `entries: LogEntry[]` An array of transaction log entries.
  - `data: Buffer` The entry data.
  - `flags: number` Transaction related flags.
  - `length: number` The size of the entry data.
  - `timestamp: number` The entry timestamp.

### `shutdown(): void`

The `shutdown()` will flush all in-memory data to disk and wait for any outstanding compactions to
finish, for all open databases. It is highly recommended to call this in a `process` `exit` event
listener (on the main thread), to ensure that all data is flushed to disk before the process exits:

```typescript
import { shutdown } from '@harperfast/rocksdb-js';
process.on('exit', shutdown);
```

## Custom Store

The store is a class that sits between the `RocksDatabase` or `Transaction` instance and the native
RocksDB interface. It owns the native RocksDB instance along with various settings including
encoding and the db name. It handles all interactions with the native RocksDB instance.

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
- `listLogs()`
- `open()`
- `putSync(context, key, value, options?)`
- `removeSync(context, key, options?)`
- `tryLock(key, onUnlocked?)`
- `unlock(key)`
- `useLog(context, name)`
- `withLock(key, callback?)`

To use it, extend the default `Store` and pass in an instance of your store into the `RocksDatabase`
constructor.

```typescript
import { RocksDatabase, Store } from '@harperfast/rocksdb-js';

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
> If your custom store overrides `putSync()` without calling `super.putSync()` and it performs its
> own `this.encodeKey(key)`, then you MUST encode the VALUE before you encode the KEY.
>
> Keys are encoded into a shared buffer. If the database is opened with the `sharedStructuresKey`
> option, encoding the value will load and save the structures which encodes the
> `sharedStructuresKey` overwriting the encoded key in the shared key buffer, so it's ultra
> important that you encode the value first!

## Interfaces

### `RocksDBOptions`

- `options: object`
  - `adaptiveReadahead: boolean` When `true`, RocksDB will do some enhancements for prefetching the
    data. Defaults to `true`. Note that RocksDB defaults this to `false`.
  - `asyncIO: boolean` When `true`, RocksDB will prefetch some data async and apply it if reads are
    sequential and its internal automatic prefetching. Defaults to `true`. Note that RocksDB
    defaults this to `false`.
  - `autoReadaheadSize: boolean` When `true`, RocksDB will auto-tune the readahead size during scans
    internally based on the block cache data when block caching is enabled, an end key (e.g. upper
    bound) is set, and prefix is the same as the start key. Defaults to `true`.
  - `backgroundPurgeOnIteratorCleanup: boolean` When `true`, after the iterator is closed, a
    background job is scheduled to flush the job queue and delete obsolete files. Defaults to
    `true`. Note that RocksDB defaults this to `false`.
  - `fillCache: boolean` When `true`, the iterator will fill the block cache. Filling the block
    cache is not desirable for bulk scans and could impact eviction order. Defaults to `false`. Note
    that RocksDB defaults this to `true`.
  - `readaheadSize: number` The RocksDB readahead size. RocksDB does auto-readahead for iterators
    when there is more than two reads for a table file. The readahead starts at 8KB and doubles on
    every additional read up to 256KB. This option can help if most of the range scans are large and
    if a larger readahead than that enabled by auto-readahead is needed. Using a large readahead
    size (> 2MB) can typically improve the performance of forward iteration on spinning disks.
    Defaults to `0`.
  - `tailing: boolean` When `true`, creates a "tailing iterator" which is a special iterator that
    has a view of the complete database including newly added data and is optimized for sequential
    reads. This will return records that were inserted into the database after the creation of the
    iterator. Defaults to `false`.

### `RangeOptions`

Extends `RocksDBOptions`.

- `options: object`
  - `end: Key | Uint8Array` The range end key, otherwise known as the "upper bound". Defaults to the
    last key in the database.
  - `exclusiveStart: boolean` When `true`, the iterator will exclude the first key if it matches the
    start key. Defaults to `false`.
  - `inclusiveEnd: boolean` When `true`, the iterator will include the last key if it matches the
    end key. Defaults to `false`.
  - `start: Key | Uint8Array` The range start key, otherwise known as the "lower bound". Defaults to
    the first key in the database.

### `IteratorOptions`

Extends `RangeOptions`.

- `options: object`
  - `reverse: boolean` When `true`, the iterator will iterate in reverse order. Defaults to `false`.

## Development

This package requires Node.js 18 or higher, pnpm, and a C++ compiler.

> [!TIP]
> Enable pnpm log streaming to see full build output:
>
> ```
> pnpm config set stream true
> ```

### Building

There are two things being built: the native binding and the TypeScript code. Each of those can be
built to be debug friendly.

| Description                                  | Command                                  |
| -------------------------------------------- | ---------------------------------------- |
| Production build (minified + native binding) | `pnpm build`                             |
| TypeScript only (minified)                   | `pnpm build:bundle`                      |
| TypeScript only (unminified)                 | `pnpm build:debug`                       |
| Native binding only (prod)                   | `pnpm rebuild`                           |
| Native binding only (with debug logging)     | `pnpm rebuild:debug`                     |
| Debug build everything                       | `pnpm build:debug && pnpm rebuild:debug` |

When building the native binding, it will download the appropriate prebuilt RocksDB library for your
platform and architecture from the
[rocksdb-prebuilds](https://github.com/HarperFast/rocksdb-prebuilds) GitHub repository. It defaults
to the pinned version in the `package.json` file. You can override this by setting the
`ROCKSDB_VERSION` environment variable. For example:

```bash
ROCKSDB_VERSION=10.9.1 pnpm build
```

You may also specify `latest` to use the latest prebuilt version.

```bash
ROCKSDB_VERSION=latest pnpm build
```

Optionally, you may also create a `.env` file in the root of the project to specify various
settings. For example:

```bash
echo "ROCKSDB_VERSION=10.9.1" >> .env
```

### Linux C runtime versions

When you compile `rocksdb-js`, you can specify the `ROCKSDB_LIBC` environment variable to choose
either `glibc` (default) or `musl`.

```bash
ROCKSDB_LIBC=musl pnpm rebuild
```

### Building RocksDB from Source

To build RocksDB from source, simply set the `ROCKSDB_PATH` environment variable to the path of the
local `rocksdb` repo:

```bash
git clone https://github.com/facebook/rocksdb.git /path/to/rocksdb
echo "ROCKSDB_PATH=/path/to/rocksdb" >> .env
pnpm rebuild
```

### Debugging

It is often helpful to do a debug build and see the internal debug logging of the native binding.
You can do a debug build by running:

```bash
pnpm rebuild:debug
```

Each debug log message is prefixed with the thread id. Most debug log messages include the instance
address making it easier to trace through the log output.

#### Debugging on macOS

In the event Node.js crashes, re-run Node.js in `lldb`:

```bash
lldb node
# Then in lldb:
# (lldb) run your-program.js
# When the crash occurs, print the stack trace:
# (lldb) bt
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

To run a specific unit test, for example all tests that mention `"column family"`, run:

```bash
pnpm test -t "column family"
```

Vitest's terminal renderer will often overwrite the debug log output, so it's highly recommended to
specify the `CI=1` environment variable to prevent Vitest from erasing log output:

```bash
CI=1 pnpm test
```

By default, the test runner deletes all test databases after the tests finish. To keep the temp
databases for closer inspection, set the `KEEP_FILES=1` environment variable:

```bash
CI=1 KEEP_FILES=1 pnpm test
```
