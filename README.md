# rocksdb-js

A Node.js binding for the RocksDB library.

## Features

- Supports optimistic and pessimistic transactions
- Hybrid sync/async data retrieval
- Range queries return an iterable with array-like methods and lazy evaluation
- Transaction log system for recording transaction related data
- Custom stores provide ability to override default database interactions
- Efficient binary key and value encoding
- Access to internal RocksDB statistics
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
  - `disableWAL: boolean` Whether to disable the RocksDB write ahead log. Defaults to `false`.
  - `enableStats: boolean` When `true` and the database is open, RocksDB will captures stats that
    are retrieved by calling `db.getStats()`. Enabling statistics imposes 5-10% in overhead.
    Defaults to `false`.
  - `name: string` The column family name. Defaults to `"default"`.
  - `noBlockCache: boolean` When `true`, disables the block cache. Block caching is enabled by
    default and the cache is shared across all database instances.
  - `parallelismThreads: number` The number of background threads to use for flush and compaction.
    Defaults to `1`.
  - `pessimistic: boolean` When `true`, throws conflict errors when they occur instead of waiting
    until commit. Defaults to `false`.
  - `readOnly: boolean` When `true`, the database is opened in read-only mode. Read operations are
    permitted. Write operations will throw an error with code `ERR_DATABASE_READONLY`. Transactions
    are a no-op in read-only mode.
  - `statsLevel: StatsLevel` Controls which type of statistics to skip and reduce statistic
    overhead. Defaults to `StatsLevel.ExceptDetailedTimers`.
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
  - `verificationTable: boolean` When `true`, this column family participates in the process-global
    [Verification Table](#verification-table): transaction writes to this column family invalidate
    the verification slot for each written key. Enable this only for column families whose records
    are cached (e.g. the primary column family of a table). Defaults to `false`. Requires
    `verificationTableEntries` to be configured before the first database is opened.

### `db.close()`

Closes a database. This function can be called multiple times and will only close an opened
database. A database instance can be reopened once its closed.

```typescript
const db = RocksDatabase.open('foo');
db.close();
```

### `db.columns: string[]`

Returns the list of column families in the RocksDB database.

```typescript
const db = RocksDatabase.open('path/to/db');
console.log(db.columns); // ['default']

const db2 = new RocksDatabase('path/to/db', { name: 'users' });
console.log(db.columns); // ['default', 'users']
```

### `db.config(options)`

Sets global database settings.

- `options: object`
  - `blockCacheSize: number` The amount of memory in bytes to use to cache uncompressed blocks.
    Defaults to 32MB. Set to `0` (zero) disables block cache for future opened databases. Existing
    block cache for any opened databases is resized immediately. Negative values throw an error.
  - `compactOnClose: boolean` When `true`, compacts the database on close. Defaults to `false`.
  - `verificationTableEntries: number` The number of slots in the process-global
    [Verification Table](#verification-table). Each slot is 8 bytes, so the default of `131072`
    (128K) slots is 1 MB. Set to `0` to disable the verification table. This must be configured
    before the first database is opened; once the table is materialized, attempts to change this
    value throw.
  - `writeBufferManagerAllowStall: boolean` When `true`, writes are stalled once the manager's
    `buffer_size` is exceeded, providing a hard cap on memtable memory. When `false`, memtables are
    allowed to grow past the limit and flushes are simply scheduled more aggressively. Off by
    default to favor write throughput over hard memory bounding. Defaults to `false`.
  - `writeBufferManagerCostToCache: boolean` When `true`, memtable memory is "charged" against the
    shared block cache so the block cache and write buffers draw from a single pool. During write
    bursts the cache shrinks to make room for memtables; once memtables flush, the cache can grow
    back into the reclaimed space. Defaults to `false`.
  - `writeBufferManagerSize: number` Total memtable memory limit (bytes) shared across every
    database opened in this process. When set, RocksDB uses a single `WriteBufferManager` so write
    buffers are bounded process-wide rather than per database. A value of `0` disables the manager.
    Defaults to `0`.

```typescript
RocksDatabase.config({
	blockCacheSize: 100 * 1024 * 1024, // 100MB
	compactOnClose: true,
	writeBufferManagerAllowStall: false,
	writeBufferManagerCostToCache: false,
	writeBufferManagerSize: 64 * 1024 * 1024, // 64MB
});
```

### `db.isOpen(): boolean`

Returns `true` if the database is open, otherwise false.

```typescript
console.log(db.isOpen()); // true or false
```

### `db.name: string`

Returns the database column family's name.

```typescript
const db = new RocksDatabase('path/to/db');
console.log(db.name); // 'default'

const db2 = new RocksDatabase('path/to/db', { name: 'users' });
console.log(db.name); // 'users'
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

### `db.status: 'opened' | 'closed'`

Returns a string `'opened'` or `'closed'` indicating if the database is opened or closed.

```typescript
console.log(db.status);
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

### `db.compact(options?): Promise<void>`

Compacts a range of keys in the database. In RocksDB, deleted keys are not immediately removed from
the database. Instead, they are marked as deleted and a tombstone is written. This function
triggers a manual compaction which removes the tombstones and reclaims space. Only one compaction
per database path can be performed at a time.

- `options: object`
  - `start?: Key` The start key of the range to compact.
  - `end?: Key` The end key of the range to compact.

```typescript
await db.compact();

await db.compact({ start: 'a', end: 'z' });
```

### `db.compactSync(options?): void`

Synchronous version of `compact()`.

```typescript
db.compactSync();

db.compactSync({ start: 'a', end: 'z' });
```

### `db.destroy(): void`

Completely removes a database based on the `db` instance's path including all data, column families,
and files on disk.

```typescript
db.destroy();
console.log(fs.existsSync(db.path)); // false
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
const value = result instanceof Promise ? await result : result;
assert.equal(result, 'foo');
```

Note that all errors are returned as rejected promises.

See [`GetOptions`](#getoptions) for the available options.

When the `expectedVersion` option is set and the [Verification Table](#verification-table) records
a matching version for the key, `get()` returns the `FRESH_VERSION_FLAG` sentinel
(`constants.FRESH_VERSION_FLAG`) instead of reading the value — signalling that any value the
caller has already cached for this key is still fresh and no read was performed. Be sure to check
for this sentinel before treating the result as a value:

```typescript
import { constants } from '@harperfast/rocksdb-js';

const result = db.get(key, { expectedVersion: cachedEntry.version });
if (result === constants.FRESH_VERSION_FLAG) {
	// the cached value is still valid; no read occurred
	return cachedEntry.value;
}
```

### `db.getSync(key: Key, options?: GetOptions): any`

Synchronous version of `get()`. Like `get()`, this can return the `FRESH_VERSION_FLAG` sentinel when
the `expectedVersion` option is used.

### `db.getEstimatedKeyCount(): number`

Retrieves the estimated number of keys in the database. This is an alias for
`db.getDBIntProperty('rocksdb.estimate-num-keys')`.

```typescript
const estimated = db.getEstimatedKeyCount();
console.log(estimated);
```

### `db.getKeys(options?: IteratorOptions): ExtendedIterable`

Retrieves all keys within a range.

```typescript
for (const key of db.getKeys()) {
	console.log(key);
}
```

### `db.getKeysCount(options?: RangeOptions): number`

Retrieves the exact number of keys in a database or a range.

```typescript
const count = db.getKeysCount(); // estimated number of keys
const range = db.getKeysCount({ start: 'a', end: 'z' }); // exact number of keys in the range
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

### `db.getDBProperty(propertyName: string): string | undefined`

Gets a RocksDB database property as a string.

- `propertyName: string` The name of the property to retrieve (e.g., ) `'rocksdb.levelstats'`.

Returns `undefined` if the property is not found.

```typescript
const db = RocksDatabase.open('/path/to/database');
const levelStats = db.getDBProperty('rocksdb.levelstats');
const stats = db.getDBProperty('rocksdb.stats');
```

### `db.getDBIntProperty(propertyName: string): number | undefined`

Gets a RocksDB database property as an integer.

- `propertyName: string` The name of the property to retrieve (e.g., ) `'rocksdb.num-blob-files'`.

Returns `undefined` if the property is not found.

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

### `db.transaction<T>(callback: TransactionCallback<T>, options?: TransactionOptions): Promise<T>`

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

### `db.transactionSync<T>(callback: TransactionCallback<T>, options?: TransactionOptions): T`

Executes a transaction callback and commits synchronously. Once the transaction callback returns,
the commit is executed synchronously and blocks the current thread until finished.

Inside a synchronous transaction, use `getSync()`, `putSync()`, and `removeSync()`.

```typescript
import type { Transaction } from '@harperfast/rocksdb-js';
db.transactionSync((txn: Transaction) => {
	txn.putSync('foo', 'baz');
});
```

### Optimistic and Pessimistic Modes

`rocksdb-js` supports two different transaction modes: optimistic and pessimistic. The default mode
is optimistic.

- Optimistic: Conflicts detected at commit time.
- Pessimistic: Conflicts throw immediately on detection.

When a database is opened in optimistic mode, transactions are not locked and can be retried if
they fail with a conflict. When a database is opened in pessimistic mode, transactions are aborted
and cannot be retried if they fail with a conflict.

Optimistic mode is the default mode and is recommended for most use cases. Pessimistic mode is
recommended for use cases where you need to know immediately if a conflict occurs.

If a database is opened in one mode, it cannot be opened in a different mode. An error will be
thrown when trying to open it in a different mode without closing the database first.

### `TransactionCallback<T>`

`(txn: Transaction, attempt: number) => T | PromiseLike<T>`

A sync or async function to encapsulate all of the transaction operations. Once the function is
executed, the transaction is automatically committed. If the function returns a value, it will be
returned from the transaction call.

The `txn` parameter is a `Transaction`. See the [Transaction](#class-transaction) section for more
details.

The `attempt` parameter is the number of times the transaction has been retried.

### `TransactionOptions`

- `coordinatedRetry?: boolean` When `true`, an `IsBusy` conflict at commit time is retried
  automatically instead of being rejected. Rather than retrying immediately and racing the
  conflicting transaction again, the retry waits until the conflicting transaction has committed and
  released its write intent, then re-runs the transaction body right away with no backoff delay.
  Requires the column family to be opened with `verificationTable: true`. See
  [Verification Table](#verification-table). Defaults to `false`.
- `disableSnapshot?: boolean` Whether to disable snapshots. Defaults to `false`.
- `maxRetries?: number` The maximum number of times to retry the transaction. Defaults to `3`.
- `retryOnBusy?: boolean` Whether to retry the transaction if the commit fails with `IsBusy`.
  Defaults to `true` when the transaction is bound to a transaction log, otherwise `false`.

### Transaction Retry Logic

The retry mechanism will only be active when the `retryOnBusy` option is `true` or when
`retryOnBusy` is `undefined` and the transaction is bound to a transaction log. The attempts starts
at `1` and ends at `maxRetries`.

When using a transaction log and the commit fails with `ERR_BUSY`, the transaction log will be in
a bad state and the transaction will need to be retried. If the max retries is reached or the
transaction is not retried, a `ERR_TRANSACTION_ABANDONED` error will be thrown.

Users should use the `attempt` transaction callback parameter to ensure duplicate transaction log
entries are not added.

When `coordinatedRetry: true`, the retry behavior changes for `IsBusy` conflicts: instead of
rejecting (or retrying immediately and potentially conflicting again), the commit waits until the
conflicting transaction has committed and released its write intent, then re-runs the transaction
body immediately with no backoff. This is still bounded by `maxRetries` — if the transaction has
not committed after the configured number of coordinated retries, the transaction is abandoned with
an `ERR_TRANSACTION_ABANDONED` error. Coordinated retry requires the column family to be opened with
`verificationTable: true`.

### Class: `Transaction`

The transaction callback is passed in a `Transaction` instance which contains all of the same data
operations methods as the `RocksDatabase` instance plus:

- `txn.abort()` Rolls back and closes the transaction. This method is automatically called after the
  transaction callback returns, so you shouldn't need to call it, but it's ok to do so. Once called,
  no further transaction operations are permitted. Calling this method multiple times has no effect.
- `txn.commit(): Promise<void>` Asynchronously commits the transaction and closes the transaction.
- `txn.commitSync()` Synchronously commits and closes the transaction.
- `txn.getTimestamp(): number` Retrieves the transaction start timestamp in seconds as a decimal. It
  defaults to the time at which the transaction was created.
- `txn.id: number` The read-only transaction ID. Transaction IDs are unique to the RocksDB database
  path, regardless the database name/column family.
- `txn.setTimestamp(ts?: number): void` Overrides the transaction start timestamp. If called without
  a timestamp, it will set the timestamp to the current time. The value must be in seconds with
  higher precision in the decimal.

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
more synchronous listener callbacks. There are two types of events:

- Per-database events: scoped by database path.
- Process-global events: not scoped by database path.

Unlike `EventEmitter`, events are emitted asynchronously, but in the same order that the listeners
were added.

```typescript
// Process-global events
RocksDatabase.on('log.warn', console.warn);

RocksDatabase.on('foo', (...args) => {
	console.log(args);
});
RocksDatabase.notify('foo', 'bar');
RocksDatabase.off('foo', callback);

// Per-database events
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

## Statistics

Retrieve RocksDB statistics at runtime. You must set `enableStats: true` when calling `db.open()`.
Statistics are captured at the database level and include all column families.

RocksDB has two types of statistics: tickers and histograms. Tickers are 64-bit unsigned integers
that measure counters. Histograms are objects containing various measurements of statistic
distribution across all operations.

```typescript
import { RocksDatabase, stats } from '@harperfast/rocksdb-js';
const db = RocksDatabase.open('/path/to/db', {
	enableStats: true,
	statsLevel: stats.StatsLevel.ExceptDetailedTimers, // default
});
console.log(db.getStats());
```

### `db.getStat(statName: string): RocksDBStat`

Retrieves a single statistic value. Return value is either a `number` or `StatsHistogramData`
object.

```typescript
console.log(db.getStat('rocksdb.block.cache.miss'));
```

### `db.getStats(all?: boolean): RocksDBStats`

Returns an object containing a curated list of column family-level properties, internal tickers
stats, and internal histogram stats. Return value is an object with the stat name as the key and
a `RocksDBStat` as the value.

By default, it only returns the most meaningful internal stats. When `all = true`, it returns the
same column family-level properties, but includes all internal tickers and histogram stats.

Column family and ticker stat values are 64-bit unsigned integers and histogram values are
`StatsHistogramData` objects.

The result also always includes a summarized, aggregate set of `txnlog.*` keys covering all of
the database's transaction logs. These are present regardless of whether statistics are enabled
and can be fetched individually with `db.getStat('txnlog.…')`. For detailed, per-log statistics —
including memory-map usage — use [`log.getStats()`](#loggetstats-transactionlogstats). All stat
names are documented in [docs/stats.md](docs/stats.md).

```typescript
// get essential stats
console.log(db.getStats());

// get all stats
console.log(db.getStats(true));

// transaction log bytes across all logs
console.log(db.getStats()['txnlog.totalSizeBytes']);
```

### `stats`

An object containing stat-specific constants. The full catalog of available stat names (RocksDB
tickers, histograms, internal properties, and transaction log stats) is documented in
[docs/stats.md](docs/stats.md).

#### `stats.StatsLevel`

The `stats.StatsLevel` contains constants used to set which types of skip and reduce statistic overhead.

- `StatsLevel.DisableAll` Disable all metrics.
- `StatsLevel.ExceptTickers` Disable all tickers.
- `StatsLevel.ExceptHistogramOrTimers` Disable timer stats and skip histogram stats.
- `StatsLevel.ExceptTimers` Skip timer stats
- `StatsLevel.ExceptDetailedTimers` Skip time waiting for mutex locks and compression.
- `StatsLevel.ExceptTimeForMutex` Skip time waiting for mutex locks.
- `StatsLevel.All` Collects all stats.

### `type RocksDBStat = number | StatsHistogramData`

A `RocksDBStat` is either a `number` or `StatsHistogramData` object.

### `type RocksDBStats = Record<string, RocksDBStat>`

A `RocksDBStats` is an object with the stat name as the key and a `RocksDBStat` as the value.

### `type StatsHistogramData`

An object is a record with the following properties:

- `average: number` A double containing the average value.
- `count: number` An unsigned 64-bit integer containing the number of values.
- `max: number` A double containing the maximum value.
- `median: number` A double containing the median value.
- `min: number` A double containing the minimum value.
- `percentile95: number` A double containing the 95th percentile value.
- `percentile99: number` A double containing the 99th percentile value.
- `standardDeviation: number` A double containing the standard deviation.
- `sum: number` An unsigned 64-bit integer containing the sum of all values.

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
		return new Promise((resolve) => setTimeout(resolve, 100));
	}),
	db.withLock('key', () => {
		console.log('second lock blocking for 100ms');
		return new Promise((resolve) => setTimeout(resolve, 100));
	}),
	db.withLock('key', () => {
		console.log('third lock acquired');
	}),
]);
```

Note: If the `callback` throws an error, Node.js suppress the error. Node.js 18.3.0 introduced a
`--force-node-api-uncaught-exceptions-policy` flag which will cause errors to emit the
`'uncaughtException'` event. Future Node.js releases will enable this flag by default.

## Exclusive File Locking

`rocksdb-js` includes helper functions for creating lock files and releasing them using native APIs.
This can be used to prevent multiple processes from concurrently accessing a resource. The lock is
automatically released when the process exits.

### `tryFileLock(file: string): number`

Attempts to acquire an exclusive lock on the given file, creating it if it doesn't exist. Returns a
non-zero token to pass to `fileLockRelease` if the lock was acquired, or `0` if another holder — in
any process, container, or worker thread — currently has it. Throws if the file's parent directory
is missing or on a hard error.

```typescript
import { tryFileLock } from '@harperfast/rocksdb-js';

const token = tryFileLock('/path/to/lock');
if (token) {
	console.log('lock acquired');
} else {
	console.log('lock not available, another process is holding it');
}
```

### `fileLockRelease(token: number): void`

Releases the file lock for the given token.

```typescript
import { fileLockRelease } from '@harperfast/rocksdb-js';

fileLockRelease(token);
```

## Verification Table

The verification table is a process-global, fixed-size structure that lets an application cheaply
check whether a value it has already cached is still fresh, without performing a full read. It is
intended for read-heavy workloads where records carry a monotonically increasing numeric version.

Each record's version is the numeric value stored in the first 8 bytes of its value (interpreted as
a big-endian float64). The table maps `(database, column family, key)` to a single 8-byte slot that
holds the last-known version for that key. Because slots are addressed by a hash, distinct keys may
share a slot; a collision only ever causes a conservative miss (a real read), never a stale value to
be treated as fresh.

The freshness check works as follows:

1. Pass `{ expectedVersion }` to `get()` / `getSync()`. If the slot currently records that version,
   the read is skipped and the `FRESH_VERSION_FLAG` sentinel is returned.
2. On a cold read, pass `{ populateVersion: true }` (or call `db.populateVersion()` afterward) to
   seed the slot with the version extracted from the value, so subsequent freshness checks hit.

Transaction writes to a column family opened with `verificationTable: true` invalidate the slot for
each written key at write time, so a stale version can never survive a write. This is also what
enables [`coordinatedRetry`](#transactionoptions): a conflicting transaction parks on the slot and
retries once the write intent is released.

The table is **process-global** and backed by a single shared structure, so versions populated on
the main thread are visible to `worker_threads` workers and vice versa.

### Enabling the verification table

The table must be sized before the first database is opened, via the
[`verificationTableEntries`](#dbconfigoptions) config option (default `131072` slots = 1 MB; set to
`0` to disable). Then opt-in per column family with the `verificationTable: true` open option:

```typescript
import { RocksDatabase } from '@harperfast/rocksdb-js';

RocksDatabase.config({ verificationTableEntries: 128 * 1024 });

const db = RocksDatabase.open('path/to/db', { verificationTable: true });
```

Enable `verificationTable` only for column families whose records are cached (e.g. the primary
column family of a table); enabling it adds per-write slot invalidation overhead.

### `db.verifyVersion(key: Key, version: number): boolean`

Returns `true` when the verification table currently records `version` for `key` (in this database
and column family), indicating a cached value with that version is still fresh. Returns `false`
otherwise — including when the table is disabled. This is a cheap, synchronous check that performs
no database read.

```typescript
if (db.verifyVersion(key, cachedEntry.version)) {
	return cachedEntry.value;
}
const value = db.getSync(key);
db.populateVersion(key, extractVersion(value));
```

### `db.populateVersion(key: Key, version: number): void`

Seeds the verification-table slot for `key` with `version`. This is typically called after a full
read where the caller already knows the version. It has no effect if the slot is currently
lock-tagged (a transaction is mid-write on that key) or if the verification table is disabled.

Passing `{ populateVersion: true }` to `get()` / `getSync()` performs the equivalent seeding
automatically after a cold read, avoiding a separate call.

## Transaction Log

A user controlled API for logging transactions. This API is designed to be generic so that you can
log gets, puts, and deletes, but also arbitrary entries.

Transaction logs are isolated by the database path allowing different column families in the same
database to share the transaction log store, but not other databases.

### Memory-Map Handling

Transaction log files are read through read-only memory maps. Understanding how these maps interact
with system memory helps when interpreting [`log.getStats()`](#loggetstats-transactionlogstats)
figures and process memory usage:

- **Maps are created lazily and live until the file is closed.** Writing log entries does not map
  anything; a log file is mapped the first time a `log.query()` reads from it. The native layer
  holds each file's map for the life of the file — it is released when the log file is purged or
  the database is closed, not by garbage collection. JS `Buffer` views over the map (including
  `entry.data`) hold an additional reference, so the underlying memory is never unmapped while a
  view is still reachable. `stats.memory.activeMaps` counts the maps currently held by the native
  layer.
- **Mapped bytes are virtual, not resident.** Creating a map reserves address space only. A page
  consumes physical RAM (RSS) when it is first read (demand paging). Querying a multi-gigabyte log
  can show `memory.mappedBytes` in the gigabytes while actual memory usage barely moves.
- **Queries are zero-copy.** The iterator returned by `log.query()` reads only each entry's small
  header; `entry.data` is a `Buffer` view directly into the map (no copy). Payload pages are
  faulted into memory only if and when the entry data is actually read.
- **Resident pages are reclaimable.** Because the maps are read-only and file-backed, every
  resident page is "clean" — the kernel can evict it at any time under memory pressure and re-read
  it from disk on the next access. Mapped log data therefore lives in the page cache and cannot
  exhaust memory the way heap allocations can; a full scan of a log larger than RAM will simply
  cycle pages through the cache.

OS-specific differences:

- **POSIX (Linux and macOS):** The active write file is mapped at the full configured
  `transactionLogMaxSize` (an anonymous reservation with the file's contents overlaid on top), so
  `memory.mappedBytes` over-reports the active file; `memory.overlayBytes` is the file-backed
  portion and is the closer proxy for real consumption.
- **macOS:** Activity Monitor's "Memory" column reports the physical footprint, which excludes
  clean file-backed pages — mapped log data is essentially invisible there even when resident. Use
  process RSS (e.g. `process.memoryUsage().rss`, `ps`, or `vmmap <pid>`) to observe it.
- **Linux:** Resident map pages are visible in process RSS, and `/proc/<pid>/smaps` reports exact
  per-file `Rss`/`Pss` for each mapped `.txnlog` file.
- **Windows:** Maps are created with `CreateFileMapping`/`MapViewOfFile` at the file's current
  size. There is no overlay mechanism, so `memory.overlayBytes` is always `0`, and the active
  write file's map is not cached because it is not growable.

### `db.listLogs(): string[]`

Returns an array of log store names.

```typescript
const names = db.listLogs();
```

### `db.purgeLogs(options?): string[]`

### `db.purgeLogs({ includeEntryCounts: true, ...options }): { path: string; entries: number }[]`

Deletes transaction log files older than the `transactionLogRetention` (defaults to 3 days).

- `options: object`
  - `before?: number` Remove all transaction log files older than the specified timestamp.
  - `destroy?: boolean` When `true`, deletes transaction log stores including all log sequence files
    on disk.
  - `includeEntryCounts?: boolean` When `true`, counts the entries in each deleted log file and
    returns an array of `{ path, entries }` objects instead of an array of file paths. Counting reads
    each file before it is removed, so it is only performed when this option is enabled.
  - `name?: string` The name of a store to limit the purging to.

The method is overloaded so the return type follows `includeEntryCounts`: by default (omitted or
`false`) it returns `string[]` — the full path of each log file deleted; when `includeEntryCounts` is
`true` it returns `{ path: string; entries: number }[]`, each entry being the `path` of the deleted
log file and the number of `entries` it held. Because of the overloads, the object-array form is
returned directly when `includeEntryCounts: true` is passed as a literal, with no casting required.

```typescript
const removed = db.purgeLogs();
console.log(`Removed ${removed.length} log files`);

// Include the entry count for each deleted log file:
const purged = db.purgeLogs({ includeEntryCounts: true });
for (const { path, entries } of purged) {
	console.log(`Removed ${path} (${entries} entries)`);
}
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
- `log.path`
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

#### `log.path: string`

Returns the path to the transaction log store files.

```typescript
const log = db.useLog('foo');
console.log(log.path);
```

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

const lastHour = Date.now() - 60 * 60 * 1000;
const rangeIter = log.query({ start: lastHour, end: Date.now() });
for (const entry of rangeIter) {
	console.log(entry.timestamp, entry.data);
}
```

#### `log.getLogFileSize(sequenceNumber?: number): number`

Returns the size of the given transaction log sequence file in bytes. Omit the sequence number to
get the total size of all the transaction log sequence files for this log.

#### `log.getStats(): TransactionLogStats`

Returns a detailed statistics snapshot for this transaction log, including file/transaction
gauges, memory-map usage, recovery positions, purge/retention gauges, and lifetime counters. All
sizes are in bytes; timestamps are milliseconds since the Unix epoch.

```typescript
const log = db.useLog('replication');
const stats = log.getStats();

stats.fileCount; // number of sequence files on disk
stats.totalSizeBytes; // total bytes across all sequence files
stats.memory.mappedBytes; // bytes mapped into memory (virtual address space)
stats.memory.overlayBytes; // POSIX file-backed overlay portion (0 on Windows)
stats.replayGapBytes; // bytes between the last flushed position and the write head
stats.purge.retainedUnflushedFiles; // files past retention but kept (not yet flushed to RocksDB)
stats.totals.transactionsWritten; // lifetime count of transactions written
```

> **Memory note:** `memory.mappedBytes` is virtual address space — the active write file is mapped
> at the full configured `transactionLogMaxSize` on POSIX, so it does not reflect resident memory.
> `memory.overlayBytes` (POSIX only) is the file-backed portion and is the closer proxy for real
> consumption.

The `purge.retainedUnflushedFiles` gauge is useful for diagnosing why logs are not being cleaned
up: a file can be older than the retention period but still retained because its transactions have
not yet been flushed to RocksDB (purging it would be unsafe for crash recovery).

### Transaction Log Initialization

When a database is opened, `rocksdb-js` will automatically discover the transaction log files. If
a corrupt transaction log file is detected, the `log.warn` event is emitted on the global
`RocksDatabase` instance. This should be wired up prior to opening the database.

```typescript
RocksDatabase.on('log.warn', console.warn);

const db = RocksDatabase.open('path/to/db');
```

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

### `currentThreadId(): number`

Returns the current thread ID.

```typescript
import { currentThreadId } from '@harperfast/rocksdb-js';
console.log(currentThreadId());
```

### `registryStatus(): RegistryStatus`

Returns an array containing that status of all active RocksDB instances.

- `path: string` The database path.
- `refCount: number` The number of JavaScript database instances plus the registry's reference.
- `columnFamiles: object` A map of column family names and their their info.
  - `userSharedBuffers: number` The count of active user shared buffers.
- `transactions: number` The count of active transactions.
- `closables: number` The count of active database, transactions, and iterators.
- `locks: number` The count of active locks.
- `listenerCallbacks: number` The count of in-flight callbacks.

```typescript
import { registryStatus } from '@harperfast/rocksdb-js';
console.log(registryStatus());
```

### `shutdown(): void`

The `shutdown()` will flush all in-memory data to disk and wait for any outstanding compactions to
finish, for all open databases. It is highly recommended to call this in a `process` `exit` event
listener (on the main thread), to ensure that all data is flushed to disk before the process exits:

```typescript
import { shutdown } from '@harperfast/rocksdb-js';
process.on('exit', shutdown);
```

### `versions: { 'rocksdb': string; 'rocksdb-js': string }`

Returns the `rocksdb-js` and RocksDB version.

```typescript
import { versions } from '@harperfast/rocksdb-js';
console.log(versions); // { "rocksdb": "10.10.1", "rocksdb-js": "0.1.2" }
```

## Checkpoints

### `db.createCheckpoint(targetPath: string): Promise<void>`

Creates a [checkpoint](https://github.com/facebook/rocksdb/wiki/Checkpoints) — a point-in-time,
fully independent copy of the entire database (all column families) at `targetPath` — and resolves
once written. Unlike a backup, a checkpoint is a normal, writable database: open it as a new
`RocksDatabase` and it diverges independently from the source.

SST and blob files are **hard-linked** when `targetPath` is on the **same filesystem** as the
database, and **copied** otherwise; other files (such as the `MANIFEST`) are always copied. As a
result the operation is near-instant on the same filesystem and as costly as a full copy across
filesystems. The memtable is flushed so the checkpoint includes the latest writes even when the
WAL is disabled.

Parent directories are created as needed. `targetPath` itself must not already exist — RocksDB
creates the checkpoint directory — and the call rejects with `Create checkpoint failed: target
path exists` if it does (other failures, such as a full disk, surface the RocksDB status message).
The caller is responsible for opening the checkpoint and for eventually deleting the directory.

```typescript
const db = RocksDatabase.open('/path/to/database');
await db.createCheckpoint('/path/to/checkpoint');

// The checkpoint is a normal, writable database.
const branch = RocksDatabase.open('/path/to/checkpoint');
```

## Backups

Backups use RocksDB's `BackupEngine` to capture consistent, incremental, checksum-verified
snapshots of a database. A backup covers the **entire database** — every column family, the
manifest, and (by default) the write-ahead log — so it is not scoped to an individual `Store`.

A backup can be written to a local **directory** (incremental, with a management API) or streamed to
a **`WritableStream`** as a tar archive with no intermediate copy on disk. See
[docs/backups.md](docs/backups.md) for a full guide covering both modes, restore, checkpoints, and
caveats.

Creating a backup is an instance method (`db.backup()`) because it needs a live database. The
remaining operations act on a backup directory and do not require an open database, so they are
grouped under the `backups` namespace export.

> **Only one backup per directory may be in-flight at a time.** RocksDB has no cross-engine lock on
> a backup directory, so the writing operations — `db.backup()`, `backups.delete()`, and
> `backups.purge()` — take an on-disk lock (a `.backup.lock` file) for the directory. A second
> writing operation on the same directory, whether from the same process, a `worker_thread`, or a
> separate process, **rejects** with a "locked" error rather than corrupting the backup; retry once
> the in-flight operation finishes. Operations on _different_ directories run in parallel, and the
> read-only operations (`list`, `verify`, `restore`) are not locked.

```typescript
import { RocksDatabase, backups } from '@harperfast/rocksdb-js';

const db = RocksDatabase.open('/path/to/database');
await db.put('foo', 'bar');

// Create a backup, then restore it into a fresh directory.
const id = await db.backup('/path/to/backups');
await backups.restore('/path/to/backups', '/path/to/restored-db');
```

### `db.backup(backupDir: string, options?: BackupOptions): Promise<number>`

Creates a new backup of the entire database into `backupDir`, creating parent directories as
needed, and resolves with the new backup id (a monotonically increasing integer). Subsequent
backups into the same directory are incremental — unchanged immutable files are shared rather than
re-copied.

When the database was opened with `disableWAL`, the memtable is flushed before the backup by
default so unflushed data is not lost; otherwise flushing follows `options.flushBeforeBackup`.

```typescript
const id = await db.backup('/path/to/backups', { metadata: 'nightly-2026-06-04' });
```

`BackupOptions`:

| Option                    | Type      | Default                | Description                                                                          |
| ------------------------- | --------- | ---------------------- | ------------------------------------------------------------------------------------ |
| `backupLogFiles`          | `boolean` | `true`                 | Include write-ahead log files in the backup.                                         |
| `flushBeforeBackup`       | `boolean` | `true` if WAL disabled | Flush the memtable before backing up.                                                |
| `maxBackgroundOperations` | `number`  | `1`                    | Number of background threads used to copy files.                                     |
| `metadata`                | `string`  | `''`                   | Application metadata stored with the backup, returned by `list()`.                   |
| `shareFilesWithChecksum`  | `boolean` | `true`                 | Distinguish shared files by checksum to avoid cross-database clashes.                |
| `shareTableFiles`         | `boolean` | `true`                 | Share files between backups to enable incremental backups.                           |
| `sync`                    | `boolean` | `true`                 | `fsync` backup files (including the transaction log snapshot) for crash consistency. |
| `transactionLogs`         | `boolean` | `false`                | Snapshot the transaction log store into `<backupDir>/transaction_logs/<backupId>/`.  |

When `transactionLogs` is enabled, the log snapshot is staged and atomically renamed into
`<backupDir>/transaction_logs/<backupId>/` only after every file has been copied (and fsynced, per
`sync`), so a crash mid-backup can never leave a partial log snapshot for a listed backup id — a
backup either has its complete snapshot or none. The snapshot is captured just after the RocksDB
engine snapshot, so restored logs may run slightly ahead of the restored key-value data (never
behind it), which is safe for redo-style logs replayed against the restored data.

### `db.backup(stream: WritableStream<Uint8Array>, options?: BackupStreamOptions): Promise<void>`

Streams a consistent snapshot of the entire database to `stream` as a tar archive, with **no
intermediate copy written to disk**, and resolves once the stream has been fully written and closed.
Backpressure is honored end to end, so a slow consumer (e.g. an upload) paces the backup rather than
buffering it in memory. The archive unpacks with any tar tool into a directory that opens as a
RocksDB database.

```typescript
import { createWriteStream } from 'node:fs';
import { Writable } from 'node:stream';

await db.backup(Writable.toWeb(createWriteStream('/path/to/backup.tar')));
// Restore: `tar -xf backup.tar -C /restored`, then open '/restored'.

// Or gzip it (`tar -xzf backup.tar.gz` to restore):
await db.backup(Writable.toWeb(createWriteStream('/path/to/backup.tar.gz')), { gzip: true });
```

`BackupStreamOptions`:

| Option              | Type      | Default                | Description                                                         |
| ------------------- | --------- | ---------------------- | ------------------------------------------------------------------- |
| `flushBeforeBackup` | `boolean` | `true` if WAL disabled | Flush the memtable before streaming.                                |
| `gzip`              | `boolean` | `false`                | Gzip-compress the archive, producing a `.tar.gz` instead of `.tar`. |

Stream backups are always full snapshots (no incremental sharing), have no `backups.*` management
API, and **cannot be resumed** — a failed transfer must be restarted from the beginning. See
[docs/backups.md](docs/backups.md#stream-backups) for details.

### `backups.restore(backupDir: string, dbDir: string, options?: RestoreOptions): Promise<void>`

Restores a backup from `backupDir` into `dbDir` (creating parent directories as needed). The
database must **not** be open at `dbDir`, and the default restore mode is **destructive** — it
purges `dbDir` before restoring. Restoring into the backup directory itself is rejected.

```typescript
// Restore the latest backup.
await backups.restore('/path/to/backups', '/path/to/restored-db');

// Restore a specific backup id without purging matching existing files.
await backups.restore('/path/to/backups', '/path/to/restored-db', {
	backupId: 1,
	mode: 'keepLatestDbSessionIdFiles',
});
```

`RestoreOptions`:

| Option         | Type                                                                  | Default           | Description                                                  |
| -------------- | --------------------------------------------------------------------- | ----------------- | ------------------------------------------------------------ |
| `backupId`     | `number`                                                              | latest backup     | The backup id to restore.                                    |
| `walDir`       | `string`                                                              | `dbDir`           | Directory to restore write-ahead log files into.             |
| `keepLogFiles` | `boolean`                                                             | `false`           | Keep existing log files in `walDir` rather than overwriting. |
| `mode`         | `'purgeAllFiles' \| 'keepLatestDbSessionIdFiles' \| 'verifyChecksum'` | `'purgeAllFiles'` | The restore strategy (default purges the destination).       |

### `backups.list(backupDir: string): Promise<BackupInfo[]>`

Lists the non-corrupt backups in `backupDir`, ordered by id.

```typescript
const list = await backups.list('/path/to/backups');
// [{ backupId: 1, timestamp: 1749000000, size: 4096, numberFiles: 3, appMetadata: '' }, ...]
```

Each `BackupInfo` contains `backupId`, `timestamp` (seconds since the epoch), `size` (bytes),
`numberFiles`, and `appMetadata`.

### `backups.delete(backupDir: string, backupId: number): Promise<void>`

Deletes a specific backup. Shared files are reference-counted and only removed once no remaining
backup references them, so this is not equivalent to deleting files manually.

### `backups.purge(backupDir: string, keepCount: number): Promise<void>`

Deletes all but the newest `keepCount` backups.

### `backups.verify(backupDir: string, backupId: number, options?: { verifyWithChecksum?: boolean }): Promise<void>`

Verifies a backup's file sizes, and optionally their checksums (which requires reading all
backed-up data). Resolves if the backup is intact and rejects otherwise.

```typescript
await backups.verify('/path/to/backups', 1, { verifyWithChecksum: true });
```

## Custom Store

The store is a class that sits between the `RocksDatabase` or `Transaction` instance and the native
RocksDB interface. It owns the native RocksDB instance along with various settings including
encoding and the db name. It handles all interactions with the native RocksDB instance.

The default `Store` contains the following methods which can be overridden:

- `constructor(path, options?)`
- `close()`
- `compact(options?)`
- `compactSync(options?)`
- `decodeKey(key)`
- `decodeValue(value)`
- `encodeKey(key)`
- `encodeValue(value)`
- `get(context, key, alwaysCreateNewBuffer?, options?)`
- `getCount(context, options?, txnId?)`
- `getKeys(context, options?)`
- `getKeysCount(context, options?)`
- `getRange(context, options?)`
- `getSync(context, key, options?)`
- `getUserSharedBuffer(key, defaultBuffer?)`
- `hasLock(key)`
- `isOpen()`
- `listLogs()`
- `open()`
- `populateVersion(key, version)`
- `putSync(context, key, value, options?)`
- `removeSync(context, key, options?)`
- `tryLock(key, onUnlocked?)`
- `unlock(key)`
- `useLog(context, name)`
- `verifyVersion(key, version)`
- `withLock(key, callback?)`

To use it, extend the default `Store` and pass in an instance of your store into the `RocksDatabase`
constructor.

```typescript
import { RocksDatabase, Store } from '@harperfast/rocksdb-js';

class MyStore extends Store {
	get(context, key, alwaysCreateNewBuffer, options) {
		console.log('Getting:', key);
		return super.get(context, key, alwaysCreateNewBuffer, options);
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

### `GetOptions`

Options for `get()`, `getSync()`, and the `getBinary*` methods.

- `options: object`
  - `expectedVersion: number` When set, the [Verification Table](#verification-table) is checked
    before reading. If the slot holds this version, the read is skipped and the
    `FRESH_VERSION_FLAG` sentinel is returned. After a database read, the slot is seeded with the
    version extracted from the value. Requires the column family to be opened with
    `verificationTable: true`.
  - `populateVersion: boolean` When `true`, after a database read the verification-table slot is
    seeded with the version extracted from the value, eliminating the need for a separate
    `db.populateVersion()` call on cold reads. Defaults to `false`.
  - `skipDecode: boolean` When `true`, the value is returned without being decoded. Defaults to
    `false`.

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

## CLI

The `rocksdb-js` CLI is a command line interface for interacting with RocksDB databases.

```bash
rocksdb-js [dbpath]
```

Options:

- `-h, --help` Show this help message
- `-r, --readonly` Open the database in read-only mode
- `-v, --version` Show the version information

Available commands:

- `backups <dir> [subcommand]` Manage database backups; with no subcommand or `ls`/`list`, lists the backups in
  `<dir>`. Subcommands: `backup` (create a backup of the open database), `restore <backup-id>`
  (restore into the open database after confirmation; unavailable in read-only mode),
  `verify <backup-id>` (checksum verification), `delete <backup-id>`, and `purge <keep-count>`
  (delete all but the newest backups). `delete` and `purge` report the recovered disk space.
- `clear` Clear all data in the current column family
- `columns` List column families
- `compact` Compact the current column family
- `count` Count the number of keys in the current column family
- `drop <column>` Permanently drop a column family
- `exit` Exit the REPL
- `get <key>` Get the value of a key
- `help` Show this help message
- `log [name] [file] [entry]` List the transaction log store names and log store files
- `prop <key>` Get a RocksDB property (try "rocksdb.stats")
- `purge-logs <name>` Purge transaction log files older than 3 days
- `put <key> <value>` Set the value of a key
- `query [start] [end]` Query a range of keys
- `remove <key>` Delete a key
- `repl` Open a JS sub-REPL; "db" refers to the current column family
- `stats` Show the statistics for the current column family
- `use [column]` Create a new column family or switch to an existing one

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

### Windows C runtime versions

By default on Windows, `rocksdb-js` is compiled with the `/MT` flag. This will statically link the
C runtime making the binary self-contained and portable.

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

#### Native C++ unit tests (GoogleTest)

Build and run standalone native tests (RocksDB + binding `core/` helpers, no Node in the test
binary):

```bash
pnpm test:native
```

Coverage report for native tests (macOS/Linux; requires `lcov`):

```bash
pnpm coverage:native
# open coverage/native/html/index.html
```

Note: Code coverage is not supported on Windows. Tests are run without coverage.

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
