# Transaction Log File Format

## Overview

The transaction log system provides an append-only, binary log format for recording database
transactions. The format is designed for:

- **Durability**: Fixed size index entries for fast traversal using binary search
- **Portability**: Big-endian encoding for platform independence
- **Efficiency**: Entry metadata and data are written to separate files
- **Scalability**: Automatic log file rotation

## File Structure

Each transaction log store consists of one or more transaction log sequence files. These sequence
log files have a `.txnlog` extension and are rotated based on a configurable maximum size (default:
16MB).

### Naming Convention

Log files follow the pattern: `{name}.{sequenceNumber}.txnlog`

- `name`: The log store name
- `sequenceNumber`: Sequential integer starting from 1
- Example: `mylog.1.txnlog`, `mylog.2.txnlog`

## Binary Format Specification

```
+---------------------+
+ File Header         | 13 bytes
+---------------------+
+ Transaction Header  | 13 bytes
+---------------------+
+ Transaction Data    | variable
+---------------------+
+ Transaction Header  | 13 bytes
+---------------------+
+ Transaction Data    | variable
+---------------------+
| ...                 |
+---------------------+
```

### File Header (13 bytes)

```
| Offset | Size | Type    | Field      | Description            |
|--------|------|---------|------------|------------------------|
| 0      | 4    | uint32  | token      | Transaction log token  |
| 4      | 1    | uint8   | version    | Format version         |
| 5      | 8    | double  | timestamp  | The latest timestamp   |
```

#### `token`

The token is used to validate that the file is indeed a transaction log.

#### `version`

The transaction log file format version. Currently, version `1` is the latest.

#### `timestamp`

The timestamp of the most recent transaction log batch that has been written.

### Transaction Header (13 bytes)

| Offset | Size | Type   | Field     | Description                   |
| ------ | ---- | ------ | --------- | ----------------------------- |
| 0      | 8    | double | timestamp | Timestamp transaction created |
| 8      | 4    | uint32 | entrySize | Size of the entry data        |
| 12     | 1    | uint8  | flags     | Transaction flags             |

#### `timestamp`

The timestamp the associated transaction was created.

#### `entrySize`

The size of the data entry directly following the transaction header.

#### `flags`

Transaction entry related flags.

| Flag                              | Value  | Description                                |
| --------------------------------- | ------ | ------------------------------------------ |
| `TRANSACTION_LOG_ENTRY_LAST_FLAG` | `0x01` | Indicates the last entry for a transaction |

## Encoding Details

### Endianness

All multi-byte numeric values are stored in **big-endian** (network byte order) format:

- **uint64**: Most significant byte first
- **uint32**: Most significant byte first
- **uint16**: Most significant byte first

This ensures the format is portable across different CPU architectures.

### Timestamps

All timestamps are stored as 64-bit doubles representing milliseconds since the Unix epoch (January
1, 1970 00:00:00 UTC).

## Transaction Buffering

The transaction log system buffers multiple log entries before committing them when the associated
transaction is committed.

```javascript
const log = db.useLog('example');
await db.transaction((txn) => {
	log.addEntry(Buffer.from('some data'), txn.id);
	log.addEntry(Buffer.from('some more data'), txn.id);
});
```

### Buffering Behavior

- Log entries are buffered in memory per transaction ID
- Multiple transactions can be buffered concurrently
- Buffered log entries are NOT written to disk until right before the transaction is committed
- If the transaction log handle is garbage collected, buffered (uncommitted) log entries are lost
- Calling `addEntry()` with an unknown transaction ID throws an error

## Usage Examples

### Basic Usage

```javascript
import { RocksDatabase } from 'rocksdb-js';

const db = RocksDatabase.open('/tmp/mydb');
const log = db.useLog('example');

await db.transaction((txn) => {
	log.addEntry(Buffer.from('some data'), txn.id);
});
```

### Manual Transaction

```javascript
import { RocksDatabase, Transaction } from 'rocksdb-js';

const db = RocksDatabase.open('/tmp/mydb');
const log = db.useLog('example');
const txn = new Transaction(db);

log.add(Buffer.from('some data'), txn.id);
await txn.commit();
```

### Multi-Entry Transaction

```javascript
const log1 = db.useLog('log1');
const log2 = db.useLog('log2');

await db.transaction((txn) => {
	log1.addEntry(Buffer.from('some data'), txn.id);
	log1.addEntry(Buffer.from('some more data'), txn.id);

	log2.addEntry(Buffer.from('some data'), txn.id);
});
```

### Transaction Scoped Log

```javascript
await db.transaction((txn) => {
	const log = txn.useLog('log1');
	log.addEntry(Buffer.from('some data'));
	log.addEntry(Buffer.from('some more data'));
});
```

## Implementation Notes

### Memory Management

- Transaction buffering is managed by the `TransactionLogHandle`, not the `TransactionLogStore`
- When a JavaScript `TransactionLog` object is garbage collected, its handle is destroyed and all
  buffered transaction data is automatically freed
- The `TransactionLogStore` is long-lived and does not hold transaction buffers

### Thread Safety

- All log operations are thread-safe
- Multiple worker threads can write to the same log store simultaneously
- Transactions are bound to a single worker thread

### File Rotation

- Log files are automatically rotated when either the index or data file reaches their configured
  maximum sizes
- Rotation happens on the next write after the size limit is exceeded
- Old log files can be automatically purged based on retention policy. The sequence file named by
  `txn.state` and every newer file form the live store's retention floor, so an idle store can keep
  one bounded file past the cutoff until a later write rotates and flushes it.

### Error Handling

- Write failures throw exceptions
- Commit with unknown transaction ID throws an error
- Invalid data is detected during read operations

### Validation

A store directory can be validated offline with `validateTransactionLogStore(path, options?)`: it
checks every log file's header (token, version) and entry framing using the same scan as open-time
crash recovery, plus file-name/sequence continuity and the `txn.state` side file. A torn tail,
sequence gap, or implausible `txn.state` position is a warning by default (a torn tail is
recoverable — open-time recovery truncates it losslessly) and an error with `{ strict: true }`,
which `backups.verify()` uses for backup snapshots since an intact snapshot can have none of them.
The CLI exposes the same check as `verify-logs [name]`.

## Reading The Transaction Log

Log entries are not in timestamp order. A transaction claims its timestamp from the process-wide
monotonic clock when it is constructed but is appended when it commits, so under concurrency a later
entry can carry a smaller timestamp; an entry that adopted an origin timestamp with
`txn.setTimestamp()` carries that origin's clock instead. Timestamps are not unique: the monotonic
clock never issues the same value twice within a process, but a restart after the wall clock moved
backwards can reissue one unless the floor below is seeded, and `txn.setTimestamp()` can assign any
value — including one already
in the log — to as many transactions as the caller likes. Deduplicating on the timestamp alone is
therefore never safe; a consumer that needs identity has to supply it (Harper pairs the timestamp
with the originating node).

When reading the transaction log file, each transaction entry header must be read and indexed. The
index records only the entries whose timestamp is greater than every earlier one in the file — a
running maxima — and a query seeks to the lower bound of that index, which is guaranteed to sit at
or before every entry in the requested range. Reading forward from there and filtering is what makes
range queries correct on an unordered file.

## The Timestamp Floor At Open

A transaction's timestamp is the key of the batch it is written under, so it has to stay unique
within the log it is written to. The process clock (`db.getMonotonicTimestamp()`) guarantees that
only within one process: a new process reads the wall clock again, so a backward step between runs
can reissue a key that is already durable in the log.

Opening with the `timestampFloorLog` option names the log whose keys this process originates. Every
segment of that store is then walked once, after open-time recovery has decided which bytes are
still durable, and the process clock is raised above the largest key found — before the database
handle is returned, so no transaction can be constructed below it. Keys are not ordered within or
across segments, and a segment header records the store's latest timestamp only as of that
segment's creation, so there is no shortcut: every segment is read.

Name only a log this process originates. A log a replication receiver writes under an adopted origin
timestamp is keyed by another node's clock, and seeding from it would ratchet this process's clock
to the fastest of those nodes at each restart. Native code cannot tell the two kinds of log apart,
which is why the caller names it and why an unset option leaves the clock alone.

The walk is the cost of the guarantee, and it is paid on the calling thread before `open()` returns.
It reads entry headers through a shared 64 KiB window and seeks past payloads, so it scales with the
number of entries in the named log rather than its size. Measured on Linux with a warm page cache,
against the same database opened without the option:

| Named log                         | Added open time (median) |
| --------------------------------- | ------------------------ |
| 25,000 entries, 99 MB, 7 segments | ~31 ms                   |
| 250,000 entries, 19 MB            | ~273 ms                  |

Roughly a millisecond per thousand entries. Retention bounds a log's age, not its entry count, so
the walk is bounded directly instead: `ROCKSDB_JS_TIMESTAMP_FLOOR_SCAN_MS` (default `2000`) caps it,
newest segment first, and a walk that runs out of budget warns and leaves the floor where it got to.
The budget is checked between segments, so it can overshoot by one segment's walk — up to roughly
1.2 million entries in a default 16 MB segment.
The value is honored literally, `0` included, and there is no unbounded setting — the failure it
bounds is an `open()` that does not return, so a deployment that would rather wait raises the number.
A database opened without `timestampFloorLog` pays none of it.

The seed is best effort, and says so when it falls short: a segment that cannot be opened or scanned
emits a `log.warn` global event, as does a mid-file framing break (the entries past it are durable
and `query()` resyncs to them, but this walk stops there), a budget that runs out, and a
`timestampFloorLog` naming a log the database does not have. A key more than ten years ahead of the
wall clock is left out of the floor as corruption rather than a rollback to recover from, per entry
rather than per segment so it does not take the real keys beside it with it, and it warns too.

The option is fixed at the first open of a path in the process — the database descriptor is shared
across handles and `worker_threads` envs — so a later open of the same path that names a log cannot
re-run the seed and warns instead of appearing to work.

### Sequential Read

1. Parse file header to get version
2. While there is bytes to read
   1. Parse first transaction entry header to identify the entry data length
   2. Read the entry data
   3. Next transaction entry header immediately follow the current entry data

```
read file header
for each transaction entry in log file:
  read transaction entry header
  read transaction entry data
```

### Range Read

1. Parse file header to get version
2. Build index
   1. While there is bytes to read
      1. Parse first transaction entry header to identify the entry data length
      2. Next transaction entry header immediately follow the current entry data
3. Query index with start and end timestamp range
4. Extract entry data from transaction log file

```
read file header

init index
for each transaction entry in log file:
  add transaction entry header to index
search index for matching entries based on timestamp
for each log entry
  extract log entry data
```

## Max Age and Automatic Rotation

In addition to max file size, if a log file hasn't been written to in more than a certain amount of
time, it will rotate to the next sequence log file. This max age is a percentage of the retention
period.

The default retention period is 3 days and the default max age is 75% of the retention period for a
threshold period of 18 hours. If a log file hasn't been written to in the past 18 hours, it will
start a new file.

## Performance Considerations

- **Max File Size**: 16MB soft limit
- **Batching**: Use transactions to batch multiple actions into fewer disk writes using `writev()`
- **Zero-Copy**: The format supports memory-mapped I/O for efficient reading

## Limitations

- Maximum single transaction size: ~4GB (uint32 limit)
- Maximum transaction size: Limited by available memory during buffering
- Maximum log file size: Configurable, default 16MB

## Version History

- **Version 1.0**: Initial format specification
  - File log format
  - Big-endian encoding
  - Transaction buffering
