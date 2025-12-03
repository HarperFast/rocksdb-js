# Transaction Log File Format

## Overview

The transaction log system provides an append-only, binary log format for
recording database transactions. The format is designed for:

- **Durability**: Fixed size index entries for fast traversal using binary search
- **Portability**: Big-endian encoding for platform independence
- **Efficiency**: Entry metadata and data are written to separate files
- **Scalability**: Automatic log file rotation

## File Structure

Each transaction log store consists of one or more transaction log sequence
files. These sequence log files have a `.txnlog` extension and are rotated based
on a configurable maximum size (default: 16MB).

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
| Offset | Size | Type    | Field          | Description            |
|--------|------|---------|----------------|------------------------|
| 0      | 4    | uint32  | token          | Transaction log token  |
| 4      | 1    | uint8   | version        | Format version         |
| 5      | 8    | double  | fileTimestamp  | The latest timestamp   |
```

#### `token`

The token is used to validate that the file is indeed a transaction log.

#### `version`

The transaction log file format version. Currently, version `1` is the latest.

#### `fileTimestamp`

?????

### Transaction Header (13 bytes)

| Offset | Size | Type    | Field         | Description                    |
|--------|------|---------|---------------|--------------------------------|
| 0      | 8    | double  | txnTimestamp  | Timestamp transaction created  |
| 8      | 4    | uint32  | entrySize     | Size of the entry data         |
| 12     | 1    | uint8   | flags         | Transaction flags              |

#### `txnTimestamp`

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

All multi-byte numeric values are stored in **big-endian** (network byte order)
format:

- **uint64**: Most significant byte first
- **uint32**: Most significant byte first
- **uint16**: Most significant byte first

This ensures the format is portable across different CPU architectures.

### Timestamps

All timestamps are stored as 64-bit doubles representing milliseconds since the
Unix epoch (January 1, 1970 00:00:00 UTC).

## Transaction Buffering

The transaction log system buffers multiple log entries before committing them
when the associated transaction is committed.

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
- Buffered log entries are NOT written to disk until right before the
  transaction is committed
- If the transaction log handle is garbage collected, buffered (uncommitted)
  log entries are lost
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

- Transaction buffering is managed by the `TransactionLogHandle`, not the
  `TransactionLogStore`
- When a JavaScript `TransactionLog` object is garbage collected, its handle is
  destroyed and all buffered transaction data is automatically freed
- The `TransactionLogStore` is long-lived and does not hold transaction buffers

### Thread Safety

- All log operations are thread-safe
- Multiple worker threads can write to the same log store simultaneously
- Transactions are bound to a single worker thread

### File Rotation

- Log files are automatically rotated when either the index or data file reaches
  their configured maximum sizes
- Rotation happens on the next write after the size limit is exceeded
- Old log files can be automatically purged based on retention policy

### Error Handling

- Write failures throw exceptions
- Commit with unknown transaction ID throws an error
- Invalid data is detected during read operations

## Reading The Transaction Log

Log entries are not guaranteed to be in order, but are guaranteed to have a
monotonic timestamp. When reading the transaction log file, each transaction
entry header must be read, then sorted and indexed. Using this index, queries can
find all entries within a time range using a binary search and seek to get the
associated entry data.

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

In addition to max file size, if a log file hasn't been written to in more than
a certain amount of time, it will rotate to the next sequence log file. This
max age is a percentage of the retention period.

The default retention period is 3 days and the default max age is 75% of the
retention period for a threshold period of 18 hours. If a log file hasn't been
written to in the past 18 hours, it will start a new file.

## Performance Considerations

- **Max File Size**: 16MB soft limit
- **Batching**: Use transactions to batch multiple actions into fewer disk
  writes using `writev()`
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
