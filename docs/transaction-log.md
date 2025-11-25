# Transaction Log File Format

## Overview

The transaction log system provides an append-only, binary log format for
recording database transactions. The format is designed for:

- **Durability**: Fixed size index entries for fast traversal using binary search
- **Portability**: Big-endian encoding for platform independence
- **Efficiency**: Entry metadata and data are written to separate files
- **Scalability**: Automatic log file rotation

## File Structure

Each transaction log file consists of comprised from two files: an index file
(`.txnlog`) and a data file (`.txndata`). Files are rotated based on a
configurable maximum size (default: 1MB for index files and 16MB for data files).

### Naming Convention

Log files follow the pattern: `{name}.{sequenceNumber}.txnlog`

- `name`: The log store name
- `sequenceNumber`: Sequential integer starting from 1
- Example: `mylog.1.txnlog`, `mylog.2.txnlog`

Data files follow this convention, but with a `.txndata` extension.

## Binary Format Specification

```
+---------------------+
+ File Header         | 6 bytes
+---------------------+
+ Transaction Header  | 22 bytes
+---------------------+
+ Transaction Data    | variable
+---------------------+
+ Transaction Header  | 22 bytes
+---------------------+
+ Transaction Data    | variable
+---------------------+
| ...                 |
+---------------------+
```

### File Header (6 bytes)

```
| Offset | Size | Type    | Field      | Description            |
|--------|------|---------|------------|------------------------|
| 0      | 4    | uint32  | token      | Transaction log token  |
| 4      | 2    | uint16  | version    | Format version         |
```

### Transaction Header (22 bytes)

| Offset | Size | Type    | Field              | Description                    |
|--------|------|---------|--------------------|--------------------------------|
| 0      | 8    | double  | earliestTimestamp  | Earliest active timestamp      |
| 8      | 8    | double  | actualTimestamp    | Timestamp transaction created  |
| 16     | 4    | uint32  | dataLength         | Size of the entry data         |
| 20     | 2    | uint16  | flags              | Transaction flags              |

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

- Actions are buffered in memory per transaction ID
- Multiple transactions can be buffered concurrently
- Buffered actions are NOT written to disk until the transaction is being
  committed
- If the transaction log handle is garbage collected, buffered (uncommitted)
  actions are lost
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
- Invalid data or corrupted blocks are detected during read operations

## Reading The Transaction Log

A transaction log can be read sequentially or by range using timestamps. The
read algorithm is optimized by branching which the logic based on whether both
start and end timestamps are present. If neither timestamps are set, then a
sequential read is performed. If one or both of the timestamps are set, then the
range logic will use a binary search to find the first block to start scanning
from.

### Sequential Read

1. Parse file header to get version and block size
2. Parse block headers to identify block boundaries
3. Reconstruct multi-block transactions using CONTINUATION flags
4. Parse transaction headers to group transaction together by timestamp
5. Process transactions in order

```
read file header
for each block in log file:
  read block header

  if CONTINUATION flag is set:
    append block body to previous transaction data
  else:
    start new transaction data with block body

  transaction is complete
  parse transaction header
  parse all transactions
  process transaction
```

Note that this is the same as a range read without a start and end timestamp.

### Range Read

1. Parse file header to get version and block size
2. Determine block count (ceiling((file_size - file_header_size) / block_size))
3. Determine start and end block
4. Determine and read middle block (floor((end_block - start_block) / 2))
5. Repeatedly divide left or right range until block with first transaction is
   found
6. Reconstruct multi-block transactions using CONTINUATION flags
7. Parse transaction headers to group transaction together by timestamp
8. Process transactions in order

```
read file header
set range start block to first block
set range end block to last block

find the first block
  determine the middle block between the range start and end
  if middle block timestamp > start timestamp
    set end block to middle block and recheck range
  else
    set start block to middle block and recheck range

for each block starting at first block:
  read block header

  if CONTINUATION flag is set:
    append block body to previous transaction data
  else:
    start new transaction data with block body

  transaction is complete
  parse transaction header
  parse transactions
  if no end timestamp
    process transaction
  else if timestamp < end timestamp
    process transaction
  else
    return
```

## Performance Considerations

- **Block Size**: 4KB blocks that can be traversed quickly using binary search
- **Batching**: Use transactions to batch multiple actions into fewer disk
  writes using `writev()`
- **Zero-Copy**: The format supports memory-mapped I/O for efficient reading

## Limitations

- Maximum single transaction size: ~4GB (uint32 limit)
- Maximum transaction size: Limited by available memory during buffering
- Maximum log file size: Configurable, default 1MB for index files and 16MB for
  data files

## Version History

- **Version 1.0**: Initial format specification
  - Hybrid index and data file log format
  - Big-endian encoding
  - Transaction buffering
