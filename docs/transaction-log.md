# Transaction Log File Format

## Overview

The transaction log system provides an append-only, binary log format for recording database transactions. The format is designed for:

- **Durability**: 4KB-aligned blocks for efficient filesystem I/O
- **Portability**: Big-endian encoding for platform independence
- **Efficiency**: Zero-padded blocks minimize write amplification
- **Scalability**: Support for multi-block transactions of arbitrary size

## File Structure

Each transaction log file (`.txnlog`) consists of one or more 4KB blocks. Files are rotated based on a configurable maximum size (default: 16MB).

### Naming Convention

Log files follow the pattern: `{name}.{sequenceNumber}.txnlog`

- `name`: The log store name
- `sequenceNumber`: Sequential integer starting from 1
- Example: `mylog.1.txnlog`, `mylog.2.txnlog`

## Binary Format Specification

### Block Structure (4096 bytes)

Each block is exactly 4KB and contains:

```
+------------------+
| Block Header     | 12 bytes
+------------------+
| Block Body       | 4084 bytes
+------------------+
| Zero Padding     | (if needed)
+------------------+
Total: 4096 bytes
```

### Block Header (12 bytes)

All multi-byte integers are encoded in **big-endian** format.

| Offset | Size | Type    | Field       | Description                                    |
|--------|------|---------|-------------|------------------------------------------------|
| 0      | 8    | uint64  | timestamp   | Block timestamp (milliseconds since epoch)     |
| 8      | 2    | uint16  | flags       | Block flags (see below)                        |
| 10     | 2    | uint16  | dataOffset  | Offset where valid data starts (usually 12)    |

#### Block Flags

| Bit | Flag         | Description                                             |
|-----|--------------|---------------------------------------------------------|
| 0   | CONTINUATION | This block continues data from the previous block       |
| 1   | HAS_MORE     | Transaction data continues in the next block            |
| 2-15| Reserved     | Reserved for future use (must be 0)                     |

### Block Body (4084 bytes)

The block body contains transaction data. If the data is less than 4084 bytes, the remainder is zero-padded.

## Transaction Format

Transactions are stored within block bodies. A transaction consists of a header followed by one or more actions.

### Transaction Header (16 bytes)

| Offset | Size | Type    | Field        | Description                                    |
|--------|------|---------|--------------|------------------------------------------------|
| 0      | 8    | uint64  | timestamp    | Earliest action timestamp in the transaction   |
| 8      | 2    | uint16  | flags        | Transaction flags (reserved, must be 0)        |
| 10     | 2    | uint16  | actionCount  | Number of actions in this transaction          |
| 12     | 4    | uint32  | totalLength  | Total transaction size including header (bytes)|

**Important**: The `actionCount` field groups actions together. All actions within a transaction are written consecutively, and this count indicates how many actions belong to the same logical transaction.

### Action Structure

Each action consists of a header followed by the action data.

#### Action Header (12 bytes)

| Offset | Size | Type    | Field      | Description                                    |
|--------|------|---------|------------|------------------------------------------------|
| 0      | 8    | uint64  | timestamp  | Action timestamp (milliseconds since epoch)    |
| 8      | 4    | uint32  | dataLength | Length of action data in bytes                 |

#### Action Data (variable)

Raw binary data stored in big-endian format. The interpretation of this data is application-specific.

## Multi-Block Transactions

When a transaction exceeds 4084 bytes (the block body size), it spans multiple blocks:

1. **First Block**:
   - Contains block header (flags = 0 or HAS_MORE if continued)
   - Contains start of transaction data
   - Block body is zero-padded to 4KB

2. **Continuation Blocks**:
   - Block header has CONTINUATION flag set
   - If more blocks follow, HAS_MORE flag is also set
   - Block body continues transaction data from previous block
   - Zero-padded to 4KB

3. **Last Block**:
   - Has CONTINUATION flag set (if not the first block)
   - Does NOT have HAS_MORE flag set
   - Contains remaining transaction data
   - Zero-padded to 4KB

### Example: 10KB Transaction

```
Block 1: [Header(HAS_MORE)] + [4084 bytes of data]
Block 2: [Header(CONTINUATION | HAS_MORE)] + [4084 bytes of data]
Block 3: [Header(CONTINUATION)] + [1832 bytes of data] + [2252 bytes padding]
```

## Encoding Details

### Endianness

All multi-byte numeric values are stored in **big-endian** (network byte order) format:

- **uint64**: Most significant byte first
- **uint32**: Most significant byte first
- **uint16**: Most significant byte first

This ensures the format is portable across different CPU architectures.

### Timestamps

All timestamps are stored as 64-bit unsigned integers representing milliseconds since the Unix epoch (January 1, 1970 00:00:00 UTC).

### Padding

Blocks that don't fully utilize the 4084-byte body are zero-padded to maintain the 4KB alignment. This ensures efficient I/O operations at the filesystem level.

## Transaction Buffering

The transaction log system supports buffering multiple actions before committing them as a single transaction:

### Without Transaction

```javascript
const log = db.useLog('mylog');
log.addEntry(Date.now(), Buffer.from('action1')); // Written immediately as 1-action transaction
log.addEntry(Date.now(), Buffer.from('action2')); // Written immediately as 1-action transaction
```

### With Transaction

```javascript
const log = db.useLog('mylog');
const txn = db.transaction();

// Buffer actions
log.addEntry(Date.now(), Buffer.from('action1'), { transaction: txn });
log.addEntry(Date.now(), Buffer.from('action2'), { transaction: txn });
log.addEntry(Date.now(), Buffer.from('action3'), { transaction: txn });

// Commit all buffered actions as a single transaction
log.commit(txn);
```

### Buffering Behavior

- Actions are buffered in memory per transaction ID
- Multiple transactions can be buffered concurrently
- Buffered actions are NOT written to disk until `commit()` is called
- If the transaction log handle is garbage collected, buffered (uncommitted) actions are lost
- Calling `commit()` with an unknown transaction ID throws an error

## Usage Examples

### Basic Usage

```javascript
import { Database } from 'rocksdb-js';

const db = new Database('/tmp/mydb');
db.open();

const log = db.useLog('orders');

// Write a single action
log.addEntry(Date.now(), Buffer.from(JSON.stringify({
  type: 'CREATE_ORDER',
  orderId: 12345,
  amount: 99.99
})));
```

### Multi-Action Transaction

```javascript
const log = db.useLog('orders');
const txn = db.transaction();

// Buffer multiple related actions
log.addEntry(Date.now(), Buffer.from(JSON.stringify({
  type: 'CREATE_ORDER',
  orderId: 12345
})), { transaction: txn });

log.addEntry(Date.now(), Buffer.from(JSON.stringify({
  type: 'RESERVE_INVENTORY',
  items: ['SKU-001', 'SKU-002']
})), { transaction: txn });

log.addEntry(Date.now(), Buffer.from(JSON.stringify({
  type: 'CHARGE_PAYMENT',
  amount: 99.99
})), { transaction: txn });

// Atomically write all actions as a single transaction
log.commit(txn);
```

### Large Data Handling

```javascript
const log = db.useLog('backups');

// Large data (>4KB) automatically spans multiple blocks
const largeData = Buffer.alloc(20000);
// ... fill with data ...

log.addEntry(Date.now(), largeData);
// This creates a multi-block transaction automatically
```

## Implementation Notes

### Memory Management

- Transaction buffering is managed by the `TransactionLogHandle`, not the `TransactionLogStore`
- When a JavaScript `TransactionLog` object is garbage collected, its handle is destroyed and all buffered transaction data is automatically freed
- The `TransactionLogStore` is long-lived and does not hold transaction buffers

### Thread Safety

- All log operations are thread-safe
- Multiple processes can write to the same log store simultaneously
- Each process maintains its own transaction buffers

### File Rotation

- Log files are automatically rotated when they reach the configured maximum size
- Rotation happens on the next write after the size limit is exceeded
- Old log files can be automatically purged based on retention policy

### Error Handling

- Write failures throw exceptions
- Commit with unknown transaction ID throws an error
- Invalid data or corrupted blocks are detected during read operations

## Recovery and Replay

When reading transaction logs:

1. Read blocks sequentially
2. Parse block headers to identify block boundaries
3. Reconstruct multi-block transactions using CONTINUATION and HAS_MORE flags
4. Parse transaction headers to group actions together
5. Process actions in order

### Reading Algorithm

```
for each block in log file:
    read block header

    if CONTINUATION flag is set:
        append block body to previous transaction data
    else:
        start new transaction data with block body

    if HAS_MORE flag is NOT set:
        transaction is complete
        parse transaction header
        parse all actions
        process transaction
```

## Best Practices

1. **Action Size**: Keep individual actions reasonably sized. While the format supports large actions, smaller actions improve memory efficiency during buffering.

2. **Transaction Grouping**: Group related actions into transactions for atomic replay during recovery.

3. **Timestamps**: Use consistent timestamp sources (e.g., `Date.now()`) for proper ordering.

4. **Data Encoding**: Store action data in a self-describing format (JSON, Protocol Buffers, etc.) for easier replay and debugging.

5. **Error Handling**: Always wrap log operations in try-catch blocks to handle write failures gracefully.

6. **Retention Policy**: Configure appropriate retention periods to manage disk space usage.

## Performance Considerations

- **Block Alignment**: 4KB blocks match common filesystem block sizes, minimizing I/O overhead
- **Batching**: Use transactions to batch multiple actions into fewer disk writes
- **Zero-Copy**: The format supports memory-mapped I/O for efficient reading
- **Write Amplification**: Zero-padding trades disk space for write efficiency

## Limitations

- Maximum action count per transaction: 65,535 (uint16 limit)
- Maximum single action size: ~4GB (uint32 limit)
- Maximum transaction size: Limited by available memory during buffering
- Maximum log file size: Configurable, default 16MB

## Version History

- **Version 1.0**: Initial format specification
  - 4KB block-aligned format
  - Big-endian encoding
  - Multi-block transaction support
  - Transaction buffering

