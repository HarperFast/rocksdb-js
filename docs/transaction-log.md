The transaction log is a file that contains a sequence of entries that describe the updates that have been made to the database. These updates are intended to be largely opaque to RocksDB, and to be can be used by the rocksdb-js consumer to implement their own replay and subscription semantics.
The transaction log consists of 4KB blocks that each have their own header.
The block header contains the following fields:
- `earliest-timestamp`: A 8-byte timestamp field that indicates the timestamp of the earliest transaction that was active or was written in this block. There should be no transactions in this block or any subsequent block that has a timestamp earlier than this. This should be stored in a double/float64 format of the timestamp in epoch milliseconds, in big-endian ordering. 
- `flags`: A 2-byte entry field with any flags that are relevant to the transaction log.
- `startOffset`:  A 2-byte entry field indicating the start of the first entry in this block. If this block begins with a continuation of transaction entry from a previous block, the starting point will came after the transaction entry. If there is no continuation, this should be 12. If the continued transaction entry covers this entire block, this should be 4096. This should be stored in big-endian ordering.

The block body contains a sequence of transaction entries. The block body has 4KB - header (4084 bytes) of space available for entries. The entries are sequentially written. Each entry has a header:
- `timestamp`: A 8-byte timestamp field that indicates the transaction timestamp of the log entry. This should be stored in a double/float64 format of the timestamp in epoch milliseconds, in big-endian ordering.
- `length`: A 4-byte entry field indicating the length of the entry in bytes. This should be stored in big-endian ordering. A value of 0 indicates there are no more entries in this block, and the next entry should be found in the next block (if there is little remaining room in the current block, it may be preferable to write the next entry in the next block if it won't fit). This length may be greater than the remaining bytes in the block, or of a whole block. In this case the entry is continued in the next block or across multiple blocks. An entry can also even span transaction log files as necessary.

This is followed by the entry data. This is opaque data to rocksdb-js.

Each entry is intended to represent an action within a transaction. Multiple entries with the same timestamp may be present in a block, which indicates multiple actions within a single transaction.

Transaction log files are organized by the log name, which is a string used to identify the directory that contains the log files. Log files are numbered sequentially, starting at 1. For a transaction log named "7", the first log file is "7/1.log". When a log file reaches its maximum size, which defaults to 16MB, a new log file is created at the next number (like "7/2.log"), and writes continue to the new file. (note that large transaction entries could span multiple files).

A new transaction log file may be started even if the previous file has not reached its maximum size. A new transaction log file should be started if the timestamp is older than 1/8 of the transaction retention period.
