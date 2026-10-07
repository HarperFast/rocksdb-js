# Design notes

- [Transaction-log mapping capacity](src/binding/transaction_log/DESIGN.md) — fixed reader capacity, first-append publication, and rotation.
- [Transaction-log retention](src/binding/transaction_log/DESIGN.md#retention-and-the-sequence-witness) — purging the current segment, and `txn.state` as the restart sequence witness.
- [Verification table](src/binding/core/DESIGN.md): slots store versions encoded with a per-key tag; a park cancels its lock wake registration when it ends.
- [Database resources](src/binding/database/DESIGN.md): optimistic commit validation policy,
  per-database lock buckets, and concurrent commit threads.
- [N-API error builders](src/binding/napi/DESIGN.md): `createRocksDBError`/`createJSError` always write their out-param, even when their own N-API calls fail.
