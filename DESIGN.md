# Design notes

- [Transaction-log mapping capacity](src/binding/transaction_log/DESIGN.md) — fixed reader capacity, first-append publication, and rotation.
- [Transaction-log retention](src/binding/transaction_log/DESIGN.md#retention-and-the-sequence-witness) — purging the current segment, and `txn.state` as the restart sequence witness.
- [Transaction-log conditional close](src/binding/transaction_log/DESIGN.md#conditional-close) — why `tryClose()` refuses when any transaction bound during its check.
- [Verification table](src/binding/core/DESIGN.md): slots store versions encoded with a per-key tag; a park cancels its lock wake registration when it ends.
- [Database resources](src/binding/database/DESIGN.md): optimistic commit validation policy,
  per-database lock buckets, concurrent commit threads, and backup table verification.
- [Transaction abort](src/binding/transaction/DESIGN.md): abort drains in-flight async gets before `Rollback()` and holds an `OperationGuard` so teardown cannot close the transaction under it.
- [Pooled transaction iterators](src/binding/iterator/DESIGN.md): transaction `getRange()` re-seeks an idle native iterator after rewriting the read-bound Slices it points to.
- [N-API error builders](src/binding/napi/DESIGN.md): `createRocksDBError`/`createJSError` always write their out-param, even when their own N-API calls fail.
