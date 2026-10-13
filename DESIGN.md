# Design notes

- [Transaction-log mapping capacity](src/binding/transaction_log/DESIGN.md) — fixed reader capacity, first-append publication, and rotation.
- [Verification table](src/binding/core/DESIGN.md): slots store versions encoded with a per-key tag; a park cancels its lock wake registration when it ends.
- [Database resources](src/binding/database/DESIGN.md): optimistic commit validation policy,
  per-database lock buckets, concurrent commit threads, and backup table verification.
- [Transaction abort](src/binding/transaction/DESIGN.md): abort drains in-flight async gets before `Rollback()` and holds an `OperationGuard` so teardown cannot close the transaction under it.
- [N-API error builders](src/binding/napi/DESIGN.md): `createRocksDBError`/`createJSError` always write their out-param, even when their own N-API calls fail.
