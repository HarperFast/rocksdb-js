# Design notes

- [Transaction-log mapping capacity](src/binding/transaction_log/DESIGN.md) — fixed reader capacity, first-append publication, and rotation.
- [Transaction-log retention](src/binding/transaction_log/DESIGN.md#retention-and-the-sequence-witness) — purging the current segment, and `txn.state` as the restart sequence witness.
- [Transaction-log conditional close](src/binding/transaction_log/DESIGN.md#conditional-close) — why `tryClose()` refuses when any transaction bound during its check.
- [Transaction-log sync failure](src/binding/transaction_log/DESIGN.md#a-failed-sync-freezes-the-flushed-state) — a failed segment sync stops `txn.state` from advancing for the life of the store instance.
- [Transaction-log exact-start lookups](src/binding/transaction_log/DESIGN.md#exact-start-lookups) — the skip span an exact-start lookup jumps over, and what it must never cross.
- [Transaction-log aborted batches](src/binding/transaction_log/DESIGN.md#aborted-batches) — readers skip a batch whose first entry carries the abort flag (0x02).
- [Transaction-log abort markers](src/binding/transaction_log/DESIGN.md#abort-markers) — a logged transaction that ends without committing gets its batch marked, synced, before its position is released; failures fail closed.
- [Write-intent refusal](src/binding/transaction/DESIGN.md#write-intent-refusal) — with abort markers and `transactionLogWriteIntents` on, an attempt behind an older write intent is refused before it logs.
- [Verification table](src/binding/core/DESIGN.md): slots store versions encoded with a per-key tag; a park cancels its lock wake registration when it ends.
- [Database resources](src/binding/database/DESIGN.md): optimistic commit validation policy,
  per-database lock buckets, concurrent commit threads, backup table verification, why
  pending lock/park wake TSFNs stay ref'd, and the WriteBufferManager flush-policy and arena-size
  defaults.
- [Transaction abort](src/binding/transaction/DESIGN.md): abort drains in-flight async gets before `Rollback()` and holds an `OperationGuard` so teardown cannot close the transaction under it.
- [Pooled transaction iterators](src/binding/iterator/DESIGN.md): transaction `getRange()` re-seeks an idle native iterator after rewriting the read-bound Slices it points to.
- [N-API error builders](src/binding/napi/DESIGN.md): `createRocksDBError`/`createJSError` always write their out-param, even when their own N-API calls fail.
