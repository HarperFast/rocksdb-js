# Design notes

- [Transaction-log mapping capacity](src/binding/transaction_log/DESIGN.md) — fixed reader capacity, first-append publication, and rotation.
- [Transaction-log retention](src/binding/transaction_log/DESIGN.md#retention-and-the-sequence-witness) — purging the current segment, and `txn.state` as the restart sequence witness.
- [Transaction-log purge watermark](src/binding/transaction_log/DESIGN.md#purge-watermark) — `txn.purged` covers every segment before it is unlinked, and is reported as known only when every deletion was recorded.
- [Transaction-log purge baseline](src/binding/transaction_log/DESIGN.md#purge-baseline) — a store older than its record becomes known only through a caller's baseline, while the record's `tracked` flag proves every later deletion was recorded.
- [Transaction-log conditional close](src/binding/transaction_log/DESIGN.md#conditional-close) — why `tryClose()` refuses when any transaction bound during its check.
- [Transaction-log sync failure](src/binding/transaction_log/DESIGN.md#a-failed-sync-freezes-the-flushed-state) — a failed segment sync stops `txn.state` from advancing for the life of the store instance.
- [Transaction-log exact-start lookups](src/binding/transaction_log/DESIGN.md#exact-start-lookups) — the skip span an exact-start lookup jumps over, and what it must never cross.
- [Transaction-log iterator handoff](src/binding/transaction_log/DESIGN.md#iterator-handoff) — physical bounds, peeked entries, and proving catch-up before reporting done.
- [Transaction-log aborted batches](src/binding/transaction_log/DESIGN.md#aborted-batches) — readers skip a batch whose first entry carries the abort flag (0x02).
- [Transaction-log abort markers](src/binding/transaction_log/DESIGN.md#abort-markers) — a logged transaction that ends without committing gets its batch marked, synced, before its position is released; failures fail closed.
- [Write-intent refusal](src/binding/transaction/DESIGN.md#write-intent-refusal) — with abort markers and `transactionLogWriteIntents` on, an attempt behind an older write intent is refused before it logs.
- [Conflict check before the log write](src/binding/transaction/DESIGN.md#conflict-check-before-the-log-write) — with abort markers on, RocksDB's own conflict check refuses a stale optimistic attempt before it logs.
- [Verification table](src/binding/core/DESIGN.md): slots store versions encoded with a per-key tag; only an opted-in handle publishes from a read, and its open creates the table before it can write; a park cancels its lock wake registration when it ends.
- [Database resources](src/binding/database/DESIGN.md): optimistic commit validation policy,
  per-database lock buckets, concurrent commit threads, backup table verification, why
  pending lock/park wake TSFNs stay ref'd, the WriteBufferManager flush-policy and arena-size
  defaults, SST filters with where their blocks live, and why close cancels compaction instead of
  waiting for it.
- [Transaction state transitions](src/binding/transaction/DESIGN.md#state-transitions): `Committed` and `Aborted` are terminal; every change is a compare-exchange, because `close()` can settle `Aborted` from another thread.
- [Transaction abort](src/binding/transaction/DESIGN.md): abort ends the transaction with `close()`, which drains in-flight async gets first; with gets in flight it holds an `OperationGuard` and defers to a teardown already under way.
- [Transaction close ownership](src/binding/transaction/DESIGN.md#close-ownership): a closing transaction stays registered until `txn` is deleted and a second `close()` waits for the first, so teardown's sweep finds it and its own `close()` waits; it leaves the registry through a non-owning link, so no descriptor pin can skip a purge.
- [Pooled transaction iterators](src/binding/iterator/DESIGN.md): transaction `getRange()` re-seeks an idle native iterator after rewriting the read-bound Slices it points to.
- [N-API error builders](src/binding/napi/DESIGN.md): `createRocksDBError`/`createJSError` always write their out-param, even when their own N-API calls fail.
- [C++ exceptions in async work](src/binding/napi/DESIGN.md#c-exceptions-in-async-work): async entry points, execute and complete callbacks turn an exception into a rejection, and what each wrapped entry must own for that to be safe.
