# Iterator design notes

## Pooled transaction iterators

A transaction-backed `getRange()` (`txn.getRange()` or `db.getRange({ transaction })`) borrows an
idle native iterator from its transaction instead of building a RocksDB iterator per call. Invariant:
a reused iterator returns the same rows, order and errors that a fresh iterator would for the same
options in the same transaction at that moment. `test/iterator-pool.test.ts` compares every case
against a fresh iterator and asserts a real pool hit.

Why each rule holds, and where it is enforced:

- **Eligibility** (`TransactionHandle::acquireIterator`): only with a fixed snapshot (`snapshotSet &&
  !disableSnapshot`), no advanced ReadOptions, and at most 64 staged deletes (see below). Without a snapshot reads see the latest commit,
  which a reused iterator would freeze. Database-level `getRange()` has no snapshot contract and is
  not pooled.
- **Writes after creation are visible**: a RocksDB transaction iterator re-seeked after more writes
  to the transaction sees them (puts, overwrites, deletes, batch growth). Pinned for the linked
  RocksDB by `test/native/transaction_iterator_reuse_test.cc`; pooling compiles out below RocksDB 11
  (`iteratorPoolEnabled`). No savepoints are used: `RollbackToSavePoint` rebuilds the batch index
  under live iterators.
- **No RocksDB bounds; the handle enforces them** (`DBIteratorHandle::valid`, `seek`): one iterator
  serves ranges with different bounds. Two consequences follow. Running past the end of a range can
  read data a bounded iterator never would, so the pooled iterator is created with
  `max_skippable_internal_keys = 64`, and any non-OK status (that limit's `Incomplete`, or a read
  error past the range) switches the handle to a fresh bounded iterator for the rest of the range
  (`fallBackToBounded`). The caller then sees what the bounded iterator reports. The test pins that a
  transaction iterator fails closed at the skip limit rather than yielding staged keys beyond it.
  Costs a bounded iterator would not pay: a probe followed by a run of deleted keys pays the old
  construction cost plus 64 skips; a range ending at a block or file boundary reads the next block
  or opens the next file (unmeasured). Staged deletes in the transaction's own batch are not counted
  by the skip limit, so a transaction with more than 64 of them is not pooled. The resume point is
  the key just copied out to JS (`advanceFrom`: the shared key buffer, or the slow path's Buffer),
  so rows pay no extra copy.
- **Matching** is by caller `DBHandle` (it also selects the shared key/value buffers `Next` writes
  into) and by column-family generation: a `weak_ptr` to the `ColumnFamilyDescriptor`, compared under
  the constructor's `OperationGuard`. A drop and same-name recreate behind one reopened `DBHandle`
  must not serve the old generation. Weak, because a strong pin can destroy a column-family handle
  after its database (invariant 24's crash).
- **Ownership**: idle handles sit in 4 inline slots on the `TransactionHandle` (no allocation when
  returning one) and stay registered in `activeIterators`, so every `closeIterators()` site closes
  them; `closeIterators()` then empties the slots, which breaks the handle-to-transaction reference
  cycle. `activeIteratorCount` counts busy handles only, so an idle one never blocks orphan close.
  A pooled handle is attached to the descriptor's closables once, when created, keeping the
  teardown barrier a foreign `destroy()`/`shutdown()` relies on, and is detached after its real close.
  Because a transaction's close detaches other closables, the descriptor's close sweep re-reads the
  map from `begin()` after every `close()` instead of keeping an iterator across it.
- **Leaving the JS iterator**: on exhaustion, `return()` or `throw()` the handle goes back to the
  pool and the `NativeIterator` drops it, so a retained JS iterator can never drive a handle lent to
  another range. An exhausted one keeps reporting done until the transaction's `iteratorEpoch` moves
  (every `closeIterators()`), then throws "Iterator not initialized" as a closed iterator always has.
  One history differs from an unpooled iterator: an exhausted iterator no longer defers the orphan
  close of a transaction whose JS wrapper was collected, so re-polling it afterwards throws.
- **Locks**: `iteratorMutex -> iteratorsMutex` (as `closeIfOpen` already nests). Acquire takes the
  slot under `iteratorsMutex`, drops it, then takes `iteratorMutex` to re-seek. Orphan close runs
  after `iteratorMutex` is released, because it closes every registered handle, including this one.
- **Retention** (`transactionsToTrim` in `src/store.ts`): an idle handle pins its RocksDB
  SuperVersion, i.e. memtables charged to the WriteBufferManager and SST files compaction already
  replaced. A request transaction can stay open across I/O for seconds, so the JS layer empties the
  pool (`NativeTransaction.trimIterators()`) from a `setImmediate` once the macrotask that ran the
  ranges ends. A handle that was busy across that trim (a range consumed across an `await`) is
  closed when released, not pooled (`trimGeneration`). Reuse therefore spans one synchronous run of
  ranges, which is the pattern it targets.

`ROCKSDB_JS_ITERATOR_POOL=0` disables pooling (read once per process).
