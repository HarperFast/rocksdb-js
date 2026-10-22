# Iterator design notes

## Pooled transaction iterators

A transaction-backed `getRange()` (`txn.getRange()` or `db.getRange({ transaction })`) borrows an
idle native iterator from its transaction instead of building a RocksDB iterator per call. Invariant:
a reused iterator returns the same rows, order and errors that a fresh iterator would for the same
options in the same transaction at that moment. `test/iterator-pool.test.ts` compares every case
against a fresh iterator and asserts a real pool hit.

Why each rule holds, and where it is enforced:

- **Eligibility** (`TransactionHandle::acquireIterator`): only with a fixed snapshot (`snapshotSet &&
  !disableSnapshot`) and no advanced ReadOptions. Without a snapshot reads see the latest commit,
  which a reused iterator would freeze. Database-level `getRange()` has no snapshot contract and is
  not pooled.
- **Retargeting keeps RocksDB's own bounds** (`DBIteratorHandle::reuse`): the iterator's
  `ReadOptions` hold pointers to the handle's `startKey`/`endKey` Slices, and RocksDB re-reads bound
  pointers on every seek (upstream `DBIteratorTest.IterateBoundChangedBeforeSeek`;
  `BlockBasedTableIterator::SeekImpl` recomputes its cached block bound check on reseek). Reuse
  rewrites those Slices in place and re-seeks, so a reused range stops, reads ahead and reports
  errors exactly like a bounded fresh one. A bound's _presence_ is fixed for an iterator's life, so
  an idle handle only serves ranges with the same lower/upper presence (`boundsMatch`).
  `test/native/transaction_iterator_reuse_test.cc` pins both RocksDB behaviors this needs for the
  transaction iterator (later writes visible after a reseek; retargeted bounds equal fresh ones
  across blocks, files and levels); pooling compiles out below RocksDB 11 (`iteratorPoolEnabled`).
  No savepoints are used: `RollbackToSavePoint` rebuilds the batch index under live iterators.
- **Matching** is by caller `DBHandle` (it also selects the shared key/value buffers `Next` writes
  into), bound presence, and column-family state: a `weak_ptr` to the `ColumnFamilyDescriptor`
  (compared by control block under the constructor's `OperationGuard`) and its `clearEpoch`. A drop
  and same-name recreate behind one reopened `DBHandle`, or a `clear()` (which deletes the family's
  files outright, unseen by an iterator that pinned them), must not serve old rows. Weak, because a
  strong pin can destroy a column-family handle after its database (invariant 24's crash).
- **Ownership**: idle handles sit in 4 inline slots on the `TransactionHandle` (no allocation when
  returning one) and stay registered in `activeIterators`, so every `closeIterators()` site closes
  them; `closeIterators()` then empties the slots, which breaks the handle-to-transaction reference
  cycle. `activeIteratorCount` counts busy handles only, so an idle one never blocks orphan close.
  A pooled handle is attached to the descriptor's closables once, when created, keeping the
  teardown barrier a foreign `destroy()`/`shutdown()` relies on, and is detached after its real close.
  Because a transaction's close detaches other closables, the descriptor's close sweep re-reads the
  map from `begin()` after every `close()` instead of keeping an iterator across it
  (`test/fixtures/fork-pooled-iterator-shutdown.mts` segfaults without that).
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
  ranges, which is the pattern it targets; a transaction that runs one range per macrotask gets no
  reuse and measured no slower.

`ROCKSDB_JS_ITERATOR_POOL=0` disables pooling (read once per process).
