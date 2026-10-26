# Iterator design notes

## Pooled transaction iterators

A transaction-backed `getRange()` (`txn.getRange()` or `db.getRange({ transaction })`) borrows an
idle native iterator from its transaction instead of building a RocksDB iterator per call. Invariant:
a reused iterator returns the same rows, order and errors that a fresh iterator would for the same
options in the same transaction at that moment. `test/iterator-pool.test.ts` compares every case
against a fresh iterator and asserts a real pool hit.

Why each rule holds, and where it is enforced:

- **Eligibility** (`TransactionHandle::acquireIterator`): only with a fixed snapshot (`snapshotSet &&
  !disableSnapshot`), no advanced ReadOptions, and no stalling WriteBufferManager. Without a
  snapshot reads see the latest commit, which a reused iterator would freeze. Database-level
  `getRange()` has no snapshot contract and is not pooled. With `writeBufferManagerAllowStall`, a
  synchronous write can block the JS thread until memory is released, and the trim that releases
  idle iterators' pinned memtables (below) runs on that same thread. So turning stalls on publishes
  the flag (no new pooling, no re-pooling on release), closes every idle pooled iterator in the
  process (`DBRegistry::CloseIdlePooledIterators`), and only then lets the manager stall
  (`DBSettings` config). A non-stalling manager (Harper's default) cannot block a writer; it only
  lets usage run over budget until the macrotask ends.
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
  `clear()` counts itself as running in `clearEpoch` _before_ `DeleteFilesInRange` installs the
  post-delete version, and finishes after: a range on another thread that already sees the
  deletion through a fresh iterator must not then reuse an older one
  (`test/fixtures/fork-pooled-iterator-clear-race.mts`, which uses `ROCKSDB_JS_CLEAR_DELAY_MS` to
  hold that window open). An iterator built while a clear runs is never reused. A `dropSync()` on
  the same open handle leaves its pooled iterators reusable on purpose: RocksDB keeps a dropped
  family readable through its retained handle (invariant 24), so a fresh iterator returns the same
  rows. That is a RocksDB behavior, pinned by the `iterator-pool` test "returns what a fresh
  iterator does after its column family is dropped"; if it changes, `readsColumnOf` must also
  refuse a retired family.
- **Ownership**: idle handles sit in 4 inline slots on the `TransactionHandle` (no allocation when
  returning one) and stay registered in `activeIterators`, so every `closeIterators()` site closes
  them; `closeIterators()` then empties the slots, which breaks the handle-to-transaction reference
  cycle. `activeIteratorCount` counts busy handles only, so an idle one never blocks orphan close.
  A pooled handle is attached to the descriptor's closables once, when created, keeping the
  teardown barrier a foreign `destroy()`/`shutdown()` relies on, and is detached after its real close.
  Every iterator detaches inside `closeIfOpen()`, after resetting its RocksDB iterator, through the
  descriptor pointer recorded at attach (never `DBHandle::descriptor`, which only the owner thread
  may read, and never a locked `weak_ptr`, whose transient reference can make a racing last-handle
  close skip its purge). The raw pointer is safe because it is only read while the iterator is
  still open, and a descriptor is never released with an attached iterator still open: its close
  sweep closes every attached handle under that handle's `iteratorMutex`, and a descriptor
  quarantined by a failed close (whose sweep never ran) gets the same sweep from
  `DBRegistry::Teardown` before it is released at exit.
  Because a transaction's close detaches other closables, the descriptor's close sweep re-reads the
  map from `begin()` after every `close()` instead of keeping an iterator across it
  (`test/fixtures/fork-pooled-iterator-shutdown.mts` segfaults without that).
- **Leaving the JS iterator**: on exhaustion, `return()` or `throw()` the handle goes back to the
  pool (`ITERATOR_RESULT_RELEASED` / `return()` → true) and the `NativeIterator` drops it, so a
  retained JS iterator can never drive a handle lent to another range. Terminal behavior matches an
  unpooled iterator, which stayed registered until collected: an exhausted one keeps reporting done
  until the transaction's `iteratorEpoch` moves (every `closeIterators()`), then throws "Iterator not
  initialized". The JS `DBIterator` keeps a reference to its native transaction after exhaustion
  (as the registered unpooled iterator deferred the orphan close of a dropped transaction) and
  drops it on `return()`/`throw()` (after which an orphan close could already proceed).
- **Locks**: `iteratorMutex -> iteratorsMutex` (as `closeIfOpen` already nests). Acquire takes the
  slot under `iteratorsMutex`, drops it, then takes `iteratorMutex` to re-seek. Orphan close runs
  after `iteratorMutex` is released, because it closes every registered handle, including this one.
- **Retention** (`scheduleTrim` in `src/dbi-iterator.ts`): an idle handle pins its RocksDB
  SuperVersion, i.e. memtables charged to the WriteBufferManager and SST files compaction already
  replaced. A request transaction can stay open across I/O for seconds, so when a handle goes back
  to the pool the JS iterator queues its transaction, and a `setImmediate` empties those pools
  (`NativeTransaction.trimIterators()`) once the macrotask ends. A handle that was busy across a trim
  (a range consumed across an `await` while others released) is closed when released, not pooled
  (`trimGeneration`). Reuse therefore spans one synchronous run of ranges, which is the pattern it
  targets; a transaction that runs one range per macrotask gets no reuse and measured no slower.

`ROCKSDB_JS_ITERATOR_POOL=0` disables pooling (read once per process).
