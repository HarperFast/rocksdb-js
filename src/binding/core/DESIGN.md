# Verification table

## Slot encoding

A slot is addressed by `hash(dbEpoch, cfId, key) & mask`, so unrelated keys share slots. Lock and
settled-empty values in a shared slot only cause misses, but a version must identify its key: a
producer that stamps one version per transaction gives every key in it the same version, so a plain
version left in a shared slot by key B would answer FRESH for key A after A was rewritten: a stale
read.

`VerificationTable::slotRefFor()` therefore returns the slot together with a 63-bit tag taken from
the same hash (`h >> 1`; keys sharing a slot share only its index bits, leaving 47 independent bits at
128K slots), and every version that is compared with or stored into a slot goes through
`vtEncodeVersion(version, tag)` (`version ^ tag`, bit 63 kept clear). Callers hold a `VtSlotRef` on
every verify and populate path; the bare-pointer primitives are named `*Encoded` so a plain version
is not passed to them by mistake. Lock and settle operations work on `slotFor()`'s bare pointer because
they never compare versions. An
encoding of 0 is unpublishable and never matches.

`test/verification-table-collision.test.ts` reproduces the stale read with a 16-slot table, and
`test/native/verification_table_test.cc` covers the encoding directly.

## Who may publish

A slot may hold a version only while every write of its key invalidates the slot. Two rules keep it:

- **Only a participating column family publishes from a read.** Writes lock a slot only on a column
  family whose `VerificationParticipation` guards writes, and reads publish only once it is
  `Publishing` (`DBHandle::verificationGates()`, passed to `vtPopulateIfSettled()` and stored in
  `AsyncGetState::vtPublish`). The explicit `populateVersion()` primitive is not gated on a
  non-participating column family; its caller owns that contract.
- **The table exists before an opted-in handle can write.** Writers never create it:
  `TransactionHandle::lockVTSlot()` and `Database::PutSync` record no write intent when it is
  absent, and a read that created it later could publish the committed version that an earlier
  staged write's commit never invalidates. `Database::Open` creates it for an opted-in handle
  (`test/fixtures/fork-vt-cold-populate.mts`, `staged-write`).

## Wake registrations

A coordinated-retry park registers a callback on the conflicting holder's `LockTracker`, and the
park can end without that holder releasing: by timeout, by its env exiting, or by its database
closing. `addWakeCallback()` therefore returns a `LockTracker::WakeRegistration` that the park's
`ParkTimeoutRegistry` entry owns and cancels as it ends, so the callbacks registered on a lock are
the parks still waiting on it. `test/fixtures/fork-park-wake-registration.mts` covers each ending;
`test/native/verification_table_test.cc` covers the list itself.

- **The registration holds its list, never the tracker.** The park releases its tracker reference
  right after registering, and dropping one is `unrefTracker()`, which takes the global
  `writerMutex_` that `wake()` already runs under. The list is a separate `shared_ptr` allocation
  that outlives the tracker while any registration holds it, so a registration's iterator stays
  valid until it lets go: only its own `cancel()` erases its node.
- **It is created by the first registration, not with the tracker.** `lockSlotForWrite()` installs
  a tracker under `writerMutex_` on every transactional write; only contended locks get a list.
- **Cancelling cannot recall a claimed callback.** `wake()` marks the list drained under its mutex,
  then runs the callbacks in place, unlocked; once drained, nothing else modifies the list. A
  cancel that loses that race leaves its node alone and the callback runs anyway, which is why the
  park's closure keeps its weak references and exactly-once gate.
- **`wake()` does not allocate.** Callers hold `writerMutex_`, and `releaseWriteIntent()` runs after
  the releasing transaction's commit has landed, so a throw there has no recovery. Running the
  callbacks in place is what avoids a second container (an empty `std::list` allocates a sentinel
  on MSVC).
- **Lock order:** `wakeCallbacksMutex -> WakeList::mutex` when adding, and the registry's mutex
  `-> WakeList::mutex` when a park cancels. Nothing is called while a list mutex is held, so it is
  a leaf.
- **A park cancels before it resolves.** `ParkTimeoutRegistry::resolve()` cancels before calling
  the TSFN, so JavaScript that observes a park's result never sees it still registered.

## Participation is per column family

Slots are keyed by database epoch and column family, not by handle, and outlive the handle that
seeded them, so participation is one `VerificationParticipation` per column-family generation
(`ColumnFamilyDescriptor::participation`), which every handle on it reads. `DBRegistry::OpenDB`
changes it:

- **A later handle inherits participation**, for as long as the generation lives in that descriptor:
  a slot a closed handle seeded can still be trusted.
- **An explicit opt-out ends participation once no handle is attached**, and settles every slot,
  since its writes skip the slots the participants seeded. Before that, a `verificationTable: false`
  open is refused with `ERR_VERIFICATION_TABLE_CONFLICT`.
- **A late opt-in converts the column family in place**, unless a handle opened with an explicit
  `false` is attached (refused, as above). Writes through every handle take write intents from the
  moment of the opt-in, but a write a non-participating handle staged before it holds none, so the
  table stays inert for that column family (no publish, no answer from a slot, no explicit seed) until
  `DBDescriptor::activateVerification()` finds no registered transaction with an id up to the
  watermark taken at the opt-in still open, and no operation besides the caller's own in flight (a
  closed transaction's commit, and a non-transactional write, hold one). Every verification-table
  access through a handle tries it, under an `OperationGuard` of its own; after a scan that found
  such a transaction, the next scan waits for any transaction in the process to close
  (`transactionCloseCount()`), or 100 ms. The watermark is an RMW on
  `nextTransactionId` after the guard is published, so a transaction allocated later sees the guard.
  A transaction that existed at the opt-in therefore never sees a verified read of that column
  family. `convertToVerification()` settles every slot first: a non-participating handle could have
  seeded one (`populateVersion()`) that its own writes left stale. A column family no
  non-participating handle ever attached to publishes at once.
- **Publish eligibility is decided before the slot is observed**, never after the read: a write
  without an intent leaves the settled generation a read observed unchanged when it commits, so a
  read that started while the column family converted must not publish once it activates.
  An explicit `populateVersion()` CASes from the slot value it observed before reading the mode, so
  a conversion that settles the slot in between defeats it.

`test/verification-table.test.ts` (`participation is per column family`) covers these;
`test/fixtures/fork-vt-late-opt-in.mts` covers a read that crosses activation.
