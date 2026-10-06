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

## Wake registrations

A coordinated-retry park registers a callback on the conflicting holder's `LockTracker`, and the
park can end without that holder releasing: by timeout, by its env exiting, or by its database
closing. `addWakeCallback()` therefore returns a `LockTracker::WakeRegistration` that the park's
`ParkTimeoutRegistry` entry owns and cancels as it ends, so the callbacks registered on a lock are
the parks still waiting on it. `test/fixtures/fork-park-wake-registration.mts` covers each ending;
`test/native/verification_table_test.cc` covers the list itself.

- **The registration holds its list weakly, never the tracker.** The park releases its tracker
  reference right after registering, and dropping one is `unrefTracker()`, which takes the global
  `writerMutex_` that `wake()` already runs under. The list is a separate `shared_ptr` allocation,
  so cancelling after the tracker is freed is a no-op instead of a use-after-free.
- **It is created by the first registration, not with the tracker.** `lockSlotForWrite()` installs
  a tracker under `writerMutex_` on every transactional write; only contended locks get a list.
- **Cancelling cannot recall a detached callback.** `wake()` marks the list drained and swaps the
  callbacks into its own batch under the list mutex, then runs them unlocked. A cancel that loses
  that race leaves its iterator alone (it now belongs to the batch) and the callback runs anyway,
  which is why the park's closure keeps its weak references and exactly-once gate.
- **Lock order:** `wakeCallbacksMutex -> WakeList::mutex` when adding, and the registry's mutex
  `-> WakeList::mutex` when a park cancels. Nothing is called while a list mutex is held, so it is
  a leaf.
- **A park cancels before it resolves.** `ParkTimeoutRegistry::resolve()` cancels before calling
  the TSFN, so JavaScript that observes a park's result never sees it still registered.
