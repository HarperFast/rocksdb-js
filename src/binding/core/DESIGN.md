# Verification table slot encoding

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
