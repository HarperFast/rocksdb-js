// Deterministic coverage for the Verification Table's lock-free cold-populate
// race defense (Proposal 2). The hot race — a writer settling a slot between a
// reader's observation and its populate CAS — can't be reproduced reliably from
// the N-API/JS layer, so we drive the primitives directly here.

#include <gtest/gtest.h>
#include "core/verification_table.h"
#include "rocksdb/slice.h"

using namespace rocksdb_js;

namespace {
// Realistic positive-float64 version bit patterns (sign bit clear).
constexpr uint64_t kV1 = 0x4278bcfe56800000ULL;
constexpr uint64_t kV2 = 0x4278bcfe56900000ULL;
}  // namespace

// A cold populate is a single CAS from the value observed before the read; it
// succeeds when nothing changed the slot in the meantime.
TEST(VerificationTable, ColdPopulateSucceedsWhenSlotUnchanged) {
	VerificationTable vt(8, 0xABCD);
	auto* slot = vt.slotFor(0x1, 0, rocksdb::Slice("k"));
	ASSERT_NE(slot, nullptr);

	uint64_t observed = slot->load();
	EXPECT_EQ(observed, 0u);
	EXPECT_TRUE(VerificationTable::populateVersionIfUnchanged(slot, observed, kV1));
	EXPECT_TRUE(VerificationTable::verifyVersion(slot, kV1));
}

// If a full write cycle settles between the reader's observation and its CAS,
// the CAS must fail — no stale version is published. This is the ABA defense:
// settling moves the slot to a fresh generation, never back to the observed 0.
TEST(VerificationTable, ColdPopulateLosesToInterveningWriteCycle) {
	VerificationTable vt(8, 0xABCD);
	auto* slot = vt.slotFor(0x1, 0, rocksdb::Slice("k"));
	ASSERT_NE(slot, nullptr);

	uint64_t observed = slot->load();  // 0, "empty"

	// A concurrent writer locks the slot and then settles it.
	LockTracker* t = vt.lockSlotForWrite(slot, 0x1);
	ASSERT_NE(t, nullptr);
	vt.releaseWriteIntent(slot, t);

	// The stale reader's CAS from the pre-write value must fail.
	EXPECT_FALSE(VerificationTable::populateVersionIfUnchanged(slot, observed, kV1));
	EXPECT_FALSE(VerificationTable::verifyVersion(slot, kV1));
	EXPECT_TRUE(vtIsSettled(slot->load()));
}

// A settled slot never returns to 0, and successive settles carry distinct
// generations — which is exactly what makes the empty state non-ABA-able.
TEST(VerificationTable, SettleUsesDistinctNonZeroGenerations) {
	VerificationTable vt(8, 0xABCD);
	auto* slot = vt.slotFor(0x1, 0, rocksdb::Slice("k"));
	ASSERT_NE(slot, nullptr);

	LockTracker* t1 = vt.lockSlotForWrite(slot, 0x1);
	vt.releaseWriteIntent(slot, t1);
	uint64_t s1 = slot->load();
	EXPECT_NE(s1, 0u);
	EXPECT_TRUE(vtIsSettled(s1));
	EXPECT_FALSE(vtIsVersion(s1));
	EXPECT_FALSE(vtIsLock(s1));

	LockTracker* t2 = vt.lockSlotForWrite(slot, 0x1);
	vt.releaseWriteIntent(slot, t2);
	uint64_t s2 = slot->load();
	EXPECT_TRUE(vtIsSettled(s2));
	EXPECT_NE(s1, s2);  // distinct generations
}

// A populate must never overwrite a lock (a write is in flight on the slot).
TEST(VerificationTable, ColdPopulateNeverOverwritesLock) {
	VerificationTable vt(8, 0xABCD);
	auto* slot = vt.slotFor(0x1, 0, rocksdb::Slice("k"));
	ASSERT_NE(slot, nullptr);

	LockTracker* t = vt.lockSlotForWrite(slot, 0x1);
	uint64_t lockVal = slot->load();
	EXPECT_TRUE(vtIsLock(lockVal));

	// Even if `observed` happened to equal the lock value, never publish over it.
	EXPECT_FALSE(VerificationTable::populateVersionIfUnchanged(slot, lockVal, kV1));
	EXPECT_TRUE(vtIsLock(slot->load()));

	vt.releaseWriteIntent(slot, t);  // cleanup
}

// Overwrite-via-explicit-primitive after a re-observe still works: a reader that
// observes the settled state and re-reads can publish the current version.
TEST(VerificationTable, ColdPopulateSucceedsFromSettledWhenReobserved) {
	VerificationTable vt(8, 0xABCD);
	auto* slot = vt.slotFor(0x1, 0, rocksdb::Slice("k"));
	ASSERT_NE(slot, nullptr);

	LockTracker* t = vt.lockSlotForWrite(slot, 0x1);
	vt.releaseWriteIntent(slot, t);
	uint64_t settled = slot->load();
	ASSERT_TRUE(vtIsSettled(settled));

	// Re-observe the settled value, then publish — succeeds (no intervening write).
	EXPECT_TRUE(VerificationTable::populateVersionIfUnchanged(slot, settled, kV2));
	EXPECT_TRUE(VerificationTable::verifyVersion(slot, kV2));
}

// Cross-incarnation isolation (HarperFast/harper#1864). A slot is addressed by
// (dbId, cfId, key) where dbId is DBDescriptor::vtEpoch — a process-unique per-open
// value. A later in-process reopen of the same path gets a fresh epoch, so even
// though the reused descriptor address and (stable) cfId could be identical, the
// new incarnation addresses a different, cold slot and can never observe the prior
// incarnation's cached version (which previously surfaced as a spurious FRESH hit
// resolving present keys as stale/absent). Deterministic: skip the rare hash
// collision and assert on the first independent slot, which is found immediately.
TEST(VerificationTable, DistinctDbEpochsAddressIndependentSlots) {
	VerificationTable vt(1 << 12, 0xABCD);
	rocksdb::Slice key("record-key");
	const uint64_t epochOld = 1;

	auto* slotOld = vt.slotFor(epochOld, 0, key);
	ASSERT_NE(slotOld, nullptr);
	ASSERT_TRUE(VerificationTable::populateVersion(slotOld, kV1));
	ASSERT_TRUE(VerificationTable::verifyVersion(slotOld, kV1));

	for (uint64_t epochNew = 2; epochNew < 100; ++epochNew) {
		auto* slotNew = vt.slotFor(epochNew, 0, key);
		if (slotNew == slotOld) continue;  // rare hash collision — try the next epoch
		EXPECT_FALSE(VerificationTable::verifyVersion(slotNew, kV1))
			<< "epoch " << epochNew << " must not see a prior incarnation's version";
		EXPECT_EQ(slotNew->load(), 0u);  // fresh/cold, not a leaked version
		return;
	}
	FAIL() << "expected at least one distinct slot across epochs";
}

// The same (dbId, cfId, key) is stable: a live incarnation keeps addressing its
// own slot for the life of the open.
TEST(VerificationTable, SameDbEpochIsStable) {
	VerificationTable vt(1 << 12, 0xABCD);
	rocksdb::Slice key("record-key");
	EXPECT_EQ(vt.slotFor(7, 3, key), vt.slotFor(7, 3, key));
}

// Encoding classes (version / lock / settled / empty) are mutually exclusive.
TEST(VerificationTable, EncodingClassesAreDisjoint) {
	EXPECT_TRUE(vtIsVersion(kV1));
	EXPECT_FALSE(vtIsLock(kV1));
	EXPECT_FALSE(vtIsSettled(kV1));

	uint64_t settled = vtEncodeSettled(12345);
	EXPECT_TRUE(vtIsSettled(settled));
	EXPECT_FALSE(vtIsVersion(settled));
	EXPECT_FALSE(vtIsLock(settled));

	EXPECT_FALSE(vtIsVersion(0));
	EXPECT_FALSE(vtIsLock(0));
	EXPECT_FALSE(vtIsSettled(0));
}
