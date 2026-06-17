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
