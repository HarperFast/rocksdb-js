// Deterministic coverage for the Verification Table's lock-free cold-populate
// race defense (Proposal 2). The hot race — a writer settling a slot between a
// reader's observation and its populate CAS — can't be reproduced reliably from
// the N-API/JS layer, so we drive the primitives directly here.

#include <gtest/gtest.h>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>
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
	EXPECT_TRUE(VerificationTable::populateEncodedIfUnchanged(slot, observed, kV1));
	EXPECT_TRUE(VerificationTable::verifyEncoded(slot, kV1));
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
	EXPECT_FALSE(VerificationTable::populateEncodedIfUnchanged(slot, observed, kV1));
	EXPECT_FALSE(VerificationTable::verifyEncoded(slot, kV1));
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
	EXPECT_FALSE(VerificationTable::populateEncodedIfUnchanged(slot, lockVal, kV1));
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
	EXPECT_TRUE(VerificationTable::populateEncodedIfUnchanged(slot, settled, kV2));
	EXPECT_TRUE(VerificationTable::verifyEncoded(slot, kV2));
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
	ASSERT_TRUE(VerificationTable::populateEncoded(slotOld, kV1));
	ASSERT_TRUE(VerificationTable::verifyEncoded(slotOld, kV1));

	for (uint64_t epochNew = 2; epochNew < 100; ++epochNew) {
		auto* slotNew = vt.slotFor(epochNew, 0, key);
		if (slotNew == slotOld) continue;  // rare hash collision — try the next epoch
		EXPECT_FALSE(VerificationTable::verifyEncoded(slotNew, kV1))
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

// The value-header predicate: the flag is only read from a word tagged as one, and every shape
// that is not one answers "unique", which is the permissive direction (caching keeps working for
// producers that write no header).
TEST(VerificationTable, ValueVersionIsNotUniqueReadsTaggedFlagOnly) {
	auto value = [](uint8_t tag, uint32_t flags, size_t size = 16) {
		std::string v(size, '\0');
		if (size >= 8) {
			for (int i = 0; i < 8; i++) v[i] = static_cast<char>((kV1 >> (56 - 8 * i)) & 0xFF);
		}
		if (size >= 12) {
			v[8] = static_cast<char>(tag);
			v[9] = static_cast<char>((flags >> 16) & 0xFF);
			v[10] = static_cast<char>((flags >> 8) & 0xFF);
			v[11] = static_cast<char>(flags & 0xFF);
		}
		return v;
	};

	const std::string marked = value(VERSION_HEADER_TAG, VERSION_NOT_UNIQUE_FLAG);
	EXPECT_TRUE(VerificationTable::valueVersionIsNotUnique(rocksdb::Slice(marked)));

	const std::string tagged = value(VERSION_HEADER_TAG, 0);
	EXPECT_FALSE(VerificationTable::valueVersionIsNotUnique(rocksdb::Slice(tagged)));

	// Other producer flags in the same word do not imply this one.
	const std::string otherFlags = value(VERSION_HEADER_TAG, 0x00FEFFFF & ~VERSION_NOT_UNIQUE_FLAG);
	EXPECT_FALSE(VerificationTable::valueVersionIsNotUnique(rocksdb::Slice(otherFlags)));

	// Same bit, untagged word: payload, not a header.
	const std::string untagged = value(0xAB, VERSION_NOT_UNIQUE_FLAG);
	EXPECT_FALSE(VerificationTable::valueVersionIsNotUnique(rocksdb::Slice(untagged)));

	// Too short to carry the word, and too short to carry a version at all.
	const std::string shortValue = value(VERSION_HEADER_TAG, VERSION_NOT_UNIQUE_FLAG, 11);
	EXPECT_FALSE(VerificationTable::valueVersionIsNotUnique(rocksdb::Slice(shortValue)));
	EXPECT_FALSE(VerificationTable::valueVersionIsNotUnique(rocksdb::Slice(std::string())));
}

// Keys that share a slot and a version must not vouch for each other: the slot stores the version
// encoded with each key's tag, so only the key that populated it verifies.
TEST(VerificationTable, CollidingKeysWithSameVersionDoNotVouchForEachOther) {
	VerificationTable vt(2, 0xABCD);
	const VtSlotRef a = vt.slotRefFor(0x1, 0, rocksdb::Slice("a"));
	VtSlotRef b;
	for (int i = 0; !b || b.slot != a.slot; ++i) {
		b = vt.slotRefFor(0x1, 0, rocksdb::Slice("b" + std::to_string(i)));
	}
	ASSERT_NE(a.keyTag, b.keyTag);

	EXPECT_TRUE(VerificationTable::populateVersion(a, kV1));
	EXPECT_TRUE(VerificationTable::verifyVersion(a, kV1));
	EXPECT_TRUE(a.holds(a.load(), kV1));
	EXPECT_FALSE(VerificationTable::verifyVersion(b, kV1));
	EXPECT_FALSE(b.holds(b.load(), kV1));
	EXPECT_FALSE(VerificationTable::verifyEncoded(a.slot, kV1));

	const uint64_t observed = b.load();
	EXPECT_TRUE(VerificationTable::populateVersionIfUnchanged(b, observed, kV1));
	EXPECT_TRUE(VerificationTable::verifyVersion(b, kV1));
	EXPECT_FALSE(VerificationTable::verifyVersion(a, kV1));
}

// A version equal to its key's tag would encode to 0, the never-written value, so it is neither
// published nor matched.
TEST(VerificationTable, VersionEqualToKeyTagIsNeverCached) {
	VerificationTable vt(8, 0xABCD);
	const VtSlotRef ref = vt.slotRefFor(0x1, 0, rocksdb::Slice("k"));
	ASSERT_TRUE(vtIsVersion(ref.keyTag));
	EXPECT_FALSE(VerificationTable::populateVersion(ref, ref.keyTag));
	EXPECT_FALSE(VerificationTable::verifyVersion(ref, ref.keyTag));
	EXPECT_FALSE(ref.holds(ref.load(), ref.keyTag));
}

// ---- LockTracker wake registrations ----
//
// A coordinated-retry park registers a wake callback on the conflicting holder's tracker and must
// remove it when the park ends without a wake (timeout, env teardown, close); otherwise a holder
// that never releases accumulates one callback per re-park. registeredWakeCallbacks() is
// process-wide, so each test asserts against the count it started with.

// N parks that each time out against one held lock leave nothing registered, and the eventual
// wake invokes none of them.
TEST(LockTrackerWake, CancelledParksAgainstHeldLockLeaveNothingRegistered) {
	VerificationTable vt(8, 0xABCD);
	auto* slot = vt.slotFor(0x1, 0, rocksdb::Slice("k"));
	const int64_t base = LockTracker::registeredWakeCallbacks();

	LockTracker* holder = vt.lockSlotForWrite(slot, 0x1);
	int invoked = 0;
	for (int i = 0; i < 16; ++i) {
		LockTracker* t = vt.refTrackerIfLocked(slot);
		ASSERT_EQ(t, holder);
		LockTracker::WakeRegistration registration = t->addWakeCallback([&invoked] { ++invoked; });
		vt.unrefTracker(t);
		ASSERT_TRUE(registration);
		EXPECT_EQ(LockTracker::registeredWakeCallbacks(), base + 1);
		EXPECT_TRUE(registration.cancel());
		EXPECT_EQ(LockTracker::registeredWakeCallbacks(), base);
	}

	vt.releaseWriteIntent(slot, holder);
	EXPECT_EQ(invoked, 0);
	EXPECT_EQ(LockTracker::registeredWakeCallbacks(), base);
}

// Live registrations are invoked exactly once, in registration order; a cancelled one is skipped.
TEST(LockTrackerWake, WakeInvokesLiveCallbacksInOrderOnce) {
	LockTracker t(0, 1, 0x1);
	const int64_t base = LockTracker::registeredWakeCallbacks();
	std::vector<int> order;
	auto a = t.addWakeCallback([&order] { order.push_back(0); });
	auto b = t.addWakeCallback([&order] { order.push_back(1); });
	auto c = t.addWakeCallback([&order] { order.push_back(2); });
	EXPECT_EQ(LockTracker::registeredWakeCallbacks(), base + 3);
	EXPECT_TRUE(b.cancel());

	t.wake();
	EXPECT_EQ(order, (std::vector<int>{0, 2}));
	EXPECT_EQ(LockTracker::registeredWakeCallbacks(), base);

	t.wake();
	EXPECT_EQ(order, (std::vector<int>{0, 2}));
	EXPECT_FALSE(a.cancel());
	EXPECT_FALSE(c.cancel());
	EXPECT_EQ(LockTracker::registeredWakeCallbacks(), base);
}

// A tracker that never had a waiter (the common case) wakes, and wakes again, without a list.
TEST(LockTrackerWake, WakeWithoutWaitersAndRepeatedWake) {
	LockTracker t(0, 1, 0x1);
	const int64_t base = LockTracker::registeredWakeCallbacks();
	t.wake();
	t.wake();
	bool invoked = false;
	auto late = t.addWakeCallback([&invoked] { invoked = true; });
	EXPECT_FALSE(late);
	EXPECT_FALSE(late.cancel());
	EXPECT_FALSE(invoked);
	EXPECT_EQ(LockTracker::registeredWakeCallbacks(), base);
}

// Cancelling cannot recall a callback wake() already detached: it still runs, and the cancel
// neither erases from wake()'s batch nor double-counts.
TEST(LockTrackerWake, CancelDuringWakeDoesNotRecallDetachedCallback) {
	LockTracker t(0, 1, 0x1);
	const int64_t base = LockTracker::registeredWakeCallbacks();
	LockTracker::WakeRegistration second;
	bool secondInvoked = false;
	bool cancelledDuringWake = true;
	auto first = t.addWakeCallback([&] { cancelledDuringWake = second.cancel(); });
	second = t.addWakeCallback([&secondInvoked] { secondInvoked = true; });

	t.wake();
	EXPECT_FALSE(cancelledDuringWake);
	EXPECT_TRUE(secondInvoked);
	EXPECT_EQ(LockTracker::registeredWakeCallbacks(), base);
}

// A registration outlives its tracker: cancelling after the tracker is freed is a safe no-op.
TEST(LockTrackerWake, CancelAfterTrackerFreed) {
	VerificationTable vt(8, 0xABCD);
	auto* slot = vt.slotFor(0x1, 0, rocksdb::Slice("k"));
	const int64_t base = LockTracker::registeredWakeCallbacks();
	LockTracker* holder = vt.lockSlotForWrite(slot, 0x1);
	bool invoked = false;
	auto registration = holder->addWakeCallback([&invoked] { invoked = true; });
	vt.releaseWriteIntent(slot, holder);  // wakes and frees the tracker
	EXPECT_TRUE(invoked);
	EXPECT_FALSE(registration.cancel());
	EXPECT_EQ(LockTracker::registeredWakeCallbacks(), base);
}

// Destroying a registration cancels it and releases what its callback captured.
TEST(LockTrackerWake, DestroyingRegistrationReleasesCallback) {
	LockTracker t(0, 1, 0x1);
	const int64_t base = LockTracker::registeredWakeCallbacks();
	auto captured = std::make_shared<int>(0);
	{
		auto registration = t.addWakeCallback([captured] { ++*captured; });
		EXPECT_EQ(captured.use_count(), 2);
	}
	EXPECT_EQ(captured.use_count(), 1);
	EXPECT_EQ(LockTracker::registeredWakeCallbacks(), base);
	t.wake();
	EXPECT_EQ(*captured, 0);
}

// Move-assigning over a live registration cancels the one it replaces; the moved-from
// registration no longer owns anything.
TEST(LockTrackerWake, MoveAssignmentCancelsReplacedRegistration) {
	LockTracker t(0, 1, 0x1);
	const int64_t base = LockTracker::registeredWakeCallbacks();
	std::vector<int> invoked;
	auto kept = t.addWakeCallback([&invoked] { invoked.push_back(0); });
	auto moved = t.addWakeCallback([&invoked] { invoked.push_back(1); });
	kept = std::move(moved);
	EXPECT_TRUE(kept);
	EXPECT_FALSE(moved);
	EXPECT_FALSE(moved.cancel());
	EXPECT_EQ(LockTracker::registeredWakeCallbacks(), base + 1);

	t.wake();
	EXPECT_EQ(invoked, (std::vector<int>{1}));
	EXPECT_EQ(LockTracker::registeredWakeCallbacks(), base);
}

// Registrations racing a wake: each is either removed by its cancel or invoked by the wake, never
// both and never neither, and the count returns to where it started. Each thread holds a batch of
// registrations before cancelling them, and the wake waits until half the adds are done, so a
// round exercises cancel-before-wake, wake-before-cancel, and add-after-wake together.
TEST(LockTrackerWake, CancelRacingWakeSettlesEachRegistrationOnce) {
	constexpr int kThreads = 4;
	constexpr int kBatches = 64;
	constexpr int kBatch = 16;
	constexpr int kPerThread = kBatches * kBatch;
	for (int round = 0; round < 8; ++round) {
		LockTracker t(0, 1, 0x1);
		const int64_t base = LockTracker::registeredWakeCallbacks();
		std::vector<std::atomic<int>> settled(kThreads * kPerThread);
		std::atomic<int> added{0};
		std::vector<std::thread> threads;
		for (int th = 0; th < kThreads; ++th) {
			threads.emplace_back([&, th] {
				for (int b = 0; b < kBatches; ++b) {
					std::vector<LockTracker::WakeRegistration> batch;
					for (int i = 0; i < kBatch; ++i) {
						const int index = th * kPerThread + b * kBatch + i;
						auto registration = t.addWakeCallback([&settled, index] { ++settled[index]; });
						if (!registration) {
							++settled[index];  // woken first: nothing was registered
						}
						batch.push_back(std::move(registration));
						++added;
					}
					for (int i = 0; i < kBatch; ++i) {
						if (batch[i].cancel()) {
							++settled[th * kPerThread + b * kBatch + i];
						}
					}
				}
			});
		}
		while (added.load() < kThreads * kPerThread / 2) {
			std::this_thread::yield();
		}
		t.wake();
		for (auto& thread : threads) {
			thread.join();
		}
		for (size_t index = 0; index < settled.size(); ++index) {
			EXPECT_EQ(settled[index].load(), 1) << "registration " << index;
		}
		EXPECT_EQ(LockTracker::registeredWakeCallbacks(), base);
	}
}
