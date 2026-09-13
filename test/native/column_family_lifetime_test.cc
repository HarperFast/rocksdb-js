#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <vector>
#include "core/column_family_lifetime.h"

using rocksdb_js::ColumnFamilyLifetime;

TEST(ColumnFamilyLifetime, RetireWithNoClaimReclaimsImmediately) {
	ColumnFamilyLifetime lifetime;
	bool reclaimNow = false;
	EXPECT_TRUE(lifetime.retire(reclaimNow));
	EXPECT_TRUE(reclaimNow);
	EXPECT_TRUE(lifetime.isRetired());
}

TEST(ColumnFamilyLifetime, SecondRetireIsIdempotentNoOp) {
	ColumnFamilyLifetime lifetime;
	bool reclaimNow = false;
	ASSERT_TRUE(lifetime.retire(reclaimNow));
	reclaimNow = true;
	EXPECT_FALSE(lifetime.retire(reclaimNow));
	EXPECT_FALSE(reclaimNow);
}

TEST(ColumnFamilyLifetime, AdmittedCommitDefersReclaimToItsRelease) {
	ColumnFamilyLifetime lifetime;
	bool reclaimNow = true;
	ASSERT_TRUE(lifetime.admit(reclaimNow));
	EXPECT_FALSE(reclaimNow);

	ASSERT_TRUE(lifetime.retire(reclaimNow));
	EXPECT_FALSE(reclaimNow) << "a claim is held, so the retirer must not drop";

	EXPECT_TRUE(lifetime.release()) << "the last release on a retired generation owns the drop";
}

TEST(ColumnFamilyLifetime, ReleaseOnLiveGenerationNeverReclaims) {
	ColumnFamilyLifetime lifetime;
	bool reclaimNow = false;
	ASSERT_TRUE(lifetime.admit(reclaimNow));
	EXPECT_FALSE(lifetime.release());
}

TEST(ColumnFamilyLifetime, OnlyTheLastOfSeveralClaimsReclaims) {
	ColumnFamilyLifetime lifetime;
	bool reclaimNow = false;
	ASSERT_TRUE(lifetime.admit(reclaimNow));
	ASSERT_TRUE(lifetime.admit(reclaimNow));
	ASSERT_TRUE(lifetime.retire(reclaimNow));
	EXPECT_FALSE(reclaimNow);
	EXPECT_FALSE(lifetime.release());
	EXPECT_TRUE(lifetime.release());
}

TEST(ColumnFamilyLifetime, AdmitAfterRetireIsRefusedAndUndone) {
	ColumnFamilyLifetime lifetime;
	bool reclaimNow = false;
	ASSERT_TRUE(lifetime.retire(reclaimNow));
	EXPECT_TRUE(reclaimNow);

	// The retirer saw no claim and owns the drop; the refused admitter's undo
	// also reports "reclaim now" (its transient claim made admitted non-zero
	// and back), which the claimReclaim gate below collapses to one drop.
	EXPECT_FALSE(lifetime.admit(reclaimNow));
	EXPECT_TRUE(reclaimNow);
	EXPECT_EQ(lifetime.admitted.load(), 0u);
}

TEST(ColumnFamilyLifetime, RefusedAdmitBehindALiveClaimLeavesReclaimToThatClaim) {
	ColumnFamilyLifetime lifetime;
	bool reclaimNow = false;
	ASSERT_TRUE(lifetime.admit(reclaimNow));
	ASSERT_TRUE(lifetime.retire(reclaimNow));
	EXPECT_FALSE(lifetime.admit(reclaimNow));
	EXPECT_FALSE(reclaimNow) << "the earlier claim is still held";
	EXPECT_TRUE(lifetime.release());
}

TEST(ColumnFamilyLifetime, ReclaimIsClaimedExactlyOnceUntilUnclaimed) {
	ColumnFamilyLifetime lifetime;
	EXPECT_TRUE(lifetime.claimReclaim());
	EXPECT_FALSE(lifetime.claimReclaim());
	lifetime.unclaimReclaim();
	EXPECT_TRUE(lifetime.claimReclaim());
}

// Many committers racing one retirer: exactly one side ends up owning the
// drop, and every admitted commit was either admitted before the retirement
// or refused.
TEST(ColumnFamilyLifetime, ConcurrentAdmittersAndRetirerAgreeOnOneOwner) {
	for (int round = 0; round < 200; round++) {
		ColumnFamilyLifetime lifetime;
		std::atomic<int> owners{0};
		std::atomic<int> start{0};
		auto claimDrop = [&]() {
			if (lifetime.claimReclaim()) {
				owners.fetch_add(1);
			}
		};

		std::vector<std::thread> threads;
		for (int i = 0; i < 4; i++) {
			threads.emplace_back([&]() {
				while (start.load() == 0) {}
				bool reclaimNow = false;
				if (lifetime.admit(reclaimNow)) {
					if (lifetime.release()) claimDrop();
				} else if (reclaimNow) {
					claimDrop();
				}
			});
		}
		threads.emplace_back([&]() {
			while (start.load() == 0) {}
			bool reclaimNow = false;
			if (lifetime.retire(reclaimNow) && reclaimNow) claimDrop();
		});
		start.store(1);
		for (auto& thread : threads) thread.join();

		EXPECT_EQ(owners.load(), 1) << "round " << round;
		EXPECT_EQ(lifetime.admitted.load(), 0u);
	}
}
