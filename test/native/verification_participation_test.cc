#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <vector>
#include "core/verification_participation.h"

using rocksdb_js::VerificationParticipation;

TEST(VerificationParticipation, StartsOutsideTheTable) {
	VerificationParticipation participation;
	EXPECT_FALSE(participation.guardsWrites());
	EXPECT_FALSE(participation.publishing());
	// A non-participating column family keeps the explicit primitives.
	EXPECT_TRUE(participation.verifies());
}

TEST(VerificationParticipation, FreshOptInPublishesAtOnce) {
	VerificationParticipation participation;
	participation.publish();
	EXPECT_TRUE(participation.guardsWrites());
	EXPECT_TRUE(participation.publishing());
	EXPECT_TRUE(participation.verifies());
}

TEST(VerificationParticipation, ConversionGuardsWritesBeforeTrustingSlots) {
	VerificationParticipation participation;
	participation.beginConversion();
	EXPECT_TRUE(participation.guardsWrites());
	EXPECT_FALSE(participation.verifies());
	EXPECT_FALSE(participation.converting());
	// Nothing may activate while slots are still being settled.
	EXPECT_FALSE(participation.activate());
	EXPECT_FALSE(participation.publishing());

	participation.finishSweep(42);
	EXPECT_EQ(participation.watermark.load(), 42u);
	EXPECT_TRUE(participation.converting());
	EXPECT_TRUE(participation.guardsWrites());
	EXPECT_FALSE(participation.verifies());

	EXPECT_TRUE(participation.activate());
	EXPECT_TRUE(participation.publishing());
	EXPECT_TRUE(participation.verifies());
	EXPECT_TRUE(participation.activate());
}

TEST(VerificationParticipation, OptOutStopsGuardingWrites) {
	VerificationParticipation participation;
	participation.publish();
	participation.optOut();
	EXPECT_FALSE(participation.guardsWrites());
	EXPECT_FALSE(participation.activate());
	EXPECT_FALSE(participation.publishing());
}

TEST(VerificationParticipation, PendingCountCoversConversionUntilItEnds) {
	const uint32_t before = rocksdb_js::verificationConversionsPending().load();
	{
		VerificationParticipation activated;
		activated.beginConversion();
		activated.finishSweep(1);
		EXPECT_EQ(rocksdb_js::verificationConversionsPending().load(), before + 1);
		EXPECT_TRUE(activated.activate());
		EXPECT_EQ(rocksdb_js::verificationConversionsPending().load(), before);

		VerificationParticipation optedOut;
		optedOut.beginConversion();
		optedOut.finishSweep(1);
		optedOut.optOut();
		EXPECT_EQ(rocksdb_js::verificationConversionsPending().load(), before);

		VerificationParticipation abandoned;
		abandoned.beginConversion();
		EXPECT_EQ(rocksdb_js::verificationConversionsPending().load(), before + 1);
	}
	EXPECT_EQ(rocksdb_js::verificationConversionsPending().load(), before);
}

TEST(VerificationParticipation, ConcurrentActivationsAllObservePublishing) {
	const uint32_t before = rocksdb_js::verificationConversionsPending().load();
	VerificationParticipation participation;
	participation.beginConversion();
	participation.finishSweep(1);
	std::atomic<int> published{0};
	std::vector<std::thread> threads;
	for (int i = 0; i < 8; i++) {
		threads.emplace_back([&] {
			if (participation.activate()) {
				published.fetch_add(1);
			}
		});
	}
	for (auto& thread : threads) {
		thread.join();
	}
	EXPECT_EQ(published.load(), 8);
	EXPECT_TRUE(participation.publishing());
	EXPECT_EQ(rocksdb_js::verificationConversionsPending().load(), before);
}
