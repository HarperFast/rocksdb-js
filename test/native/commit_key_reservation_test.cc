#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include "core/commit_key_reservation.h"

using rocksdb_js::CommitKeyReservation;
using rocksdb_js::CommitKeyReservationScope;

namespace {

void waitFor(std::atomic<bool>& flag) {
	for (int i = 0; i < 5000 && !flag.load(); i++) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
}

} // namespace

TEST(CommitKeyReservation, DisjointWritersDoNotWait) {
	CommitKeyReservation reservation;
	CommitKeyReservationScope first;
	first.acquire(reservation, { 1, 2, 3 });
	CommitKeyReservationScope second;
	second.acquire(reservation, { 4, 5 });
	EXPECT_EQ(reservation.heldCount(), 5u);
	first.release();
	second.release();
	EXPECT_EQ(reservation.heldCount(), 0u);
}

TEST(CommitKeyReservation, OverlappingWriterWaitsUntilRelease) {
	CommitKeyReservation reservation;
	CommitKeyReservationScope holder;
	holder.acquire(reservation, { 7, 8 });

	std::atomic<bool> started{ false };
	std::atomic<bool> acquired{ false };
	std::thread waiter([&]() {
		started.store(true);
		CommitKeyReservationScope scope;
		scope.acquire(reservation, { 8, 9 });
		acquired.store(true);
	});
	waitFor(started);
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	EXPECT_FALSE(acquired.load()) << "a shared key must block the second writer";

	holder.release();
	waitFor(acquired);
	EXPECT_TRUE(acquired.load());
	waiter.join();
	EXPECT_EQ(reservation.heldCount(), 0u);
}

TEST(CommitKeyReservation, ScopeReleasesOnDestructionAndIsIdempotent) {
	CommitKeyReservation reservation;
	{
		CommitKeyReservationScope scope;
		scope.acquire(reservation, { 42 });
		EXPECT_TRUE(scope.active());
		scope.release();
		scope.release();
		EXPECT_FALSE(scope.active());
		scope.acquire(reservation, { 43 });
	}
	EXPECT_EQ(reservation.heldCount(), 0u);
}

TEST(CommitKeyReservation, ManyWritersOnOneKeySerializeWithoutLosingWakeups) {
	CommitKeyReservation reservation;
	std::atomic<int> inside{ 0 };
	std::atomic<int> maxInside{ 0 };
	std::atomic<int> done{ 0 };
	std::vector<std::thread> threads;
	for (int t = 0; t < 8; t++) {
		threads.emplace_back([&]() {
			for (int i = 0; i < 200; i++) {
				CommitKeyReservationScope scope;
				scope.acquire(reservation, { 1 });
				const int now = ++inside;
				int seen = maxInside.load();
				while (now > seen && !maxInside.compare_exchange_weak(seen, now)) {
				}
				--inside;
				scope.release();
			}
			++done;
		});
	}
	for (auto& thread : threads) {
		thread.join();
	}
	EXPECT_EQ(done.load(), 8);
	EXPECT_EQ(maxInside.load(), 1);
	EXPECT_EQ(reservation.heldCount(), 0u);
}
