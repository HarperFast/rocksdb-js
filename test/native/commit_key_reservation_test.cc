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
	ASSERT_TRUE(first.acquire(reservation, { 1, 2, 3 }, &first));
	CommitKeyReservationScope second;
	ASSERT_TRUE(second.acquire(reservation, { 4, 5 }, &second));
	EXPECT_EQ(reservation.heldCount(), 5u);
	first.release();
	second.release();
	EXPECT_EQ(reservation.heldCount(), 0u);
}

TEST(CommitKeyReservation, OverlappingWriterWaitsUntilRelease) {
	CommitKeyReservation reservation;
	CommitKeyReservationScope holder;
	ASSERT_TRUE(holder.acquire(reservation, { 7, 8 }, &holder));

	std::atomic<bool> started{ false };
	std::atomic<bool> acquired{ false };
	std::thread waiter([&]() {
		started.store(true);
		CommitKeyReservationScope scope;
		if (!scope.acquire(reservation, { 8, 9 }, &scope)) return;
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
		ASSERT_TRUE(scope.acquire(reservation, { 42 }, &scope));
		EXPECT_TRUE(scope.active());
		scope.release();
		scope.release();
		EXPECT_FALSE(scope.active());
		ASSERT_TRUE(scope.acquire(reservation, { 43 }, &scope));
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
				if (!scope.acquire(reservation, { 1 }, &scope)) continue;
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

TEST(CommitKeyReservation, DurableHoldRefusesOtherOwnersWithoutWaitingAndAdmitsItsOwner) {
	CommitKeyReservation reservation;
	int owner = 0;
	int other = 0;
	CommitKeyReservationScope scope;
	ASSERT_TRUE(scope.acquire(reservation, { 5, 6 }, &owner));
	rocksdb_js::DurableKeyHold hold;
	hold.adopt(scope, &owner);
	EXPECT_FALSE(scope.active());
	EXPECT_EQ(reservation.heldCount(), 2u);

	CommitKeyReservationScope contender;
	EXPECT_FALSE(contender.acquire(reservation, { 6, 7 }, &other)) << "a durable hold is refused, not waited for";
	EXPECT_FALSE(contender.active());
	EXPECT_EQ(reservation.heldCount(), 2u) << "a refused acquisition holds nothing";
	ASSERT_TRUE(contender.acquire(reservation, { 7 }, &other)) << "disjoint keys stay free";
	contender.release();

	CommitKeyReservationScope retry;
	ASSERT_TRUE(retry.acquire(reservation, { 5, 6, 8 }, &owner)) << "the owner commits over its own hold";
	retry.release();
	hold.release(&owner);
	EXPECT_EQ(reservation.heldCount(), 0u);
	ASSERT_TRUE(contender.acquire(reservation, { 5, 6 }, &other));
}
