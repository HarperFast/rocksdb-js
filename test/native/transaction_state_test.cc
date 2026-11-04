#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include "core/transaction_state.h"

using rocksdb_js::TransactionState;
using rocksdb_js::TransactionStateCell;

TEST(TransactionStateCell, StartsPending) {
	TransactionStateCell state;
	EXPECT_EQ(state.load(), TransactionState::Pending);
}

TEST(TransactionStateCell, TransitionRequiresExpectedState) {
	TransactionStateCell state;
	EXPECT_FALSE(state.transition(TransactionState::Committing, TransactionState::Pending));
	EXPECT_EQ(state.load(), TransactionState::Pending);
	EXPECT_TRUE(state.transition(TransactionState::Pending, TransactionState::Committing));
	EXPECT_TRUE(state.transition(TransactionState::Committing, TransactionState::Pending));
	EXPECT_TRUE(state.transition(TransactionState::Pending, TransactionState::Committing));
	EXPECT_TRUE(state.transition(TransactionState::Committing, TransactionState::Committed));
	EXPECT_EQ(state.load(), TransactionState::Committed);
}

TEST(TransactionStateCell, AbortSettlesPendingAndCommitting) {
	TransactionStateCell pending;
	EXPECT_EQ(pending.abort(), TransactionState::Pending);
	EXPECT_EQ(pending.load(), TransactionState::Aborted);

	TransactionStateCell committing;
	ASSERT_TRUE(committing.transition(TransactionState::Pending, TransactionState::Committing));
	EXPECT_EQ(committing.abort(), TransactionState::Committing);
	EXPECT_EQ(committing.load(), TransactionState::Aborted);
}

TEST(TransactionStateCell, AbortedIsTerminal) {
	TransactionStateCell state;
	ASSERT_TRUE(state.transition(TransactionState::Pending, TransactionState::Committing));
	state.abort();
	// The failed-commit completion that lost the race to close() (#769).
	EXPECT_FALSE(state.transition(TransactionState::Committing, TransactionState::Pending));
	EXPECT_FALSE(state.transition(TransactionState::Pending, TransactionState::Committing));
	EXPECT_FALSE(state.transition(TransactionState::Committing, TransactionState::Committed));
	EXPECT_EQ(state.abort(), TransactionState::Aborted);
	EXPECT_EQ(state.load(), TransactionState::Aborted);
}

TEST(TransactionStateCell, CommittedIsTerminal) {
	TransactionStateCell state;
	ASSERT_TRUE(state.transition(TransactionState::Pending, TransactionState::Committing));
	ASSERT_TRUE(state.transition(TransactionState::Committing, TransactionState::Committed));
	EXPECT_EQ(state.abort(), TransactionState::Committed);
	EXPECT_EQ(state.load(), TransactionState::Committed);
}

// A completion reverting Committing -> Pending races a foreign close() settling Aborted: whichever
// runs first, the handle must end Aborted.
TEST(TransactionStateCell, AbortWinsAgainstConcurrentRevert) {
	for (int i = 0; i < 20000; ++i) {
		TransactionStateCell state;
		ASSERT_TRUE(state.transition(TransactionState::Pending, TransactionState::Committing));
		std::atomic<bool> go{false};
		std::thread completion([&] {
			while (!go.load(std::memory_order_acquire)) {
			}
			state.transition(TransactionState::Committing, TransactionState::Pending);
		});
		std::thread closer([&] {
			while (!go.load(std::memory_order_acquire)) {
			}
			state.abort();
		});
		go.store(true, std::memory_order_release);
		completion.join();
		closer.join();
		ASSERT_EQ(state.load(), TransactionState::Aborted) << "iteration " << i;
	}
}
