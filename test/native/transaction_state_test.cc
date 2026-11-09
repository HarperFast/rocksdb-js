#include <gtest/gtest.h>
#include <array>
#include <barrier>
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
	std::array<TransactionStateCell, 20000> states;
	for (auto& state : states) {
		ASSERT_TRUE(state.transition(TransactionState::Pending, TransactionState::Committing));
	}
	std::barrier start(2);
	std::thread completion([&] {
		for (auto& state : states) {
			start.arrive_and_wait();
			state.transition(TransactionState::Committing, TransactionState::Pending);
		}
	});
	std::thread closer([&] {
		for (auto& state : states) {
			start.arrive_and_wait();
			state.abort();
		}
	});
	completion.join();
	closer.join();
	for (size_t i = 0; i < states.size(); ++i) {
		ASSERT_EQ(states[i].load(), TransactionState::Aborted) << "iteration " << i;
	}
}
