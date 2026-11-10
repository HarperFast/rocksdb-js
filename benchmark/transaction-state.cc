// Standalone Clang/GCC microbenchmark; see docs/performance/transaction-state.md.
#include <chrono>
#include <cstdio>
#include "core/transaction_state.h"

using rocksdb_js::TransactionState;
using rocksdb_js::TransactionStateCell;

// Single-threaded reference for the old plain-field checks and stores. Never use concurrently.
struct PlainStateCell {
	TransactionState state{TransactionState::Pending};

	bool transition(TransactionState from, TransactionState to) {
		// Only the failed-commit revert used a check before the old store.
		if (from == TransactionState::Committing && to == TransactionState::Pending && state != from) {
			return false;
		}
		state = to;
		return true;
	}

	TransactionState abort() {
		const auto current = state;
		if (current == TransactionState::Pending || current == TransactionState::Committing) {
			state = TransactionState::Aborted;
		}
		return current;
	}
};

template <typename Cell>
static void observe(Cell& cell) {
	// Keep both variants' accesses across phases; emits no hardware fence or instruction.
	asm volatile("" : "+m"(cell) : : "memory");
}

enum class Lifecycle { Commit, RetryThenCommit, Abort };

template <typename Cell, Lifecycle lifecycle>
static double measure(int iterations) {
	const auto start = std::chrono::steady_clock::now();
	for (int i = 0; i < iterations; ++i) {
		Cell cell;
		observe(cell);
		if constexpr (lifecycle == Lifecycle::Abort) {
			cell.abort();
			observe(cell);
		} else {
			cell.transition(TransactionState::Pending, TransactionState::Committing);
			observe(cell);
			if constexpr (lifecycle == Lifecycle::RetryThenCommit) {
				cell.transition(TransactionState::Committing, TransactionState::Pending);
				observe(cell);
				cell.transition(TransactionState::Pending, TransactionState::Committing);
				observe(cell);
			}
			cell.transition(TransactionState::Committing, TransactionState::Committed);
			observe(cell);
		}
		cell.abort(); // close() after commit or explicit abort observes a terminal state.
		observe(cell);
	}
	return std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - start).count() / iterations;
}

template <Lifecycle lifecycle>
static void report(const char* name) {
	measure<PlainStateCell, lifecycle>(100000);
	measure<TransactionStateCell, lifecycle>(100000);
	for (int trial = 0; trial < 9; ++trial) {
		double plain;
		double atomic;
		if (trial % 2 == 0) {
			plain = measure<PlainStateCell, lifecycle>(5000000);
			atomic = measure<TransactionStateCell, lifecycle>(5000000);
		} else {
			atomic = measure<TransactionStateCell, lifecycle>(5000000);
			plain = measure<PlainStateCell, lifecycle>(5000000);
		}
		std::printf("%s,%d,%.3f,%.3f\n", name, trial + 1, plain, atomic);
	}
}

int main() {
	std::puts("lifecycle,trial,plain_ns,atomic_ns");
	report<Lifecycle::Commit>("commit");
	report<Lifecycle::RetryThenCommit>("retry_then_commit");
	report<Lifecycle::Abort>("abort");
}
