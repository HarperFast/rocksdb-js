#ifndef __CORE_TRANSACTION_STATE_H__
#define __CORE_TRANSACTION_STATE_H__

#include <atomic>
#include <cstdint>

namespace rocksdb_js {

enum class TransactionState : uint8_t {
	Pending,
	Committing,
	Committed,
	Aborted
};

/**
 * A transaction's lifecycle state. `close()` settles `Aborted` from whichever
 * thread tears the transaction down, concurrently with the owning thread's
 * commit transitions, so every change is a compare-exchange from the state the
 * writer expects: `Committed` and `Aborted` are terminal and nothing can move a
 * handle back out of them. See transaction/DESIGN.md, "State transitions".
 */
class TransactionStateCell final {
public:
	/**
	 * Advisory for gating: a concurrent `abort()` can follow immediately, so a
	 * caller that must not proceed past an abort needs a `transition()`.
	 */
	TransactionState load() const noexcept {
		return this->state.load(std::memory_order_relaxed);
	}

	bool transition(TransactionState from, TransactionState to) noexcept {
		return this->state.compare_exchange_strong(from, to, std::memory_order_acq_rel, std::memory_order_relaxed);
	}

	/** Returns the state found, so `close()` can tell a commit that already landed. */
	TransactionState abort() noexcept {
		TransactionState current = this->load();
		while ((current == TransactionState::Pending || current == TransactionState::Committing) &&
			!this->state.compare_exchange_weak(current, TransactionState::Aborted, std::memory_order_acq_rel, std::memory_order_relaxed)) {
		}
		return current;
	}

private:
	std::atomic<TransactionState> state{ TransactionState::Pending };
};

} // namespace rocksdb_js

#endif
