#ifndef __CORE_COLUMN_FAMILY_LIFETIME_H__
#define __CORE_COLUMN_FAMILY_LIFETIME_H__

#include <atomic>
#include <cstdint>

namespace rocksdb_js {

/**
 * Lifetime protocol for one column-family generation (AGENTS.md invariant 23).
 *
 * A drop retires the generation logically and returns; the physical
 * `DropColumnFamily` runs only when no commit holds a claim on it. A commit
 * claims every family its batch names once, at admission (before the
 * transaction-log write), and releases after `txn->Commit()` returns.
 *
 * Every operation is sequentially consistent, so for a retire (`retired =
 * true`, then read `admitted`) racing an admission (`admitted++`, then read
 * `retired`) at least one side observes the other: either the retirer sees a
 * claim and leaves reclamation to its releaser, or the admitter sees the
 * retirement and refuses. `claimReclaim()` then guarantees that when both
 * sides conclude "reclaim now" only one of them runs the physical drop.
 */
struct ColumnFamilyLifetime final {
	std::atomic<bool> retired{false};
	std::atomic<uint32_t> admitted{0};
	std::atomic<bool> reclaimClaimed{false};

	/**
	 * Commit admission. On `false` the claim has already been undone; the
	 * caller must then treat `release()`'s answer for that undo like any other
	 * release, so a retirer that saw the transient claim is not left waiting.
	 */
	bool admit(bool& reclaimNow) {
		reclaimNow = false;
		this->admitted.fetch_add(1);
		if (!this->retired.load()) {
			return true;
		}
		reclaimNow = this->release();
		return false;
	}

	/**
	 * Releases one claim. Returns true when this was the last claim on a
	 * retired generation: the caller owns the physical drop (subject to
	 * `claimReclaim()`).
	 */
	bool release() {
		return this->admitted.fetch_sub(1) == 1 && this->retired.load();
	}

	/**
	 * Logical drop. Returns false when the generation was already retired
	 * (idempotent re-drop). `reclaimNow` reports whether no claim is held, in
	 * which case the caller owns the physical drop.
	 */
	bool retire(bool& reclaimNow) {
		reclaimNow = false;
		if (this->retired.exchange(true)) {
			return false;
		}
		reclaimNow = this->admitted.load() == 0;
		return true;
	}

	/**
	 * Exactly-once gate for the physical drop. A failed attempt calls
	 * `unclaimReclaim()` so a later retry can claim again.
	 */
	bool claimReclaim() {
		bool expected = false;
		return this->reclaimClaimed.compare_exchange_strong(expected, true);
	}

	void unclaimReclaim() {
		this->reclaimClaimed.store(false);
	}

	/**
	 * Advisory (staging refusal, discard of non-transactional writes);
	 * `admit()` is the ordered gate.
	 */
	bool isRetired() const {
		return this->retired.load(std::memory_order_relaxed);
	}
};

} // namespace rocksdb_js

#endif
