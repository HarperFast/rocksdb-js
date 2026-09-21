#ifndef __CORE_COLUMN_FAMILY_LIFETIME_H__
#define __CORE_COLUMN_FAMILY_LIFETIME_H__

#include <atomic>
#include <cstdint>

namespace rocksdb_js {

/** Lifetime protocol for one column-family generation; see AGENTS.md invariant 24. */
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

	bool release() {
		return this->admitted.fetch_sub(1) == 1 && this->retired.load();
	}

	bool retire(bool& reclaimNow) {
		reclaimNow = false;
		if (this->retired.exchange(true)) {
			return false;
		}
		reclaimNow = this->admitted.load() == 0;
		return true;
	}

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

	/** The physical drop follows retire(), so a post-RocksDB-failure recheck is authoritative. */
	bool isRetiredOrdered() const {
		return this->retired.load();
	}
};

} // namespace rocksdb_js

#endif
