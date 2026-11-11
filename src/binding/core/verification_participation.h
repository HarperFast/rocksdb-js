#ifndef __CORE_VERIFICATION_PARTICIPATION_H__
#define __CORE_VERIFICATION_PARTICIPATION_H__

#include <atomic>
#include <cstdint>

namespace rocksdb_js {

/**
 * Verification-table participation of one column-family generation, shared by
 * every handle on it (AGENTS.md invariant 39; core/DESIGN.md "Participation is
 * per column family"). Mode changes happen under `DBRegistry::databasesMutex`,
 * except `activate()`, which any reader may win.
 *
 * A late opt-in converts the column family in place: writes take write intents
 * from `Sweeping` on, while the verification table stays inert for it until
 * `activate()`, which the caller makes only once nothing that may hold a write
 * staged without an intent (any transaction with an id up to `watermark`, any
 * operation in flight at the opt-in) remains.
 */
struct VerificationParticipation final {
	enum Mode : uint32_t {
		None = 0,
		// Opt-in under way: writes guarded, slots being settled, nothing trusted.
		Sweeping = 1,
		// Writes guarded; waiting for unguarded writers to drain.
		Guarding = 2,
		Publishing = 3,
	};

	std::atomic<uint32_t> mode{None};
	std::atomic<uint64_t> watermark{0};
	// The descriptor's transaction-removal count when a drain scan last found a
	// blocker, and a steady_clock deadline (ns) after which it is rescanned anyway:
	// a transaction whose handle closed first can close without leaving the registry.
	std::atomic<uint64_t> blockedAtRemovals{UINT64_MAX};
	std::atomic<int64_t> rescanAt{0};

	bool guardsWrites() const {
		return this->mode.load() != None;
	}

	/** Slots may be trusted (FRESH, `verifyVersion`) or explicitly seeded. */
	bool verifies() const {
		const uint32_t current = this->mode.load();
		return current == None || current == Publishing;
	}

	bool publishing() const {
		return this->mode.load() == Publishing;
	}

	bool converting() const {
		return this->mode.load() == Guarding;
	}

	/** From `None`, with no write staged before it to wait out. */
	void publish() {
		this->mode.store(Publishing);
	}

	/**
	 * From `None`. The caller then raises the watermark past every existing
	 * transaction (an RMW on the id counter, ordered after this store, so a
	 * transaction created later observes the guard), settles every slot, and
	 * calls `finishSweep()`.
	 */
	void beginConversion() {
		this->blockedAtRemovals.store(UINT64_MAX, std::memory_order_relaxed);
		this->mode.store(Sweeping);
	}

	void finishSweep(uint64_t transactionWatermark) {
		this->watermark.store(transactionWatermark);
		this->mode.store(Guarding);
	}

	/** Returns whether the column family now publishes. */
	bool activate() {
		uint32_t expected = Guarding;
		return this->mode.compare_exchange_strong(expected, Publishing) || expected == Publishing;
	}

	void optOut() {
		this->mode.store(None);
	}
};

} // namespace rocksdb_js

#endif
