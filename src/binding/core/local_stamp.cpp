#include "core/local_stamp.h"

#include <cmath>
#include <limits>

namespace rocksdb_js {

StampClaim tryClaimLocalStamp(
	std::atomic<uint64_t>& watermark,
	const std::atomic<uint64_t>& reserve,
	double candidate,
	bool candidateIsReceiverTime,
	StampNowFn now
) {
	uint64_t wmBits = watermark.load(std::memory_order_acquire);
	for (;;) {
		const double wmValue = localStampFromBits(wmBits);

		// Keep-if-greater: NaN and non-positive candidates fail the first
		// comparison and fall through to re-stamp. The skew check is the only
		// clock consumer on the keep path and is skipped for receiver-time
		// candidates, whose source already is receiver time.
		double next;
		if (candidate > wmValue && candidate < LOCAL_STAMP_MAX &&
			(candidateIsReceiverTime || candidate <= now() + LOCAL_STAMP_MAX_KEPT_SKEW_MS)) {
			next = candidate;
		} else {
			// Receiver-derived, accepted unconditionally so the loop terminates
			// even when the wall clock sits arbitrarily far below the recovered
			// floor (the logical clock then runs ahead from nextafter(wm)).
			next = std::nextafter(wmValue, std::numeric_limits<double>::infinity());
			const double receiverNow = now();
			if (receiverNow > next) {
				next = receiverNow;
			}
		}

		if (!(next > 0.0) || !(next < LOCAL_STAMP_MAX)) {
			return { StampClaimStatus::Exhausted, 0.0 };
		}

		const uint64_t nextBits = localStampToBits(next);
		if (nextBits > reserve.load(std::memory_order_acquire)) {
			return { StampClaimStatus::NeedsReserve, next };
		}

		if (watermark.compare_exchange_weak(
				wmBits, nextBits, std::memory_order_acq_rel, std::memory_order_acquire)) {
			return { StampClaimStatus::Claimed, next };
		}
		// CAS failure reloaded wmBits; re-evaluate against the new watermark.
	}
}

double localStampReserveTarget(double value, double now, double reserveWindowMs) {
	double target = (now > value ? now : value) + reserveWindowMs;
	// Largest persistable ceiling: strictly below the domain bound so a claim
	// at the ceiling itself still satisfies next < LOCAL_STAMP_MAX.
	const double maxCeiling =
		std::nextafter(LOCAL_STAMP_MAX, -std::numeric_limits<double>::infinity());
	if (!(target < LOCAL_STAMP_MAX)) {
		target = maxCeiling;
	}
	return target;
}

} // namespace rocksdb_js
