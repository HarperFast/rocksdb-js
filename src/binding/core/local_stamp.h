#ifndef __CORE_LOCAL_STAMP_H__
#define __CORE_LOCAL_STAMP_H__

#include <atomic>
#include <cstdint>
#include <cstring>

namespace rocksdb_js {

/**
 * Commit-time local mutation stamp allocator (dual-clock stage 1).
 *
 * A stamp is a positive float64 in milliseconds-since-epoch space, claimed at
 * commit against a per-database watermark with a keep-if-greater shortcut:
 * a candidate above every previously claimed stamp is kept, otherwise a fresh
 * receiver-time value is minted. The compare-and-swap claim admits any given
 * double at most once per database, which is what makes stamps unique per
 * write even when callers supply duplicate or origin-derived timestamps.
 *
 * Durability contract: no claim may exceed the persisted reserve ceiling
 * (see docs/design/local-mutation-stamping.md §3.7). This module is Node-free
 * and does no I/O — a claim that needs more ceiling reports NeedsReserve and
 * the caller extends the reserve durably before retrying, never while holding
 * a transaction-log append mutex.
 */

// Upper bound of the stamp domain (harper's MAX_DATE_TIMESTAMP). No candidate,
// claim, or persisted ceiling may reach it.
constexpr double LOCAL_STAMP_MAX = 8.64e15;

// How far above receiver time a caller-supplied candidate may be kept; a
// receiver-generated candidate (getMonotonicTimestamp provenance) skips the
// check, so the keep path reads no clock. Bounds how far any caller can push
// the local clock ahead (watermark <= now + skew at all times).
constexpr double LOCAL_STAMP_MAX_KEPT_SKEW_MS = 3600000.0;

static_assert(std::atomic<uint64_t>::is_always_lock_free,
	"local-stamp watermark requires lock-free 64-bit atomics");

// Positive IEEE-754 doubles order-preserve when their bit patterns compare as
// unsigned integers — the same trick the VerificationTable relies on.
inline uint64_t localStampToBits(double value) {
	uint64_t bits;
	std::memcpy(&bits, &value, sizeof bits);
	return bits;
}

inline double localStampFromBits(uint64_t bits) {
	double value;
	std::memcpy(&value, &bits, sizeof value);
	return value;
}

enum class StampClaimStatus : uint8_t {
	// `value` is the claimed stamp; the watermark advanced to it.
	Claimed,
	// `value` is the stamp that would be claimed, but it exceeds the reserve
	// ceiling; extend the reserve to at least `value` durably, then retry.
	NeedsReserve,
	// No claimable value exists below LOCAL_STAMP_MAX (terminal clock
	// exhaustion; surfaced to the caller as an explicit error, never a spin).
	Exhausted,
};

struct StampClaim {
	StampClaimStatus status;
	double value;
};

// Receiver-time source, injectable for tests. Must return a value >= the wall
// clock in stamp space; production passes getMonotonicTimestamp.
using StampNowFn = double (*)();

/**
 * One lock-free claim attempt. Zero allocations; the only clock read is on the
 * re-stamp path or the skew check for caller-supplied candidates. Any
 * non-finite, non-positive, out-of-domain, or non-monotonic candidate falls to
 * the re-stamp path rather than failing.
 */
StampClaim tryClaimLocalStamp(
	std::atomic<uint64_t>& watermark,
	const std::atomic<uint64_t>& reserve,
	double candidate,
	bool candidateIsReceiverTime,
	StampNowFn now
);

/**
 * The ceiling a reserve extension should persist so that `value` (and the next
 * `reserveWindowMs` of receiver time) becomes claimable. Checked: never
 * returns a value at or above LOCAL_STAMP_MAX; returns a value < `value` only
 * when the domain is exhausted (caller must then fail open, not persist).
 */
double localStampReserveTarget(double value, double now, double reserveWindowMs);

} // namespace rocksdb_js

#endif
