#ifndef __VERIFICATION_TABLE_H__
#define __VERIFICATION_TABLE_H__

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>
#include "rocksdb/slice.h"

namespace rocksdb_js {

// Forward declaration so vtEncodeLock/vtDecodeLock helpers compile.
struct LockTracker;

/**
 * Process-wide cache verification table.
 *
 * A fixed-size, lock-free array of std::atomic<uint64_t> slots, addressed by
 * a hash of (db pointer, column family id, record key). Each slot encodes:
 *
 *   bit 63       : tag (0 = version, 1 = lock pointer)
 *   bits 62..48  : 0 for versions; for locks, a 15-bit generation counter
 *   bits 47..0   : a 64-bit version low bits, OR a 48-bit LockTracker pointer
 *
 * Real Harper record versions are positive float64 timestamps; their sign bit
 * is always 0 when their bit pattern is interpreted as uint64, so bit 63 is
 * available as the lock-tag bit without colliding with any real version. A
 * value of 0 means "empty / unknown".
 *
 * Hash collisions are intentional. Two different keys hashing to the same
 * slot will spuriously share state; this can cause false invalidations
 * (revert to slow path) but never incorrect results.
 */

constexpr uint64_t VT_TAG_BIT  = 1ULL << 63;
constexpr uint64_t VT_GEN_MASK = 0x7FFFULL << 48;  // bits 62..48: 15-bit generation
constexpr uint64_t VT_PTR_MASK = (1ULL << 48) - 1;  // bits 47..0:  48-bit pointer

inline bool vtIsLock(uint64_t v)    { return (v & VT_TAG_BIT) != 0; }
inline bool vtIsVersion(uint64_t v) { return v != 0 && !vtIsLock(v); }

// LockTracker slot encoding helpers.
// x86-64 canonical user-space pointers fit in 48 bits.
inline uint64_t vtEncodeLock(LockTracker* p, uint16_t gen) {
	return VT_TAG_BIT
	     | (static_cast<uint64_t>(gen & 0x7FFF) << 48)
	     | (reinterpret_cast<uintptr_t>(p) & VT_PTR_MASK);
}
inline LockTracker* vtDecodeLock(uint64_t v) {
	return reinterpret_cast<LockTracker*>(v & VT_PTR_MASK);
}
inline uint16_t vtGenFromLock(uint64_t v) {
	return static_cast<uint16_t>((v & VT_GEN_MASK) >> 48);
}

/**
 * Per-slot intent tracker for write-in-flight coordination.
 *
 * Installed (bit 63 set) in a VT slot while one or more transactions are
 * committing keys that hash to that slot. Readers that see the lock tag fall
 * through to a normal RocksDB read instead of trusting cached versions.
 * Released back to 0 after the last holder commits or aborts.
 *
 * Lifetime: heap-allocated; freed when refcount drops to zero.
 *
 * Phase 3 will add: waitersMutex + waiters[] for TSFN-based wake-up.
 */
struct LockTracker {
	std::atomic<uint32_t> refcount{1};  // 1 for the slot reference + 1 per holder
	std::atomic<uint32_t> holders{0};   // count of active intent registrations
	uint16_t              generation;   // immutable after install; matches slot encoding
	size_t                slotIndex;    // index in VT slots_ array (for Phase 4 cleanup)

	bool                               woken{false};
	std::mutex                         wakeCallbacksMutex;
	std::vector<std::function<void()>> wakeCallbacks;

	LockTracker(size_t idx, uint16_t gen)
		: refcount(1), holders(0), generation(gen), slotIndex(idx) {}

	/**
	 * Registers a callback to be invoked when wake() is called.
	 * If wake() was already called, returns false immediately — the caller should
	 * proceed without parking (the lock has already been released).
	 */
	bool addWakeCallback(std::function<void()> cb);

	/**
	 * Fires all registered wake callbacks and marks this tracker as woken.
	 * Subsequent addWakeCallback() calls return false.
	 * Called from releaseIntent() after zeroing the VT slot.
	 */
	void wake();
};

// Returns a fresh 15-bit generation tag for a new LockTracker install.
// Process-global monotonic counter wraps every 32 K installs.
uint16_t vtNextGen();

class VerificationTable final {
public:
	/**
	 * Construct a table with at least `numEntries` atomic slots. The actual
	 * size is rounded up to the next power of two. A `numEntries` of 0
	 * disables the table; `slotFor()` then returns null and all helpers are
	 * no-ops.
	 */
	VerificationTable(size_t numEntries, uint64_t seed);
	~VerificationTable();

	/**
	 * Returns a pointer to the slot for the given (db, cf, key). Returns null
	 * when the table is disabled.
	 */
	std::atomic<uint64_t>* slotFor(
		uintptr_t dbPtr,
		uint32_t cfId,
		const rocksdb::Slice& key
	) const;

	/**
	 * Returns true if the slot currently holds a version equal to
	 * `expectedVersion`.
	 */
	static bool verifyVersion(std::atomic<uint64_t>* slot, uint64_t expectedVersion);

	/**
	 * Attempts to install `newVersion` in the slot. Only succeeds if the slot
	 * currently holds 0 or a version (never overwrites a lock-tagged slot).
	 * Returns true on success.
	 */
	static bool populateVersion(std::atomic<uint64_t>* slot, uint64_t newVersion);

	/**
	 * Reads the first 8 bytes of `value` as a big-endian uint64 and converts
	 * to host-endian. This matches the float64 timestamp Harper writes via
	 * DataView.setFloat64() at offset 0 of every record value, reinterpreted
	 * as the host-endian uint64 bit pattern that JavaScript's Number `v`
	 * occupies in memory.
	 *
	 * Returns 0 when `value.size() < 8`.
	 */
	static uint64_t extractVersionFromValue(const rocksdb::Slice& value);

	size_t size() const { return slots_ ? mask_ + 1 : 0; }
	uint64_t seed() const { return seed_; }

	/**
	 * Returns the index (into slots_) of a pointer previously returned by
	 * slotFor(). Undefined behaviour if the pointer was not from this table.
	 */
	size_t slotIndexOf(std::atomic<uint64_t>* slot) const {
		return static_cast<size_t>(slot - slots_.get());
	}

private:
	std::unique_ptr<std::atomic<uint64_t>[]> slots_;
	size_t mask_;
	uint64_t seed_;
};

} // namespace rocksdb_js

#endif
