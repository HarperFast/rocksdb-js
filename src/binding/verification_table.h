#ifndef __VERIFICATION_TABLE_H__
#define __VERIFICATION_TABLE_H__

#include <atomic>
#include <cstdint>
#include <memory>
#include "rocksdb/slice.h"

namespace rocksdb_js {

/**
 * Process-wide cache verification table.
 *
 * A fixed-size, lock-free array of std::atomic<uint64_t> slots, addressed by
 * a hash of (db pointer, column family id, record key). Each slot encodes:
 *
 *   bit 63       : tag (0 = version, 1 = lock pointer; lock support added
 *                  in Phase 2 -- not yet implemented)
 *   bits 62..0   : a 64-bit version, OR (Phase 2) a tagged LockTracker pointer
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

constexpr uint64_t VT_TAG_BIT = 1ULL << 63;

inline bool vtIsLock(uint64_t v) { return (v & VT_TAG_BIT) != 0; }
inline bool vtIsVersion(uint64_t v) { return v != 0 && !vtIsLock(v); }

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

private:
	std::unique_ptr<std::atomic<uint64_t>[]> slots_;
	size_t mask_;
	uint64_t seed_;
};

} // namespace rocksdb_js

#endif
