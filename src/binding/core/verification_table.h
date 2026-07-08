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
 *   bit 63        : tag (0 = version, 1 = lock or settled-empty)
 *   bit 62        : among tagged values, 0 = lock, 1 = settled-empty
 *   versions      : the full uint64 is the version bit pattern (bit 63 == 0)
 *   locks         : bits 61..48 a 14-bit generation, bits 47..0 a LockTracker*
 *   settled-empty : bits 61..0 a 62-bit monotonic settle generation
 *
 * Real Harper record versions are positive float64 timestamps; their sign bit
 * is always 0 when their bit pattern is interpreted as uint64, so bit 63 is
 * available as the tag bit without colliding with any real version.
 *
 * A slot holds 0 only before its first write/populate. After a write cycle
 * settles it never returns to 0 — it goes to a *settled-empty* marker carrying a
 * fresh, ever-increasing generation. This is what makes the lock-free cold
 * populate safe: a populate is a single CAS from the value observed *before* the
 * read (see populateVersionIfUnchanged); because a completed write cycle always
 * moves the slot through a lock and then to a new generation, it can never
 * restore the pre-read value, so an intervening write always fails that CAS and
 * a stale/superseded version can never be published (no ABA on the empty state).
 *
 * Hash collisions are intentional. Two different keys hashing to the same
 * slot will spuriously share state; this can cause false invalidations
 * (revert to slow path) but never incorrect results.
 */

constexpr uint64_t VT_TAG_BIT     = 1ULL << 63;             // 1 = not a version
constexpr uint64_t VT_SETTLED_BIT = 1ULL << 62;             // among tagged: 1 = settled-empty
constexpr uint64_t VT_GEN_MASK    = 0x3FFFULL << 48;        // bits 61..48: 14-bit lock generation
constexpr uint64_t VT_PTR_MASK    = (1ULL << 48) - 1;       // bits 47..0:  48-bit pointer
constexpr uint64_t VT_SETTLED_GEN_MASK = (1ULL << 62) - 1;  // bits 61..0:  62-bit settle generation

// A real version: bit 63 clear and nonzero.
inline bool vtIsVersion(uint64_t v) { return v != 0 && (v & VT_TAG_BIT) == 0; }
// A lock: tagged with the settled bit clear.
inline bool vtIsLock(uint64_t v)    { return (v & VT_TAG_BIT) != 0 && (v & VT_SETTLED_BIT) == 0; }
// A settled-empty marker: tagged with the settled bit set (carries a generation).
inline bool vtIsSettled(uint64_t v) { return (v & VT_TAG_BIT) != 0 && (v & VT_SETTLED_BIT) != 0; }

// LockTracker slot encoding helpers.
// x86-64 canonical user-space pointers fit in 48 bits.
inline uint64_t vtEncodeLock(LockTracker* p, uint16_t gen) {
	return VT_TAG_BIT
	     | (static_cast<uint64_t>(gen & 0x3FFF) << 48)
	     | (reinterpret_cast<uintptr_t>(p) & VT_PTR_MASK);
}
inline LockTracker* vtDecodeLock(uint64_t v) {
	return reinterpret_cast<LockTracker*>(v & VT_PTR_MASK);
}
inline uint16_t vtGenFromLock(uint64_t v) {
	return static_cast<uint16_t>((v & VT_GEN_MASK) >> 48);
}
// Encode a settled-empty marker carrying a 62-bit generation.
inline uint64_t vtEncodeSettled(uint64_t gen) {
	return VT_TAG_BIT | VT_SETTLED_BIT | (gen & VT_SETTLED_GEN_MASK);
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
	size_t                slotIndex;    // index in VT slots_ array (for cancelForDB)
	uintptr_t             dbPtr;        // identity of the owning DB descriptor

	bool                               woken{false};
	std::mutex                         wakeCallbacksMutex;
	std::vector<std::function<void()>> wakeCallbacks;

	LockTracker(size_t idx, uint16_t gen, uintptr_t dbPtr)
		: refcount(1), holders(0), generation(gen), slotIndex(idx), dbPtr(dbPtr) {}

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

// Returns a fresh 14-bit generation tag for a new LockTracker install.
// Process-global monotonic counter wraps every 16 K installs.
uint16_t vtNextGen();

// Returns a fresh 62-bit settle generation, stamped into a slot when a write
// cycle settles (see vtEncodeSettled). Process-global monotonic counter; at
// 62 bits it never wraps in practice, so each settle leaves a slot value
// distinct from any value a reader could have observed before the write.
uint64_t vtNextSettleGen();

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
	 * Unconditionally installs `newVersion`, overwriting whatever the slot holds
	 * (0, a settled-empty marker, or an older version) except a lock, which is
	 * never overwritten. This is the low-level "force set" primitive behind the
	 * explicit populateVersion() JS API; the cold read path uses
	 * populateVersionIfUnchanged() instead. Returns true on success.
	 */
	static bool populateVersion(std::atomic<uint64_t>* slot, uint64_t newVersion);

	/**
	 * Conditionally publishes `newVersion`, succeeding only if the slot still
	 * holds `observed` — the value the caller loaded *before* reading the value
	 * from RocksDB. This is the lock-free cold populate: a single CAS that is a
	 * no-op if any write cycle intervened between the read and the populate
	 * (which always moves the slot through a lock and on to a fresh settle
	 * generation, so it can never restore `observed`). Therefore it can never
	 * publish a stale or superseded version. Skips (returns false) when `observed`
	 * is a lock or `newVersion` is not a real version.
	 */
	static bool populateVersionIfUnchanged(
		std::atomic<uint64_t>* slot,
		uint64_t observed,
		uint64_t newVersion
	);

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

	/**
	 * Safety-net scan called from DBDescriptor::close(). Finds every slot
	 * whose LockTracker was installed by the given database, CASes it back to
	 * 0, and calls wake() to unpark any parked TSFN waiters.
	 *
	 * Under normal shutdown all TransactionHandle::close() calls already do
	 * this via releaseIntent(); cancelForDB() is a defensive final pass for
	 * unexpected races or shutdown ordering issues.
	 */
	void cancelForDB(uintptr_t dbPtr);

	/**
	 * Sweeps every non-lock slot in the table and advances it to a fresh
	 * settled-empty generation. Called after a bulk-delete operation (clear)
	 * to ensure that any pre-clear version cached in a slot can no longer be
	 * re-published via a concurrent populate CAS. The sweep is coarse (it
	 * covers all slots, not just those for the cleared store) because slot
	 * identities are not tracked in version or settled-empty slots — only in
	 * LockTracker, which belongs to lock slots that are intentionally skipped.
	 *
	 * Lock slots are skipped: the write-intent lifecycle already advances them
	 * to a fresh settled-empty generation when the last holder releases. A
	 * concurrent lockSlotForWrite may overwrite a slot we just settled via an
	 * unconditional store (not a CAS) — this is safe because the subsequent
	 * commit+releaseWriteIntent also advances to a new generation, blocking
	 * any pre-clear populate that observed the slot value before the clear.
	 *
	 * Ordering: call AFTER the data-delete operations complete. Any stale
	 * version that a concurrent reader published between delete-start and
	 * sweep-start is overwritten by the sweep; after the sweep verifyVersion
	 * with any pre-clear version returns false.
	 */
	void settleAllSlots();

	// ---- Write-intent lifecycle (LockTracker management) ----
	//
	// These four methods own the full lifecycle of a slot's LockTracker and are
	// the ONLY places a tracker pointer is loaded-from-a-slot-then-dereferenced.
	// They are serialized by writerMutex_ so a tracker can never be freed by one
	// thread (releaseWriteIntent / unrefTracker) in the window where another
	// thread (lockSlotForWrite / refTrackerIfLocked / cancelForDB) has loaded its
	// pointer from a slot but not yet taken a reference — closing the lock-free
	// load-then-incref use-after-free. The hot read path (verifyVersion,
	// populateVersion) stays lock-free; writers are the cold/contended path.

	/**
	 * Registers a write intent on `slot`. If the slot is already locked by a
	 * concurrent writer, joins that LockTracker as an additional holder (so the
	 * slot is not cleared until every writer releases); otherwise installs a new
	 * tracker. Returns the tracker the caller now holds an intent on (to be
	 * passed to releaseWriteIntent), or nullptr if the slot is null.
	 */
	LockTracker* lockSlotForWrite(std::atomic<uint64_t>* slot, uintptr_t dbPtr);

	/**
	 * Releases one write intent previously taken via lockSlotForWrite. When the
	 * last holder releases, the slot is CAS'd back to 0 and parked waiters are
	 * woken. Frees the tracker once its last reference is dropped.
	 */
	void releaseWriteIntent(std::atomic<uint64_t>* slot, LockTracker* tracker);

	/**
	 * If `slot` currently holds a lock, takes a temporary reference on its
	 * LockTracker and returns it (caller must later call unrefTracker); returns
	 * nullptr if the slot is not locked. Used by the coordinated-retry parker.
	 */
	LockTracker* refTrackerIfLocked(std::atomic<uint64_t>* slot);

	/** Drops a temporary reference taken via refTrackerIfLocked. */
	void unrefTracker(LockTracker* tracker);

private:
	std::unique_ptr<std::atomic<uint64_t>[]> slots_;
	size_t mask_;
	uint64_t seed_;

	// Serializes all LockTracker install / join / release / reference / reclaim
	// operations (see the write-intent lifecycle methods above). Not taken on
	// the lock-free read path.
	std::mutex writerMutex_;
};

} // namespace rocksdb_js

#endif
