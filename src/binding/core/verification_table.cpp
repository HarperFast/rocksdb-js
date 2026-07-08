#include "core/verification_table.h"
#include <cstring>
#include "core/debug.h"

namespace rocksdb_js {

namespace {

// Process-global counter for LockTracker generation tags.
static std::atomic<uint16_t> vtGlobalGen{0};

// Process-global counter for settle generations (62-bit; starts at 1 so a
// settled-empty marker is always distinct from the all-zero initial state).
static std::atomic<uint64_t> vtGlobalSettleGen{1};

// SplitMix64 finalizer.
inline uint64_t mix64(uint64_t x) {
	x ^= x >> 33;
	x *= 0xff51afd7ed558ccdULL;
	x ^= x >> 33;
	x *= 0xc4ceb9fe1a85ec53ULL;
	x ^= x >> 33;
	return x;
}

inline uint64_t loadUnaligned64(const uint8_t* p) {
	uint64_t v;
	std::memcpy(&v, p, sizeof(v));
	return v;
}

inline uint64_t hashKeyBytes(const uint8_t* data, size_t len, uint64_t seed) {
	uint64_t h = seed;
	size_t i = 0;
	while (i + 8 <= len) {
		h ^= loadUnaligned64(data + i);
		h = mix64(h);
		i += 8;
	}
	if (i < len) {
		uint64_t tail = 0;
		std::memcpy(&tail, data + i, len - i);
		h ^= tail;
		h = mix64(h);
	}
	h ^= static_cast<uint64_t>(len);
	return mix64(h);
}

inline size_t roundUpToPowerOf2(size_t n) {
	if (n <= 1) return n;
	size_t p = 1;
	while (p < n) {
		p <<= 1;
	}
	return p;
}

// Big-endian byte-swap of a 64-bit value. The first 8 bytes of a Harper
// record value are written via DataView.setFloat64(offset, ts), which uses
// big-endian byte order. We want to produce the host-endian uint64 that
// matches the JS Number's in-memory representation; on a LE host that's
// bswap(value-loaded-as-uint64-from-record-bytes).
inline uint64_t toHostEndian(uint64_t beValue) {
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
	return __builtin_bswap64(beValue);
#elif defined(_WIN32)
	// Windows is always little-endian on the architectures we support.
	return _byteswap_uint64(beValue);
#else
	// Big-endian host: bytes are already in host order.
	return beValue;
#endif
}

} // namespace

VerificationTable::VerificationTable(size_t numEntries, uint64_t seed)
	: slots_(nullptr), mask_(0), seed_(seed) {
	if (numEntries == 0) {
		return;
	}
	size_t n = roundUpToPowerOf2(numEntries);
	slots_ = std::unique_ptr<std::atomic<uint64_t>[]>(new std::atomic<uint64_t>[n]);
	for (size_t i = 0; i < n; ++i) {
		slots_[i].store(0, std::memory_order_relaxed);
	}
	mask_ = n - 1;
	DEBUG_LOG(
		"VerificationTable initialized: %zu slots (%zu bytes), seed=0x%llx\n",
		n,
		n * sizeof(std::atomic<uint64_t>),
		static_cast<unsigned long long>(seed)
	);
}

VerificationTable::~VerificationTable() = default;

std::atomic<uint64_t>* VerificationTable::slotFor(
	uintptr_t dbPtr,
	uint32_t cfId,
	const rocksdb::Slice& key
) const {
	if (!slots_) {
		return nullptr;
	}
	uint64_t h = seed_;
	h ^= static_cast<uint64_t>(dbPtr);
	h = mix64(h);
	h ^= static_cast<uint64_t>(cfId);
	h = mix64(h);
	h = hashKeyBytes(reinterpret_cast<const uint8_t*>(key.data()), key.size(), h);
	return &slots_[h & mask_];
}

bool VerificationTable::verifyVersion(
	std::atomic<uint64_t>* slot,
	uint64_t expectedVersion
) {
	if (!slot || expectedVersion == 0 || vtIsLock(expectedVersion)) {
		return false;
	}
	uint64_t v = slot->load(std::memory_order_acquire);
	return v == expectedVersion;
}

bool VerificationTable::populateVersion(
	std::atomic<uint64_t>* slot,
	uint64_t newVersion
) {
	if (!slot || !vtIsVersion(newVersion)) {
		// Defensive: only ever publish a real version (rejects 0, locks, and
		// settled-empty bit patterns).
		return false;
	}
	uint64_t expected = slot->load(std::memory_order_acquire);
	while (true) {
		if (vtIsLock(expected)) {
			return false; // never overwrite a lock
		}
		if (expected == newVersion) {
			return true; // already there
		}
		if (slot->compare_exchange_weak(
				expected,
				newVersion,
				std::memory_order_release,
				std::memory_order_acquire
			)) {
			return true;
		}
		// expected has been updated by CAS to current value; loop
	}
}

bool VerificationTable::populateVersionIfUnchanged(
	std::atomic<uint64_t>* slot,
	uint64_t observed,
	uint64_t newVersion
) {
	if (!slot || !vtIsVersion(newVersion)) {
		return false;
	}
	if (vtIsLock(observed)) {
		// A write is in flight on this slot — never publish over it.
		return false;
	}
	if (observed == newVersion) {
		return true; // already current; nothing to do
	}
	// Single CAS from the pre-read value. If any write cycle intervened the slot
	// no longer equals `observed` (it moved to a lock and then to a fresh settle
	// generation), so this fails and we leave the slot cold rather than publish a
	// possibly-stale version.
	return slot->compare_exchange_strong(
		observed,
		newVersion,
		std::memory_order_release,
		std::memory_order_acquire
	);
}

uint64_t VerificationTable::extractVersionFromValue(const rocksdb::Slice& value) {
	if (value.size() < sizeof(uint64_t)) {
		return 0;
	}
	uint64_t be = loadUnaligned64(reinterpret_cast<const uint8_t*>(value.data()));
	return toHostEndian(be);
}

uint16_t vtNextGen() {
	return vtGlobalGen.fetch_add(1, std::memory_order_relaxed) & 0x3FFF;
}

uint64_t vtNextSettleGen() {
	return vtGlobalSettleGen.fetch_add(1, std::memory_order_relaxed) & VT_SETTLED_GEN_MASK;
}

LockTracker* VerificationTable::lockSlotForWrite(std::atomic<uint64_t>* slot, uintptr_t dbPtr) {
	if (!slot) return nullptr;
	std::lock_guard<std::mutex> lock(writerMutex_);
	uint64_t v = slot->load(std::memory_order_acquire);
	if (vtIsLock(v)) {
		// Slot already locked by another (or our own earlier) writer — join the
		// existing tracker as an additional holder so the slot stays locked, and
		// is only cleared, until every holder releases. This preserves the
		// invariant that a verifiable (cacheable) version is never published
		// while any write to a colliding key is still pending.
		LockTracker* t = vtDecodeLock(v);
		t->holders.fetch_add(1, std::memory_order_relaxed);
		t->refcount.fetch_add(1, std::memory_order_relaxed);
		return t;
	}
	// Slot holds 0 or a version — install a fresh tracker.
	uint16_t gen = vtNextGen();
	LockTracker* t = new LockTracker(slotIndexOf(slot), gen, dbPtr);
	t->holders.store(1, std::memory_order_relaxed);
	t->refcount.store(1, std::memory_order_relaxed); // 1 reference == this holder
	slot->store(vtEncodeLock(t, gen), std::memory_order_release);
	return t;
}

void VerificationTable::releaseWriteIntent(std::atomic<uint64_t>* slot, LockTracker* t) {
	if (!t) return;
	std::lock_guard<std::mutex> lock(writerMutex_);
	bool lastHolder = (t->holders.fetch_sub(1, std::memory_order_acq_rel) == 1);
	if (lastHolder) {
		// Last writer out: settle the slot to a fresh settled-empty generation
		// (only if it still holds our lock — a concurrent cancelForDB / populate
		// may already have moved it) and wake any parked waiters. Settling to a
		// new generation rather than 0 is what defeats ABA on the cold populate:
		// a reader that observed the pre-write state can never CAS its (now stale)
		// version into this slot, because the slot no longer equals what it saw.
		uint64_t expected = vtEncodeLock(t, t->generation);
		slot->compare_exchange_strong(expected, vtEncodeSettled(vtNextSettleGen()),
		    std::memory_order_release, std::memory_order_acquire);
		t->wake();
	}
	if (t->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) delete t;
}

LockTracker* VerificationTable::refTrackerIfLocked(std::atomic<uint64_t>* slot) {
	if (!slot) return nullptr;
	std::lock_guard<std::mutex> lock(writerMutex_);
	uint64_t v = slot->load(std::memory_order_acquire);
	if (!vtIsLock(v)) return nullptr;
	LockTracker* t = vtDecodeLock(v);
	t->refcount.fetch_add(1, std::memory_order_relaxed);
	return t;
}

void VerificationTable::unrefTracker(LockTracker* t) {
	if (!t) return;
	std::lock_guard<std::mutex> lock(writerMutex_);
	if (t->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) delete t;
}

void VerificationTable::settleAllSlots() {
	if (!slots_) return;
	size_t n = mask_ + 1;
	// One fresh generation shared by every slot in this sweep: the CAS is
	// per-slot, so uniqueness only matters versus values a reader may have
	// previously observed in that slot — a just-minted generation satisfies
	// that for all slots, and hoisting avoids n atomic fetch-adds.
	const uint64_t settledValue = vtEncodeSettled(vtNextSettleGen());
	for (size_t i = 0; i < n; ++i) {
		uint64_t v = slots_[i].load(std::memory_order_acquire);
		// Skip lock slots — the write-intent lifecycle (releaseWriteIntent)
		// advances them to a fresh settled-empty when the last holder releases.
		// A concurrent lockSlotForWrite uses an unconditional store (under
		// writerMutex_) that can race with our CAS; if it wins, the slot is a
		// lock that will advance further when committed/aborted — still correct.
		if (vtIsLock(v)) continue;
		// CAS non-lock (version or settled-empty) to the sweep generation,
		// retrying until the CAS lands or the slot turns into a lock (each
		// failed CAS reloads v; a lock exits via the loop condition).
		while (!vtIsLock(v)) {
			if (slots_[i].compare_exchange_strong(
					v,
					settledValue,
					std::memory_order_release,
					std::memory_order_acquire)) {
				break;
			}
		}
	}
}

void VerificationTable::cancelForDB(uintptr_t dbPtr) {
	if (!slots_) return;
	std::lock_guard<std::mutex> lock(writerMutex_);
	size_t n = mask_ + 1;
	for (size_t i = 0; i < n; ++i) {
		uint64_t v = slots_[i].load(std::memory_order_acquire);
		if (!vtIsLock(v)) continue;

		// Holding writerMutex_ means no concurrent release can free this tracker
		// out from under us, so the loaded pointer is safe to dereference.
		LockTracker* t = vtDecodeLock(v);
		uint16_t gen = vtGenFromLock(v);
		if (t->dbPtr != dbPtr) continue;

		// Settle the slot to a fresh settled-empty generation; may fail
		// harmlessly if it changed. (Like releaseWriteIntent, never back to 0.)
		uint64_t expected = vtEncodeLock(t, gen);
		slots_[i].compare_exchange_strong(expected, vtEncodeSettled(vtNextSettleGen()),
		    std::memory_order_release, std::memory_order_acquire);

		// Wake any parked TSFN waiters — idempotent if already woken. The
		// outstanding holders' releaseWriteIntent calls will free the tracker;
		// cancelForDB only clears the slot and unparks waiters.
		t->wake();
	}
}

bool LockTracker::addWakeCallback(std::function<void()> cb) {
	std::lock_guard<std::mutex> lock(wakeCallbacksMutex);
	if (woken) {
		return false;
	}
	wakeCallbacks.push_back(std::move(cb));
	return true;
}

void LockTracker::wake() {
	std::vector<std::function<void()>> cbs;
	{
		std::lock_guard<std::mutex> lock(wakeCallbacksMutex);
		woken = true;
		cbs.swap(wakeCallbacks);
	}
	for (auto& cb : cbs) cb();
}

} // namespace rocksdb_js
