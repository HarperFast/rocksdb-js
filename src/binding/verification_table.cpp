#include "verification_table.h"
#include <cstring>
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

namespace {

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
	if (!slot || newVersion == 0 || vtIsLock(newVersion)) {
		// Defensive: never write 0 (means "empty") or a lock-tagged value
		// via populate.
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

uint64_t VerificationTable::extractVersionFromValue(const rocksdb::Slice& value) {
	if (value.size() < sizeof(uint64_t)) {
		return 0;
	}
	uint64_t be = loadUnaligned64(reinterpret_cast<const uint8_t*>(value.data()));
	return toHostEndian(be);
}

} // namespace rocksdb_js
