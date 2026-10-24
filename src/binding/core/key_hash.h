#ifndef __CORE_KEY_HASH_H__
#define __CORE_KEY_HASH_H__

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace rocksdb_js {

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

} // namespace rocksdb_js

#endif
