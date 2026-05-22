#ifndef __CORE_ENCODING_H__
#define __CORE_ENCODING_H__

#include <cstdint>

namespace rocksdb_js {

// Big-endian encoding/decoding helpers for transaction log format
inline void writeUint64BE(char* buffer, uint64_t value) {
	buffer[0] = static_cast<char>((value >> 56) & 0xFF);
	buffer[1] = static_cast<char>((value >> 48) & 0xFF);
	buffer[2] = static_cast<char>((value >> 40) & 0xFF);
	buffer[3] = static_cast<char>((value >> 32) & 0xFF);
	buffer[4] = static_cast<char>((value >> 24) & 0xFF);
	buffer[5] = static_cast<char>((value >> 16) & 0xFF);
	buffer[6] = static_cast<char>((value >> 8) & 0xFF);
	buffer[7] = static_cast<char>(value & 0xFF);
}

inline void writeUint32BE(char* buffer, uint32_t value) {
	buffer[0] = static_cast<char>((value >> 24) & 0xFF);
	buffer[1] = static_cast<char>((value >> 16) & 0xFF);
	buffer[2] = static_cast<char>((value >> 8) & 0xFF);
	buffer[3] = static_cast<char>(value & 0xFF);
}

inline void writeUint16BE(char* buffer, uint16_t value) {
	buffer[0] = static_cast<char>((value >> 8) & 0xFF);
	buffer[1] = static_cast<char>(value & 0xFF);
}

inline void writeUint8(char* buffer, uint8_t value) {
	buffer[0] = static_cast<char>(value);
}

inline uint64_t readUint64BE(const char* buffer) {
	return (static_cast<uint64_t>(static_cast<uint8_t>(buffer[0])) << 56) |
	       (static_cast<uint64_t>(static_cast<uint8_t>(buffer[1])) << 48) |
	       (static_cast<uint64_t>(static_cast<uint8_t>(buffer[2])) << 40) |
	       (static_cast<uint64_t>(static_cast<uint8_t>(buffer[3])) << 32) |
	       (static_cast<uint64_t>(static_cast<uint8_t>(buffer[4])) << 24) |
	       (static_cast<uint64_t>(static_cast<uint8_t>(buffer[5])) << 16) |
	       (static_cast<uint64_t>(static_cast<uint8_t>(buffer[6])) << 8) |
	       static_cast<uint64_t>(static_cast<uint8_t>(buffer[7]));
}

inline uint32_t readUint32BE(const char* buffer) {
	return (static_cast<uint32_t>(static_cast<uint8_t>(buffer[0])) << 24) |
	       (static_cast<uint32_t>(static_cast<uint8_t>(buffer[1])) << 16) |
	       (static_cast<uint32_t>(static_cast<uint8_t>(buffer[2])) << 8) |
	       static_cast<uint32_t>(static_cast<uint8_t>(buffer[3]));
}

inline uint16_t readUint16BE(const char* buffer) {
	return (static_cast<uint16_t>(static_cast<uint8_t>(buffer[0])) << 8) |
	       static_cast<uint16_t>(static_cast<uint8_t>(buffer[1]));
}

inline uint8_t readUint8(const char* buffer) {
	return static_cast<uint8_t>(buffer[0]);
}

inline void writeDoubleBE(char* buffer, double value) {
	union {
		double d;
		uint64_t u;
	} converter;
	converter.d = value;
	writeUint64BE(buffer, converter.u);
}

inline double readDoubleBE(const char* buffer) {
	union {
		double d;
		uint64_t u;
	} converter;
	converter.u = readUint64BE(buffer);
	return converter.d;
}

} // namespace rocksdb_js

#endif
