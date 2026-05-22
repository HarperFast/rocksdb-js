#include <gtest/gtest.h>
#include <cstring>
#include "core/encoding.h"

using rocksdb_js::readDoubleBE;
using rocksdb_js::readUint16BE;
using rocksdb_js::readUint32BE;
using rocksdb_js::readUint64BE;
using rocksdb_js::readUint8;
using rocksdb_js::writeDoubleBE;
using rocksdb_js::writeUint16BE;
using rocksdb_js::writeUint32BE;
using rocksdb_js::writeUint64BE;
using rocksdb_js::writeUint8;

TEST(Encoding, Uint64RoundTrip) {
	char buf[8] = {};
	writeUint64BE(buf, 0x0123456789ABCDEFULL);
	EXPECT_EQ(readUint64BE(buf), 0x0123456789ABCDEFULL);
}

TEST(Encoding, Uint32RoundTrip) {
	char buf[4] = {};
	writeUint32BE(buf, 0xDEADBEEF);
	EXPECT_EQ(readUint32BE(buf), 0xDEADBEEFU);
}

TEST(Encoding, Uint16RoundTrip) {
	char buf[2] = {};
	writeUint16BE(buf, 0xABCD);
	EXPECT_EQ(readUint16BE(buf), 0xABCD);
}

TEST(Encoding, Uint8RoundTrip) {
	char buf[1] = {};
	writeUint8(buf, 0x42);
	EXPECT_EQ(readUint8(buf), 0x42);
}

TEST(Encoding, DoubleRoundTrip) {
	char buf[8] = {};
	const double value = 3.141592653589793;
	writeDoubleBE(buf, value);
	EXPECT_DOUBLE_EQ(readDoubleBE(buf), value);
}
