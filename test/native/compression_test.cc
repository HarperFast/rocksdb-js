#include <gtest/gtest.h>
#include <algorithm>
#include <string>
#include <vector>
#include "core/compression.h"
#include "rocksdb/compression_type.h"

using rocksdb_js::compressionNameFromType;
using rocksdb_js::compressionTypeFromName;
using rocksdb_js::isCompressionSupported;
using rocksdb_js::supportedCompressionNames;

TEST(Compression, NameToTypeKnownAlgorithms) {
	EXPECT_EQ(compressionTypeFromName("none"), rocksdb::kNoCompression);
	EXPECT_EQ(compressionTypeFromName("snappy"), rocksdb::kSnappyCompression);
	EXPECT_EQ(compressionTypeFromName("zlib"), rocksdb::kZlibCompression);
	EXPECT_EQ(compressionTypeFromName("bzip2"), rocksdb::kBZip2Compression);
	EXPECT_EQ(compressionTypeFromName("lz4"), rocksdb::kLZ4Compression);
	EXPECT_EQ(compressionTypeFromName("lz4hc"), rocksdb::kLZ4HCCompression);
	EXPECT_EQ(compressionTypeFromName("xpress"), rocksdb::kXpressCompression);
	EXPECT_EQ(compressionTypeFromName("zstd"), rocksdb::kZSTD);
}

TEST(Compression, NameToTypeIsCaseInsensitive) {
	EXPECT_EQ(compressionTypeFromName("LZ4"), rocksdb::kLZ4Compression);
	EXPECT_EQ(compressionTypeFromName("ZStd"), rocksdb::kZSTD);
}

TEST(Compression, NameToTypeUnknownReturnsNullopt) {
	EXPECT_FALSE(compressionTypeFromName("gzip").has_value());
	EXPECT_FALSE(compressionTypeFromName("").has_value());
}

TEST(Compression, TypeToNameRoundTrips) {
	EXPECT_EQ(compressionNameFromType(rocksdb::kNoCompression), "none");
	EXPECT_EQ(compressionNameFromType(rocksdb::kLZ4Compression), "lz4");
	EXPECT_EQ(compressionNameFromType(rocksdb::kZSTD), "zstd");
}

TEST(Compression, TypeToNameUnknownReturnsUnknown) {
	EXPECT_EQ(compressionNameFromType(rocksdb::kCustomCompression80), "unknown");
}

TEST(Compression, NoCompressionAlwaysSupported) {
	EXPECT_TRUE(isCompressionSupported(rocksdb::kNoCompression));
}

TEST(Compression, SupportedListMatchesRuntimeSupport) {
	const std::vector<std::string>& names = supportedCompressionNames();
	// Every reported name must map back to a supported type, and "none" is
	// always present (GetSupportedCompressions always includes kNoCompression).
	EXPECT_NE(std::find(names.begin(), names.end(), "none"), names.end());
	for (const std::string& name : names) {
		auto type = compressionTypeFromName(name);
		ASSERT_TRUE(type.has_value()) << "unmapped name: " << name;
		EXPECT_TRUE(isCompressionSupported(*type)) << "unsupported name reported: " << name;
	}
}
