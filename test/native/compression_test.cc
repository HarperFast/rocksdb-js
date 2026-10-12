#include <gtest/gtest.h>
#include <algorithm>
#include <string>
#include <vector>
#include "core/compression.h"
#include "rocksdb/advanced_compression.h"
#include "rocksdb/compression_type.h"

using rocksdb_js::compressionNameFromType;
using rocksdb_js::compressionTypeFromName;
using rocksdb_js::isCompressionSupported;
using rocksdb_js::supportedCompressionNames;
using rocksdb_js::tableCompressionTypes;

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

namespace {

std::string builtinCompat() {
	return rocksdb::GetBuiltinV2CompressionManager()->CompatibilityName();
}

using Types = std::vector<rocksdb::CompressionType>;

} // namespace

TEST(Compression, TableCompressionLegacyNames) {
	EXPECT_EQ(tableCompressionTypes("NoCompression"), Types{});
	EXPECT_EQ(tableCompressionTypes("Snappy"), Types{ rocksdb::kSnappyCompression });
	EXPECT_EQ(tableCompressionTypes("LZ4"), Types{ rocksdb::kLZ4Compression });
	EXPECT_EQ(tableCompressionTypes("ZSTD"), Types{ rocksdb::kZSTD });
	EXPECT_EQ(tableCompressionTypes("ZSTDNotFinal"), Types{ rocksdb::kZSTD });
}

TEST(Compression, TableCompressionV7Encoding) {
	const std::string compat = builtinCompat();
	EXPECT_EQ(tableCompressionTypes(compat + ";04;"), Types{ rocksdb::kLZ4Compression });
	EXPECT_EQ(tableCompressionTypes(compat + ";0407;"), (Types{ rocksdb::kLZ4Compression, rocksdb::kZSTD }));
	EXPECT_EQ(tableCompressionTypes(compat + ";;"), Types{});
	EXPECT_EQ(tableCompressionTypes(";;"), Types{});
	EXPECT_EQ(tableCompressionTypes(compat + ";07;reserved"), Types{ rocksdb::kZSTD });
	EXPECT_EQ(tableCompressionTypes(compat + ";8a;"), Types{ rocksdb::kCustomCompression8A });
}

TEST(Compression, TableCompressionRejectsWhatItCannotInterpret) {
	const std::string compat = builtinCompat();
	EXPECT_FALSE(tableCompressionTypes("").has_value());
	EXPECT_FALSE(tableCompressionTypes("Gzip").has_value());
	EXPECT_FALSE(tableCompressionTypes("SomeCustomManager;80;").has_value());
	EXPECT_FALSE(tableCompressionTypes(compat + ";4;").has_value());
	EXPECT_FALSE(tableCompressionTypes(compat + ";zz;").has_value());
	EXPECT_FALSE(tableCompressionTypes(compat + ";04").has_value());
}
