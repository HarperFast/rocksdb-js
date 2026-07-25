#include <gtest/gtest.h>
#include <algorithm>
#include <string>
#include <vector>
#include "core/compression.h"
#include "rocksdb/convenience.h"
#include "rocksdb/options.h"

using rocksdb_js::supportedCompressionNames;
using rocksdb_js::tryResolveCompressionType;

namespace {

bool isLinkedIn(rocksdb::CompressionType type) {
	const std::vector<rocksdb::CompressionType> supported = rocksdb::GetSupportedCompressions();
	return std::find(supported.begin(), supported.end(), type) != supported.end();
}

} // namespace

// `none` requires no compressor, so it resolves in every build. This is the
// case the resolver must never gate on GetSupportedCompressions(), which does
// not report kNoCompression.
TEST(CompressionTest, ResolvesNoneInAnyBuild) {
	rocksdb::CompressionType type = rocksdb::kZlibCompression;
	std::string error;
	ASSERT_TRUE(tryResolveCompressionType("none", type, error)) << error;
	EXPECT_EQ(type, rocksdb::kNoCompression);
	EXPECT_TRUE(error.empty());
}

TEST(CompressionTest, RejectsUnknownNameAndListsAlternatives) {
	rocksdb::CompressionType type = rocksdb::kNoCompression;
	std::string error;
	EXPECT_FALSE(tryResolveCompressionType("gzip", type, error));
	EXPECT_NE(error.find("Unknown compression type"), std::string::npos);
	// The message must name what the caller can actually use instead.
	EXPECT_NE(error.find(supportedCompressionNames()), std::string::npos);
}

// Names are matched exactly: the TypeScript layer lowercases before handing
// them over, so an uppercase name reaching here is a caller bug, not a variant
// to silently accept.
TEST(CompressionTest, RejectsNonLowercaseName) {
	rocksdb::CompressionType type = rocksdb::kNoCompression;
	std::string error;
	EXPECT_FALSE(tryResolveCompressionType("ZLIB", type, error));
	EXPECT_FALSE(tryResolveCompressionType("", type, error));
}

// The point of the resolver: a known name whose compressor was not linked in
// must fail loudly instead of letting RocksDB degrade to writing uncompressed
// table files. Which algorithms are linked varies per build, so assert the
// rule rather than a fixed set.
TEST(CompressionTest, EveryKnownNameEitherResolvesOrReportsUnsupported) {
	const char* names[] = {"none", "snappy", "zlib", "bzip2", "lz4", "lz4hc", "zstd"};
	for (const char* name : names) {
		rocksdb::CompressionType type = rocksdb::kNoCompression;
		std::string error;
		if (tryResolveCompressionType(name, type, error)) {
			EXPECT_TRUE(type == rocksdb::kNoCompression || isLinkedIn(type))
				<< name << " resolved but is not linked in";
			EXPECT_NE(supportedCompressionNames().find(name), std::string::npos)
				<< name << " resolved but is missing from the supported list";
		} else {
			EXPECT_NE(error.find("not supported by this RocksDB build"), std::string::npos)
				<< name << " failed with an unexpected error: " << error;
			EXPECT_EQ(supportedCompressionNames().find(name), std::string::npos)
				<< name << " is unsupported but listed as available";
		}
	}
}

// zlib backs the `compression: true` shorthand, and the prebuilt links libz
// explicitly for this reason. If this fails on a platform, that shorthand is
// broken there and needs a different default.
TEST(CompressionTest, ZlibIsAvailable) {
	rocksdb::CompressionType type = rocksdb::kNoCompression;
	std::string error;
	EXPECT_TRUE(tryResolveCompressionType("zlib", type, error)) << error;
	EXPECT_EQ(type, rocksdb::kZlibCompression);
}

TEST(CompressionTest, SupportedNamesAlwaysIncludesNone) {
	EXPECT_NE(supportedCompressionNames().find("none"), std::string::npos);
}
