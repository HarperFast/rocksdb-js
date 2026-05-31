#include <gtest/gtest.h>
#include <cstdlib>
#include <string>
#include "rocksdb/version.h"

namespace {

std::string expectedVersion() {
	if (const char* env = std::getenv("ROCKSDB_EXPECTED_VERSION")) {
		return env;
	}
#ifdef ROCKSDB_EXPECTED_VERSION
	return ROCKSDB_EXPECTED_VERSION;
#else
	return "";
#endif
}

} // namespace

TEST(RocksDBVersion, MatchesPackagePin) {
	std::string version = rocksdb::GetRocksVersionAsString();
	ASSERT_FALSE(version.empty());

	std::string expected = expectedVersion();
	if (!expected.empty()) {
		EXPECT_EQ(version, expected) << "Linked librocksdb version should match package.json rocksdb.version";
	} else {
		EXPECT_FALSE(version.empty());
	}
}
