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
		// package.json may pin a downstream prebuild revision suffix (e.g.
		// "11.1.2-1") that RocksDB's own version.h does not encode. Compare
		// against the base MAJOR.MINOR.PATCH, ignoring any "-N" suffix.
		std::string expectedBase = expected.substr(0, expected.find('-'));
		EXPECT_EQ(version, expectedBase)
			<< "Linked librocksdb version should match package.json rocksdb.version "
			<< "(base version, ignoring any -N prebuild revision suffix)";
	} else {
		EXPECT_FALSE(version.empty());
	}
}
