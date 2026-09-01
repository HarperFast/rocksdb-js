#include <gtest/gtest.h>
#include <initializer_list>
#include <string>
#include <utility>
#include <vector>
#include "core/destroy_layout.h"

using rocksdb_js::DBFileLayout;
using rocksdb_js::updateRetainedDestroyLayout;

namespace {

std::vector<rocksdb::DbPath> paths(std::initializer_list<const char*> dirs) {
	std::vector<rocksdb::DbPath> result;
	for (const char* dir : dirs) {
		result.emplace_back(dir, 0);
	}
	return result;
}

std::vector<std::string> pathNames(const DBFileLayout& layout) {
	std::vector<std::string> result;
	for (const auto& path : layout.dbPaths) {
		result.push_back(path.path);
	}
	return result;
}

}

TEST(DestroyLayout, ReadOnlyOpenCannotEstablishExternalPaths) {
	DBFileLayout retained;
	DBFileLayout reader{ paths({ "/data/db", "/data/neighbor" }), { { "default", "" } } };

	EXPECT_FALSE(updateRetainedDestroyLayout(retained, std::move(reader), false));
	EXPECT_TRUE(retained.dbPaths.empty());
	EXPECT_EQ(retained.blobDirs.at("default"), "");
}

TEST(DestroyLayout, WritableOpenEstablishesAndAppendsPaths) {
	DBFileLayout retained;
	EXPECT_TRUE(updateRetainedDestroyLayout(
		retained,
		DBFileLayout{ paths({ "/data/db" }), {} },
		true
	));
	EXPECT_TRUE(updateRetainedDestroyLayout(
		retained,
		DBFileLayout{ paths({ "/data/db", "/data/cold" }), {} },
		true
	));
	EXPECT_EQ(pathNames(retained), (std::vector<std::string>{ "/data/db", "/data/cold" }));
}

TEST(DestroyLayout, ShorterAndDivergentWritableListsDoNotReplaceTheRecord) {
	DBFileLayout retained{ paths({ "/data/db", "/data/cold" }), {} };

	EXPECT_FALSE(updateRetainedDestroyLayout(
		retained,
		DBFileLayout{ paths({ "/data/db" }), {} },
		true
	));
	EXPECT_FALSE(updateRetainedDestroyLayout(
		retained,
		DBFileLayout{ paths({ "/data/db", "/data/neighbor" }), {} },
		true
	));
	EXPECT_EQ(pathNames(retained), (std::vector<std::string>{ "/data/db", "/data/cold" }));
}

TEST(DestroyLayout, EmptyBlobDirectoryIsAnAuthoritativeReplacement) {
	DBFileLayout retained{ {}, { { "default", "/data/old-blobs" } } };
	DBFileLayout relocated{ {}, { { "default", "" } } };

	EXPECT_TRUE(updateRetainedDestroyLayout(retained, std::move(relocated), true));
	ASSERT_TRUE(retained.blobDirs.contains("default"));
	EXPECT_EQ(retained.blobDirs.at("default"), "");
}
