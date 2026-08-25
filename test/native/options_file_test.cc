#include <gtest/gtest.h>
#include <string>
#include "core/options_file.h"

using rocksdb_js::findPersistedBlobDir;

// The only caller is compiled into an UNPATCHED build, where nothing can create
// a database carrying a `blob_dir` — so these cases are the only thing that ever
// executes the scan. Its failure mode is the one it exists to prevent: an
// unpatched build opens a patched database cleanly and every value at or above
// `min_blob_size` reads as missing.

TEST(OptionsFile, NoBlobDirAtAll) {
	EXPECT_FALSE(
		findPersistedBlobDir(
			"[Version]\n  rocksdb_version=11.8.1\n\n"
			"[CFOptions \"default\"]\n  min_blob_size=2048\n  enable_blob_files=true\n"
		)
			.has_value()
	);
}

TEST(OptionsFile, EmptyContents) {
	EXPECT_FALSE(findPersistedBlobDir("").has_value());
}

TEST(OptionsFile, UnsetBlobDirIsNotAValue) {
	EXPECT_FALSE(
		findPersistedBlobDir("[CFOptions \"default\"]\n  blob_dir=\n  min_blob_size=2048\n")
			.has_value()
	);
}

TEST(OptionsFile, FindsAValue) {
	auto found = findPersistedBlobDir("[CFOptions \"default\"]\n  blob_dir=/mnt/blobs\n");
	ASSERT_TRUE(found.has_value());
	EXPECT_EQ(*found, "/mnt/blobs");
}

TEST(OptionsFile, FindsAValueOnTheLastLineWithoutATrailingNewline) {
	auto found = findPersistedBlobDir("[CFOptions \"t1\"]\n  blob_dir=/mnt/blobs");
	ASSERT_TRUE(found.has_value());
	EXPECT_EQ(*found, "/mnt/blobs");
}

TEST(OptionsFile, StopsAtCarriageReturn) {
	auto found = findPersistedBlobDir("[CFOptions \"t1\"]\r\n  blob_dir=/mnt/blobs\r\n");
	ASSERT_TRUE(found.has_value());
	EXPECT_EQ(*found, "/mnt/blobs");
}

// The scan runs over the whole file rather than per column family, so a family
// that sets one must be found no matter which one it is. This is the shape that
// actually reaches an unpatched build: `default` is created flat on the way to a
// named family, and only the named family carries the directory.
TEST(OptionsFile, FindsALaterColumnFamilysValueAfterAnUnsetOne) {
	auto found = findPersistedBlobDir(
		"[CFOptions \"default\"]\n  blob_dir=\n\n[CFOptions \"table1\"]\n  blob_dir=/mnt/blobs\n"
	);
	ASSERT_TRUE(found.has_value());
	EXPECT_EQ(*found, "/mnt/blobs");
}

// A whole-key match, not a substring: a longer option ending in `blob_dir=`
// would otherwise be read as this field.
TEST(OptionsFile, IgnoresALongerKeyEndingInBlobDir) {
	EXPECT_FALSE(
		findPersistedBlobDir("[CFOptions \"default\"]\n  wal_blob_dir=/mnt/elsewhere\n").has_value()
	);
}

TEST(OptionsFile, FindsARealKeyAfterALongerOne) {
	auto found = findPersistedBlobDir(
		"[CFOptions \"default\"]\n  wal_blob_dir=/mnt/elsewhere\n  blob_dir=/mnt/blobs\n"
	);
	ASSERT_TRUE(found.has_value());
	EXPECT_EQ(*found, "/mnt/blobs");
}

TEST(OptionsFile, AcceptsATabIndentedKey) {
	auto found = findPersistedBlobDir("[CFOptions \"t1\"]\n\tblob_dir=/mnt/blobs\n");
	ASSERT_TRUE(found.has_value());
	EXPECT_EQ(*found, "/mnt/blobs");
}

TEST(OptionsFile, KeepsSpacesInsideAValue) {
	auto found = findPersistedBlobDir("[CFOptions \"t1\"]\n  blob_dir=/mnt/my blobs\n");
	ASSERT_TRUE(found.has_value());
	EXPECT_EQ(*found, "/mnt/my blobs");
}
