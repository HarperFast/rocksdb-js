#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include "core/open_status.h"
#include "rocksdb/status.h"

using rocksdb_js::isMissingSstOpenRace;

// The Corruption-coded shape observed in the wild (HarperFast/rocksdb-js#812):
// RocksDB wraps the missing-file IO error and blames the MANIFEST.
TEST(OpenStatus, CorruptionWrappingMissingSstIsTheRace) {
	rocksdb::Status status = rocksdb::Status::Corruption(
		"IO error: No such file or directory: While open a file for random read: "
		"/data/db/000046.sst: No such file or directory",
		"The file /data/db/MANIFEST-000005 may be corrupted");
	EXPECT_TRUE(isMissingSstOpenRace(status));
}

// The IOError-coded shape: the open trips on the missing file directly.
TEST(OpenStatus, IOErrorNamingMissingSstIsTheRace) {
	rocksdb::Status status = rocksdb::Status::IOError(
		"While open a file for random read: /data/db/000046.sst",
		"No such file or directory");
	EXPECT_TRUE(isMissingSstOpenRace(status));
}

TEST(OpenStatus, PathNotFoundSubcodeNamingSstIsTheRace) {
	rocksdb::Status status = rocksdb::Status::PathNotFound(
		"While open a file for random read: /data/db/000046.sst");
	EXPECT_TRUE(isMissingSstOpenRace(status));
}

TEST(OpenStatus, WindowsNotFoundTextNamingSstIsTheRace) {
	rocksdb::Status status = rocksdb::Status::IOError(
		"While open a file for random read: C:\\data\\db\\000046.sst",
		"The system cannot find the file specified.");
	EXPECT_TRUE(isMissingSstOpenRace(status));
}

TEST(OpenStatus, LocalizedCorruptionUsesNamedFileAbsence) {
	auto root = std::filesystem::temp_directory_path() /
		("rocksdb-js-open-status-" + std::to_string(
			std::chrono::steady_clock::now().time_since_epoch().count()
		));
	std::filesystem::create_directories(root);
	rocksdb::Status status = rocksdb::Status::Corruption(
		"E/A-Fehler: Datei nicht gefunden: /data/db/000046.sst",
		"Die Manifestdatei ist möglicherweise beschädigt"
	);
	EXPECT_TRUE(isMissingSstOpenRace(status, root));

	{
		std::ofstream existing(root / "000046.sst");
		existing << "present";
	}
	EXPECT_FALSE(isMissingSstOpenRace(status, root));
	std::filesystem::remove_all(root);
}

TEST(OpenStatus, LocalizedCorruptionRequiresNumberedRocksFilename) {
	auto root = std::filesystem::temp_directory_path() /
		("rocksdb-js-open-status-" + std::to_string(
			std::chrono::steady_clock::now().time_since_epoch().count()
		));
	std::filesystem::create_directories(root);
	rocksdb::Status status = rocksdb::Status::Corruption(
		"E/A-Fehler: Datei nicht gefunden: /data/db/archive123.sst"
	);
	EXPECT_FALSE(isMissingSstOpenRace(status, root));
	std::filesystem::remove_all(root);
}

// A Windows status can end the line right after the filename (FormatMessage
// text is CRLF-terminated), so the filename token must end on \r as well —
// otherwise the classifier misses on the one platform whose wording it
// explicitly matches.
TEST(OpenStatus, CrlfTerminatedFilenameIsTheRace) {
	rocksdb::Status status = rocksdb::Status::IOError(
		"While open a file for random read: C:\\data\\db\\000046.sst\r\n"
		"The system cannot find the file specified.\r\n");
	EXPECT_TRUE(isMissingSstOpenRace(status));
}

// Real corruption inside an SST names the file but carries no not-found
// signal; it must keep failing as corruption.
TEST(OpenStatus, ChecksumMismatchInSstIsNotTheRace) {
	rocksdb::Status status = rocksdb::Status::Corruption(
		"block checksum mismatch: stored = 123, computed = 456 in /data/db/000046.sst offset 0 size 4096");
	EXPECT_FALSE(isMissingSstOpenRace(status));
}

// A corrupt MANIFEST (bad record, no missing SST) must keep failing as
// corruption.
TEST(OpenStatus, CorruptManifestIsNotTheRace) {
	rocksdb::Status status = rocksdb::Status::Corruption(
		"CURRENT points to /data/db/MANIFEST-000005", "checksum mismatch");
	EXPECT_FALSE(isMissingSstOpenRace(status));
}

// A database that does not exist fails on CURRENT — a not-found with no .sst.
TEST(OpenStatus, MissingCurrentFileIsNotTheRace) {
	rocksdb::Status status = rocksdb::Status::PathNotFound(
		"While opening a file for sequentially read: /data/db/CURRENT",
		"No such file or directory");
	EXPECT_FALSE(isMissingSstOpenRace(status));
}

// Blob files race at least as readily as SSTs: values >= 2KB live in blob
// files and blob GC deletes rewritten ones continuously under write traffic.
TEST(OpenStatus, IOErrorNamingMissingBlobIsTheRace) {
	rocksdb::Status status = rocksdb::Status::IOError(
		"While open a file for random read: /data/db/000926.blob",
		"No such file or directory");
	EXPECT_TRUE(isMissingSstOpenRace(status));
}

// A WAL segment deleted by the writer's concurrent flush trips the open's WAL
// replay the same way ("while stat a file for size: 000053.log").
TEST(OpenStatus, IOErrorNamingMissingWalIsTheRace) {
	rocksdb::Status status = rocksdb::Status::IOError(
		"While stat a file for size: /data/db/000053.log",
		"No such file or directory");
	EXPECT_TRUE(isMissingSstOpenRace(status));
}

// A missing column family is InvalidArgument, not an IO shape at all.
TEST(OpenStatus, MissingColumnFamilyIsNotTheRace) {
	rocksdb::Status status =
		rocksdb::Status::InvalidArgument("Column family not found", "baz");
	EXPECT_FALSE(isMissingSstOpenRace(status));
}

TEST(OpenStatus, OkIsNotTheRace) {
	EXPECT_FALSE(isMissingSstOpenRace(rocksdb::Status::OK()));
}

// The extension must end a filename token: a DIRECTORY named like an SST/WAL
// must not ride the path text into the race classification.
TEST(OpenStatus, DirectoryNamedLikeExtensionIsNotTheRace) {
	rocksdb::Status status = rocksdb::Status::PathNotFound(
		"While opening a file for sequentially read: /data/exports.sst/CURRENT",
		"No such file or directory");
	EXPECT_FALSE(isMissingSstOpenRace(status));
	status = rocksdb::Status::IOError(
		"While open a file for random read: /data/archive.log/MANIFEST-000005",
		"No such file or directory");
	EXPECT_FALSE(isMissingSstOpenRace(status));
}

// ...while a real file of that name inside such a directory still matches.
TEST(OpenStatus, MissingSstInsideOddDirectoryIsTheRace) {
	rocksdb::Status status = rocksdb::Status::IOError(
		"While open a file for random read: /data/archive.log/000046.sst",
		"No such file or directory");
	EXPECT_TRUE(isMissingSstOpenRace(status));
}
