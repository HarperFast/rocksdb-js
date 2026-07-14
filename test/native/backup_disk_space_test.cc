#include <gtest/gtest.h>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include "database/backup_disk_space.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

using rocksdb_js::checkBackupDiskSpace;

namespace {

// An Env whose GetFreeSpace returns a scripted result, delegating everything
// else to a real Env. Lets the test drive the reject/degrade branches
// deterministically without needing a volume of a particular size.
class FakeFreeSpaceEnv : public rocksdb::EnvWrapper {
public:
	FakeFreeSpaceEnv(rocksdb::Env* base, rocksdb::Status status, uint64_t freeBytes) :
		rocksdb::EnvWrapper(base),
		status_(status),
		freeBytes_(freeBytes) {}

	rocksdb::Status GetFreeSpace(const std::string& /*path*/, uint64_t* diskfree) override {
		if (!status_.ok()) {
			return status_;
		}
		*diskfree = freeBytes_;
		return rocksdb::Status::OK();
	}

private:
	rocksdb::Status status_;
	uint64_t freeBytes_;
};

struct OpenDb {
	std::string dir;
	std::unique_ptr<rocksdb::DB> db;

	explicit OpenDb(const char* name) {
		dir = (std::filesystem::temp_directory_path() / name).string();
		std::filesystem::remove_all(dir);
		rocksdb::Options options;
		options.create_if_missing = true;
		rocksdb::Status s = rocksdb::DB::Open(options, dir, &db);
		EXPECT_TRUE(s.ok()) << s.ToString();
		// A little data so the live-file footprint is non-zero after a flush.
		if (db) {
			db->Put(rocksdb::WriteOptions(), "k", std::string(4096, 'v'));
			db->Flush(rocksdb::FlushOptions());
		}
	}

	~OpenDb() {
		db.reset();
		std::filesystem::remove_all(dir);
	}
};

} // namespace

TEST(BackupDiskSpace, RejectsWhenFreeSpaceBelowRequired) {
	OpenDb h("rocksdb-js-backup-disk-space-reject");
	ASSERT_TRUE(h.db);

	// One byte free cannot fit any non-empty database.
	FakeFreeSpaceEnv env(rocksdb::Env::Default(), rocksdb::Status::OK(), 1);
	rocksdb::Status s = checkBackupDiskSpace(h.db.get(), h.dir, /*flushBeforeBackup=*/false, /*additionalRequiredBytes=*/0, &env);
	EXPECT_TRUE(s.IsNoSpace()) << s.ToString();
}

TEST(BackupDiskSpace, PassesWhenAmpleFreeSpace) {
	OpenDb h("rocksdb-js-backup-disk-space-ample");
	ASSERT_TRUE(h.db);

	FakeFreeSpaceEnv env(rocksdb::Env::Default(), rocksdb::Status::OK(), std::numeric_limits<uint64_t>::max());
	rocksdb::Status s = checkBackupDiskSpace(h.db.get(), h.dir, /*flushBeforeBackup=*/false, /*additionalRequiredBytes=*/0, &env);
	EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST(BackupDiskSpace, DegradesToOkWhenFreeSpaceUnsupported) {
	OpenDb h("rocksdb-js-backup-disk-space-unsupported");
	ASSERT_TRUE(h.db);

	// GetFreeSpace failing (e.g. a filesystem that doesn't implement it) must not
	// block the backup: the required size is unknowable, so skip the check.
	FakeFreeSpaceEnv env(rocksdb::Env::Default(), rocksdb::Status::NotSupported("no free space"), 0);
	rocksdb::Status s = checkBackupDiskSpace(h.db.get(), h.dir, /*flushBeforeBackup=*/false, /*additionalRequiredBytes=*/0, &env);
	EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST(BackupDiskSpace, DegradesToOkWhenFreeSpaceReportsZero) {
	OpenDb h("rocksdb-js-backup-disk-space-zero");
	ASSERT_TRUE(h.db);

	// Some network filesystems report 0 free bytes rather than erroring; treat it
	// as unknown and skip rather than reject a backup that would likely succeed.
	FakeFreeSpaceEnv env(rocksdb::Env::Default(), rocksdb::Status::OK(), 0);
	rocksdb::Status s = checkBackupDiskSpace(h.db.get(), h.dir, /*flushBeforeBackup=*/false, /*additionalRequiredBytes=*/0, &env);
	EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST(BackupDiskSpace, RealEnvHasSpaceForTinyDb) {
	OpenDb h("rocksdb-js-backup-disk-space-realenv");
	ASSERT_TRUE(h.db);

	// The real default Env against a temp dir on a normal machine has room for a
	// few-KB database — guards against GetFreeSpace/GetLiveFilesStorageInfo
	// wiring regressions on each platform.
	rocksdb::Status s =
		checkBackupDiskSpace(h.db.get(), h.dir, /*flushBeforeBackup=*/true, /*additionalRequiredBytes=*/0, rocksdb::Env::Default());
	EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST(BackupDiskSpace, AdditionalRequiredBytesCanTipOverFree) {
	OpenDb h("rocksdb-js-backup-disk-space-additional");
	ASSERT_TRUE(h.db);

	// A modest amount of free space that comfortably fits the tiny DB's live
	// files, so with no extra payload the check passes...
	const uint64_t freeBytes = 64ull * 1024 * 1024; // 64 MiB
	FakeFreeSpaceEnv env(rocksdb::Env::Default(), rocksdb::Status::OK(), freeBytes);
	rocksdb::Status ok = checkBackupDiskSpace(h.db.get(), h.dir, /*flushBeforeBackup=*/false, /*additionalRequiredBytes=*/0, &env);
	EXPECT_TRUE(ok.ok()) << ok.ToString();

	// ...but a transaction-log snapshot larger than the free space must reject,
	// even though the RocksDB live files alone would have fit.
	rocksdb::Status noSpace =
		checkBackupDiskSpace(h.db.get(), h.dir, /*flushBeforeBackup=*/false, /*additionalRequiredBytes=*/freeBytes + 1, &env);
	EXPECT_TRUE(noSpace.IsNoSpace()) << noSpace.ToString();
}
