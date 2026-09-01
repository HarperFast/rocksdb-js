// Secondary instances against a blob-enabled database. Upstream documents the
// combination as unsupported (facebook/rocksdb#13296), but this codebase
// enables blob files unconditionally (values >= 2KB) and depends on the pinned
// build's actual behavior: blob files are opened and fd-held at version
// install exactly like SSTs under max_open_files = -1, which is what makes the
// primary's blob-GC deletions safe. These tests must stay green on every
// RocksDB upgrade — a regression here breaks secondary mode for every real
// database.
#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/convenience.h"

namespace {

std::filesystem::path spikeRoot() {
	return std::filesystem::temp_directory_path() / "rocksdb-js-secondary-spike";
}

rocksdb::Options blobOptions() {
	rocksdb::Options options;
	options.create_if_missing = true;
	options.enable_blob_files = true;
	options.min_blob_size = 2048;
	options.enable_blob_garbage_collection = true;
	// Aggressive GC so compaction rewrites blob files eagerly.
	options.blob_garbage_collection_age_cutoff = 1.0;
	options.blob_garbage_collection_force_threshold = 0.0;
	return options;
}

} // namespace

TEST(SecondarySpike, BlobEnabledDatabase) {
	auto root = spikeRoot();
	std::filesystem::remove_all(root);
	std::filesystem::create_directories(root);
	std::string primaryPath = (root / "primary").string();
	std::string secondaryPath = (root / "secondary").string();

	rocksdb::Options options = blobOptions();
	std::unique_ptr<rocksdb::DB> primary;
	rocksdb::Status s = rocksdb::DB::Open(options, primaryPath, &primary);
	ASSERT_TRUE(s.ok()) << s.ToString();

	std::string large(16 * 1024, 'x');
	ASSERT_TRUE(primary->Put({}, "small", "small-v1").ok());
	ASSERT_TRUE(primary->Put({}, "large", large).ok());
	ASSERT_TRUE(primary->Flush({}).ok());

	rocksdb::Options secondaryOptions = blobOptions();
	secondaryOptions.create_if_missing = false;
	secondaryOptions.max_open_files = -1;
	std::unique_ptr<rocksdb::DB> secondary;
	s = rocksdb::DB::OpenAsSecondary(secondaryOptions, primaryPath, secondaryPath, &secondary);
	ASSERT_TRUE(s.ok()) << s.ToString();

	// Blob-resident (>= min_blob_size) values must read through a secondary.
	std::string value;
	s = secondary->Get({}, "small", &value);
	EXPECT_TRUE(s.ok() && value == "small-v1") << s.ToString();
	s = secondary->Get({}, "large", &value);
	EXPECT_TRUE(s.ok() && value == large) << s.ToString();

	// Catch-up visibility: primary writes are invisible until
	// TryCatchUpWithPrimary, visible after.
	std::string large2(20 * 1024, 'y');
	ASSERT_TRUE(primary->Put({}, "large2", large2).ok());
	ASSERT_TRUE(primary->Flush({}).ok());
	s = secondary->Get({}, "large2", &value);
	EXPECT_TRUE(s.IsNotFound()) << s.ToString();
	s = secondary->TryCatchUpWithPrimary();
	EXPECT_TRUE(s.ok()) << s.ToString();
	s = secondary->Get({}, "large2", &value);
	EXPECT_TRUE(s.ok() && value == large2) << s.ToString();

	// The blob-GC hazard: overwrite the blob value and force compaction + blob
	// GC on the primary (rewrites blob files and deletes the originals). The
	// un-caught-up secondary must still serve the OLD blob — its version holds
	// the deleted file open — and see the new one after catch-up.
	std::string large3(16 * 1024, 'z');
	ASSERT_TRUE(primary->Put({}, "large", large3).ok());
	ASSERT_TRUE(primary->Flush({}).ok());
	rocksdb::CompactRangeOptions compactOptions;
	compactOptions.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForceOptimized;
	ASSERT_TRUE(primary->CompactRange(compactOptions, nullptr, nullptr).ok());

	s = secondary->Get({}, "large", &value);
	EXPECT_TRUE(s.ok() && value == large) << s.ToString();

	s = secondary->TryCatchUpWithPrimary();
	EXPECT_TRUE(s.ok()) << s.ToString();
	s = secondary->Get({}, "large", &value);
	EXPECT_TRUE(s.ok() && value == large3) << s.ToString();

	secondary.reset();
	primary.reset();
	std::filesystem::remove_all(root);
}

#ifdef __linux__
// Counts this process's open fds whose target path is a deleted file under
// `dir` — direct evidence of unlink-survival via held descriptors.
static size_t countDeletedFdsUnder(const std::string& dir) {
	size_t count = 0;
	for (auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
		std::error_code ec;
		auto target = std::filesystem::read_symlink(entry.path(), ec);
		if (ec) continue;
		const std::string t = target.string();
		if (t.rfind(dir, 0) == 0 && t.find("(deleted)") != std::string::npos) {
			count++;
		}
	}
	return count;
}
#endif

// Blob files that arrive via TryCatchUpWithPrimary (not at open): are they
// eagerly opened at version install too?
TEST(SecondarySpike, CatchUpInstalledBlobSurvivesPrimaryGC) {
	auto root = spikeRoot();
	std::filesystem::remove_all(root);
	std::filesystem::create_directories(root);
	std::string primaryPath = (root / "primary").string();
	std::string secondaryPath = (root / "secondary").string();

	rocksdb::Options options = blobOptions();
	std::unique_ptr<rocksdb::DB> primary;
	rocksdb::Status s = rocksdb::DB::Open(options, primaryPath, &primary);
	ASSERT_TRUE(s.ok()) << s.ToString();
	ASSERT_TRUE(primary->Put({}, "seed", "seed").ok());
	ASSERT_TRUE(primary->Flush({}).ok());

	rocksdb::Options secondaryOptions = blobOptions();
	secondaryOptions.create_if_missing = false;
	secondaryOptions.max_open_files = -1;
	std::unique_ptr<rocksdb::DB> secondary;
	s = rocksdb::DB::OpenAsSecondary(secondaryOptions, primaryPath, secondaryPath, &secondary);
	ASSERT_TRUE(s.ok()) << s.ToString();

	// New blob file lands AFTER the secondary opened; the secondary learns of
	// it only through catch-up.
	std::string large(16 * 1024, 'c');
	ASSERT_TRUE(primary->Put({}, "coldblob", large).ok());
	ASSERT_TRUE(primary->Flush({}).ok());
	s = secondary->TryCatchUpWithPrimary();
	ASSERT_TRUE(s.ok()) << s.ToString();
	// Still no read of "coldblob".

	std::string replacement(16 * 1024, 'd');
	ASSERT_TRUE(primary->Put({}, "coldblob", replacement).ok());
	ASSERT_TRUE(primary->Flush({}).ok());
	rocksdb::CompactRangeOptions compactOptions;
	compactOptions.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForceOptimized;
	ASSERT_TRUE(primary->CompactRange(compactOptions, nullptr, nullptr).ok());

#ifdef __linux__
	// Direct evidence of the mechanism: the secondary holds fds on files the
	// primary already deleted (SSTs, the WAL, and — decisive for BlobDB — the
	// GC'd blob file).
	EXPECT_GT(countDeletedFdsUnder(primaryPath), 0u);
#endif

	std::string value;
	s = secondary->Get({}, "coldblob", &value);
	EXPECT_TRUE(s.ok() && value == large) << s.ToString();

	ASSERT_TRUE(secondary->TryCatchUpWithPrimary().ok());
	s = secondary->Get({}, "coldblob", &value);
	EXPECT_TRUE(s.ok() && value == replacement) << s.ToString();

	secondary.reset();
	primary.reset();
	std::filesystem::remove_all(root);
}

// The sharper hazard probe: SST table readers are opened eagerly at version
// install when max_open_files = -1 (held fds survive the primary unlinking the
// file on POSIX), but blob files may be opened lazily on first read. A blob
// value the secondary has NEVER read, whose blob file the primary's GC then
// rewrites and deletes, is the worst case: the un-caught-up secondary's version
// still references the deleted blob file with no fd held.
TEST(SecondarySpike, NeverReadBlobSurvivesPrimaryGC) {
	auto root = spikeRoot();
	std::filesystem::remove_all(root);
	std::filesystem::create_directories(root);
	std::string primaryPath = (root / "primary").string();
	std::string secondaryPath = (root / "secondary").string();

	rocksdb::Options options = blobOptions();
	std::unique_ptr<rocksdb::DB> primary;
	rocksdb::Status s = rocksdb::DB::Open(options, primaryPath, &primary);
	ASSERT_TRUE(s.ok()) << s.ToString();

	std::string large(16 * 1024, 'a');
	ASSERT_TRUE(primary->Put({}, "coldblob", large).ok());
	ASSERT_TRUE(primary->Flush({}).ok());

	rocksdb::Options secondaryOptions = blobOptions();
	secondaryOptions.create_if_missing = false;
	secondaryOptions.max_open_files = -1;
	std::unique_ptr<rocksdb::DB> secondary;
	s = rocksdb::DB::OpenAsSecondary(secondaryOptions, primaryPath, secondaryPath, &secondary);
	ASSERT_TRUE(s.ok()) << s.ToString();
	// Deliberately do NOT read "coldblob" yet: no blob-file fd may be held.

	// Overwrite the value and force blob GC so the original blob file is
	// rewritten and deleted by the primary.
	std::string replacement(16 * 1024, 'b');
	ASSERT_TRUE(primary->Put({}, "coldblob", replacement).ok());
	ASSERT_TRUE(primary->Flush({}).ok());
	rocksdb::CompactRangeOptions compactOptions;
	compactOptions.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForceOptimized;
	ASSERT_TRUE(primary->CompactRange(compactOptions, nullptr, nullptr).ok());

	// First-ever read of the old blob on the un-caught-up secondary.
	std::string value;
	s = secondary->Get({}, "coldblob", &value);
	EXPECT_TRUE(s.ok() && value == large) << s.ToString();

	ASSERT_TRUE(secondary->TryCatchUpWithPrimary().ok());
	s = secondary->Get({}, "coldblob", &value);
	EXPECT_TRUE(s.ok() && value == replacement) << s.ToString();

	secondary.reset();
	primary.reset();
	std::filesystem::remove_all(root);
}

// Documents what RocksDB itself does when two secondary instances share one
// workspace: it does NOT reject the second (no LOCK discipline in the
// secondary path), even though the instances then overwrite each other's
// workspace state. This is exactly why the binding takes its own kernel
// advisory lock on `<secondaryPath>/.secondary.lock` (DBDescriptor::open). If
// a RocksDB upgrade starts rejecting this, the lock is redundant but harmless
// — update this expectation and reconsider it then.
TEST(SecondarySpike, SameWorkspaceSecondPairInstance) {
	auto root = spikeRoot();
	std::filesystem::remove_all(root);
	std::filesystem::create_directories(root);
	std::string primaryPath = (root / "primary").string();
	std::string secondaryPath = (root / "secondary").string();

	rocksdb::Options options = blobOptions();
	std::unique_ptr<rocksdb::DB> primary;
	ASSERT_TRUE(rocksdb::DB::Open(options, primaryPath, &primary).ok());
	ASSERT_TRUE(primary->Put({}, "k", "v").ok());
	ASSERT_TRUE(primary->Flush({}).ok());

	rocksdb::Options secondaryOptions = blobOptions();
	secondaryOptions.create_if_missing = false;
	secondaryOptions.max_open_files = -1;
	std::unique_ptr<rocksdb::DB> first;
	ASSERT_TRUE(rocksdb::DB::OpenAsSecondary(secondaryOptions, primaryPath, secondaryPath, &first).ok());

	std::unique_ptr<rocksdb::DB> second;
	rocksdb::Status s = rocksdb::DB::OpenAsSecondary(secondaryOptions, primaryPath, secondaryPath, &second);
	EXPECT_TRUE(s.ok()) << s.ToString();
	second.reset();

	first.reset();
	primary.reset();
	std::filesystem::remove_all(root);
}
