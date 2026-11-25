// Secondary instances against a blob-enabled database. Upstream documents the
// combination as unsupported (facebook/rocksdb#13296), but this codebase
// enables blob files unconditionally (values >= 2KB) and depends on the pinned
// build's actual behavior: blob files are opened and fd-held at version
// install exactly like SSTs under max_open_files = -1, which is what makes the
// primary's blob-GC deletions safe. These tests must stay green on every
// RocksDB upgrade — a regression here breaks secondary mode for every real
// database.
#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>
#include <memory>
#include <set>
#include <string>
#include <vector>
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
	// Every compaction relocates every blob, so the tests' explicit compactions
	// rewrite blob files and delete the originals. Leave
	// blob_garbage_collection_force_threshold at RocksDB's default (1.0, off):
	// at 0.0 every version marks its oldest blob file for forced GC, the
	// primary recompacts its own output in an endless loop, and each catch-up
	// races that churn into the stall pinned by
	// DroppedNewerBlobFileStallsCatchUp (#1147).
	options.blob_garbage_collection_age_cutoff = 1.0;
	return options;
}

std::set<std::string> blobFilesIn(const std::string& dir) {
	std::set<std::string> names;
	for (const auto& entry : std::filesystem::directory_iterator(dir)) {
		if (entry.path().extension() == ".blob") {
			names.insert(entry.path().filename().string());
		}
	}
	return names;
}

// The hazard tests prove nothing unless the primary really deleted the blob
// files the secondary's version still references.
void expectBlobFilesDeleted(rocksdb::DB* primary, const std::string& dir, const std::set<std::string>& names) {
	ASSERT_FALSE(names.empty());
	rocksdb::WaitForCompactOptions waitOptions;
	waitOptions.wait_for_purge = true;
	waitOptions.timeout = std::chrono::seconds(30);
	rocksdb::Status s = primary->WaitForCompact(waitOptions);
	ASSERT_TRUE(s.ok()) << s.ToString();
	std::vector<rocksdb::LiveFileStorageInfo> live;
	ASSERT_TRUE(primary->GetLiveFilesStorageInfo({}, &live).ok());
	for (const auto& file : live) {
		EXPECT_EQ(names.count(file.relative_filename), 0u) << file.relative_filename << " is still live";
	}
#ifndef _WIN32
	// Windows can keep a deleted name visible while the secondary holds it open.
	for (const auto& name : names) {
		EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(dir) / name)) << name << " survived blob GC";
	}
#else
	(void)dir;
#endif
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
	const auto originalBlobs = blobFilesIn(primaryPath);
	std::string large3(16 * 1024, 'z');
	ASSERT_TRUE(primary->Put({}, "large", large3).ok());
	ASSERT_TRUE(primary->Flush({}).ok());
	rocksdb::CompactRangeOptions compactOptions;
	compactOptions.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForceOptimized;
	ASSERT_TRUE(primary->CompactRange(compactOptions, nullptr, nullptr).ok());
	ASSERT_NO_FATAL_FAILURE(expectBlobFilesDeleted(primary.get(), primaryPath, originalBlobs));

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

	const auto originalBlobs = blobFilesIn(primaryPath);
	std::string replacement(16 * 1024, 'd');
	ASSERT_TRUE(primary->Put({}, "coldblob", replacement).ok());
	ASSERT_TRUE(primary->Flush({}).ok());
	rocksdb::CompactRangeOptions compactOptions;
	compactOptions.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForceOptimized;
	ASSERT_TRUE(primary->CompactRange(compactOptions, nullptr, nullptr).ok());
	ASSERT_NO_FATAL_FAILURE(expectBlobFilesDeleted(primary.get(), primaryPath, originalBlobs));

#ifdef __linux__
	// Supporting evidence only: the count includes this process's own primary
	// handles, so the value assertions below are the actual proof.
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
	const auto originalBlobs = blobFilesIn(primaryPath);
	std::string replacement(16 * 1024, 'b');
	ASSERT_TRUE(primary->Put({}, "coldblob", replacement).ok());
	ASSERT_TRUE(primary->Flush({}).ok());
	rocksdb::CompactRangeOptions compactOptions;
	compactOptions.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForceOptimized;
	ASSERT_TRUE(primary->CompactRange(compactOptions, nullptr, nullptr).ok());
	ASSERT_NO_FATAL_FAILURE(expectBlobFilesDeleted(primary.get(), primaryPath, originalBlobs));

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

namespace {

// Drives a secondary into an upstream RocksDB stall found through #1147. The
// primary drops a blob file the secondary never saw, numbered above a blob file
// still in use. The point-in-time replay marks the dropped file missing, and
// VersionBuilder keeps only the highest missing blob number, which never goes
// back down. Every later Version then counts as incomplete while any live SST
// links an older blob file, so TryCatchUpWithPrimary() returns OK without
// installing anything. The options are the binding's blob defaults.
struct StalledSecondary {
	std::filesystem::path root;
	std::string primaryPath;
	rocksdb::Options options;
	rocksdb::Options secondaryOptions;
	std::unique_ptr<rocksdb::DB> primary;
	std::unique_ptr<rocksdb::DB> secondary;
	const std::string z2 = std::string(4096, '2');

	void setUp() {
		root = spikeRoot();
		std::filesystem::remove_all(root);
		std::filesystem::create_directories(root);
		primaryPath = (root / "primary").string();
		options.create_if_missing = true;
		options.enable_blob_files = true;
		options.min_blob_size = 2048;
		options.enable_blob_garbage_collection = true;
		ASSERT_TRUE(rocksdb::DB::Open(options, primaryPath, &primary).ok());

		ASSERT_TRUE(primary->Put({}, "a", std::string(4096, 'a')).ok());
		ASSERT_TRUE(primary->Flush({}).ok());
		ASSERT_TRUE(primary->CompactRange({}, nullptr, nullptr).ok());
		const auto olderBlobs = blobFilesIn(primaryPath);
		ASSERT_EQ(olderBlobs.size(), 1u);

		secondaryOptions = options;
		secondaryOptions.create_if_missing = false;
		secondaryOptions.max_open_files = -1;
		ASSERT_TRUE(rocksdb::DB::OpenAsSecondary(secondaryOptions, primaryPath, (root / "secondary").string(), &secondary).ok());

		ASSERT_TRUE(primary->Put({}, "z", std::string(4096, '1')).ok());
		ASSERT_TRUE(primary->Flush({}).ok());
		auto newerBlobs = blobFilesIn(primaryPath);
		for (const auto& name : olderBlobs) {
			newerBlobs.erase(name);
		}
		ASSERT_EQ(newerBlobs.size(), 1u);
		ASSERT_TRUE(primary->Put({}, "z", z2).ok());
		ASSERT_TRUE(primary->Flush({}).ok());
		rocksdb::CompactRangeOptions compactOptions;
		compactOptions.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;
		rocksdb::Slice begin("y");
		rocksdb::Slice end("zz");
		ASSERT_TRUE(primary->CompactRange(compactOptions, &begin, &end).ok());
		ASSERT_NO_FATAL_FAILURE(expectBlobFilesDeleted(primary.get(), primaryPath, newerBlobs));
		for (const auto& name : olderBlobs) {
			ASSERT_TRUE(std::filesystem::exists(std::filesystem::path(primaryPath) / name)) << name;
		}
	}

	~StalledSecondary() {
		secondary.reset();
		primary.reset();
		std::filesystem::remove_all(root);
	}
};

} // namespace

// Pins the stall's current symptoms, so they fail when a RocksDB upgrade fixes
// it: flip them then. The follower keeps tailing the WAL, so a newer unflushed
// write is visible while an older flushed one is missing, and reopening it
// discards what it tailed.
TEST(SecondarySpike, DroppedNewerBlobFileStallsCatchUp) {
	StalledSecondary st;
	ASSERT_NO_FATAL_FAILURE(st.setUp());
	std::string value;

	ASSERT_TRUE(st.primary->Put({}, "flushed", "v").ok());
	ASSERT_TRUE(st.primary->Flush({}).ok());
	ASSERT_TRUE(st.primary->Put({}, "tailed", "v").ok());
	ASSERT_TRUE(st.secondary->TryCatchUpWithPrimary().ok());
	EXPECT_TRUE(st.secondary->Get({}, "z", &value).IsNotFound()) << "fixed upstream? expect z == z2";
	EXPECT_TRUE(st.secondary->Get({}, "flushed", &value).IsNotFound()) << "fixed upstream? expect flushed == v";
	EXPECT_TRUE(st.secondary->Get({}, "tailed", &value).ok() && value == "v");

	ASSERT_TRUE(st.primary->Flush({}).ok());
	ASSERT_TRUE(st.secondary->TryCatchUpWithPrimary().ok());
	EXPECT_TRUE(st.secondary->Get({}, "tailed", &value).ok() && value == "v");
	std::unique_ptr<rocksdb::DB> fresh;
	ASSERT_TRUE(rocksdb::DB::OpenAsSecondary(st.secondaryOptions, st.primaryPath, (st.root / "fresh").string(), &fresh).ok());
	EXPECT_TRUE(fresh->Get({}, "z", &value).IsNotFound()) << "fixed upstream? expect z == z2";
	EXPECT_TRUE(fresh->Get({}, "tailed", &value).IsNotFound()) << "fixed upstream? expect tailed == v";
	fresh.reset();

	// A read-only open does not replay point in time.
	std::unique_ptr<rocksdb::DB> readOnly;
	ASSERT_TRUE(rocksdb::DB::OpenForReadOnly(st.options, st.primaryPath, &readOnly).ok());
	EXPECT_TRUE(readOnly->Get({}, "z", &value).ok() && value == st.z2);
	readOnly.reset();

	// The README workaround: the binding's bottommost compact() options.
	rocksdb::CompactRangeOptions relocateAll;
	relocateAll.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForceOptimized;
	relocateAll.blob_garbage_collection_policy = rocksdb::BlobGarbageCollectionPolicy::kForce;
	relocateAll.blob_garbage_collection_age_cutoff = 1.0;
	ASSERT_TRUE(st.primary->CompactRange(relocateAll, nullptr, nullptr).ok());
	ASSERT_TRUE(st.secondary->TryCatchUpWithPrimary().ok());
	EXPECT_TRUE(st.secondary->Get({}, "z", &value).ok() && value == st.z2);
	EXPECT_TRUE(st.secondary->Get({}, "flushed", &value).ok() && value == "v");
	EXPECT_TRUE(st.secondary->Get({}, "tailed", &value).ok() && value == "v");
}

// The other README workaround: a reopened primary writes a new MANIFEST that no
// longer records the dropped blob file, and the follower rebuilds its replay
// when it switches to it.
TEST(SecondarySpike, PrimaryReopenClearsStalledCatchUp) {
	StalledSecondary st;
	ASSERT_NO_FATAL_FAILURE(st.setUp());
	std::string value;
	ASSERT_TRUE(st.secondary->TryCatchUpWithPrimary().ok());
	ASSERT_TRUE(st.secondary->Get({}, "z", &value).IsNotFound());

	st.primary.reset();
	ASSERT_TRUE(rocksdb::DB::Open(st.options, st.primaryPath, &st.primary).ok());
	ASSERT_TRUE(st.secondary->TryCatchUpWithPrimary().ok());
	EXPECT_TRUE(st.secondary->Get({}, "z", &value).ok() && value == st.z2);
	std::unique_ptr<rocksdb::DB> fresh;
	ASSERT_TRUE(rocksdb::DB::OpenAsSecondary(st.secondaryOptions, st.primaryPath, (st.root / "fresh").string(), &fresh).ok());
	EXPECT_TRUE(fresh->Get({}, "z", &value).ok() && value == st.z2);
	fresh.reset();
}
