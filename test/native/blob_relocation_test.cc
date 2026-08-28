#include <gtest/gtest.h>
#include <filesystem>
#include <set>
#include <string>
#include <utility>
#include "core/blob_relocation.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

#ifndef _WIN32
#include <sys/stat.h>
#include <unistd.h>
#endif

using rocksdb_js::BlobDirScan;
using rocksdb_js::BlobDirScanState;
using rocksdb_js::BlobRelocationDecision;
using rocksdb_js::BlobRelocationInput;
using rocksdb_js::decideBlobRelocation;
using rocksdb_js::makeBlobDirScanner;

// A family opened against the wrong directory reads every value at or above
// `min_blob_size` as missing.

namespace {

constexpr const char* kDb = "/data/db";
constexpr const char* kOld = "/mnt/old";
constexpr const char* kNew = "/mnt/new";
constexpr const char* kTier0 = "/mnt/tier0";

/** A fake filesystem: which directories exist, and which hold `.blob` files. */
struct FakeDirs {
	std::set<std::string> exists;
	std::set<std::string> withBlobFiles;
	std::set<std::string> unknown;

	FakeDirs(
		std::set<std::string> existing = {},
		std::set<std::string> populated = {},
		std::set<std::string> unreadable = {}
	) :
		exists(std::move(existing)),
		withBlobFiles(std::move(populated)),
		unknown(std::move(unreadable)) {}

	std::function<BlobDirScan(const std::string&)> scanner() const {
		return [this](const std::string& dir) {
			if (unknown.count(dir) > 0) {
				return BlobDirScan{ BlobDirScanState::Unknown, "IO error: Permission denied" };
			}
			return BlobDirScan{
				withBlobFiles.count(dir) > 0
					? BlobDirScanState::HoldsBlobFiles
					: BlobDirScanState::Clear,
				{},
			};
		};
	}
	std::function<bool(const std::string&)> present() const {
		return [this](const std::string& dir) { return exists.count(dir) > 0; };
	}
};

/** Every directory in play exists and none of them holds a blob file. */
FakeDirs allMovedOut() {
	return FakeDirs{ { kDb, kOld, kNew }, {} };
}

BlobRelocationInput family(const std::string& cfName, bool isTarget) {
	BlobRelocationInput input;
	input.dbPath = kDb;
	input.cfName = cfName;
	input.isTarget = isTarget;
	return input;
}

BlobRelocationDecision decide(const BlobRelocationInput& input, const FakeDirs& dirs) {
	return decideBlobRelocation(input, dirs.scanner(), dirs.present());
}

class ScriptedBlobDirEnv : public rocksdb::EnvWrapper {
public:
	ScriptedBlobDirEnv(
		rocksdb::Status listStatus,
		rocksdb::Status metadataStatus,
		std::vector<std::string> children = {}
	) :
		rocksdb::EnvWrapper(rocksdb::Env::Default()),
		listStatus_(std::move(listStatus)),
		metadataStatus_(std::move(metadataStatus)),
		children_(std::move(children)) {}

	rocksdb::Status GetChildren(
		const std::string& /*dir*/,
		std::vector<std::string>* children
	) override {
		++listCalls;
		if (listStatus_.ok()) {
			*children = children_;
		}
		return listStatus_;
	}

	rocksdb::Status GetFileSize(const std::string& /*path*/, uint64_t* size) override {
		*size = 0;
		return metadataStatus_;
	}

	int listCalls = 0;

private:
	rocksdb::Status listStatus_;
	rocksdb::Status metadataStatus_;
	std::vector<std::string> children_;
};

}

TEST(BlobDirScanner, DistinguishesEmptyPopulatedAbsentAndUnknownDirectories) {
	ScriptedBlobDirEnv empty(rocksdb::Status::OK(), rocksdb::Status::OK());
	EXPECT_EQ(makeBlobDirScanner(&empty)(kOld).state, BlobDirScanState::Clear);

	ScriptedBlobDirEnv populated(
		rocksdb::Status::OK(),
		rocksdb::Status::OK(),
		{ "000001.blob", "CURRENT" }
	);
	EXPECT_EQ(
		makeBlobDirScanner(&populated)(kOld).state,
		BlobDirScanState::HoldsBlobFiles
	);

	ScriptedBlobDirEnv absent(
		rocksdb::Status::PathNotFound("listing"),
		rocksdb::Status::PathNotFound("metadata")
	);
	EXPECT_EQ(makeBlobDirScanner(&absent)(kOld).state, BlobDirScanState::Clear);

	ScriptedBlobDirEnv unreadable(
		rocksdb::Status::PathNotFound("listing"),
		rocksdb::Status::OK()
	);
	BlobDirScan unreadableScan = makeBlobDirScanner(&unreadable)(kOld);
	EXPECT_EQ(unreadableScan.state, BlobDirScanState::Unknown);
	EXPECT_NE(unreadableScan.detail.find("listing"), std::string::npos);

	ScriptedBlobDirEnv ioError(
		rocksdb::Status::IOError("listing"),
		rocksdb::Status::IOError("metadata")
	);
	BlobDirScan ioErrorScan = makeBlobDirScanner(&ioError)(kOld);
	EXPECT_EQ(ioErrorScan.state, BlobDirScanState::Unknown);
	EXPECT_NE(ioErrorScan.detail.find("metadata probe"), std::string::npos);
}

TEST(BlobDirScanner, MemoizesEachResolvedDirectory) {
	ScriptedBlobDirEnv env(rocksdb::Status::OK(), rocksdb::Status::OK());
	auto scanner = makeBlobDirScanner(&env);
	EXPECT_EQ(scanner(kOld).state, BlobDirScanState::Clear);
	EXPECT_EQ(scanner(kOld).state, BlobDirScanState::Clear);
	EXPECT_EQ(scanner(kNew).state, BlobDirScanState::Clear);
	EXPECT_EQ(env.listCalls, 2);
}

#ifndef _WIN32
TEST(BlobDirScanner, RealPosixUnreadableDirectoryIsUnknown) {
	if (geteuid() == 0) {
		GTEST_SKIP() << "root can enumerate a directory without read permission";
	}

	std::filesystem::path dir = std::filesystem::temp_directory_path() /
		("rocksdb-js-unreadable-blob-dir-" + std::to_string(getpid()));
	std::error_code error;
	std::filesystem::remove_all(dir, error);
	ASSERT_TRUE(std::filesystem::create_directory(dir));
	ASSERT_EQ(chmod(dir.c_str(), 0111), 0);

	BlobDirScan scan = makeBlobDirScanner(rocksdb::Env::Default())(dir.string());

	EXPECT_EQ(chmod(dir.c_str(), 0700), 0);
	std::filesystem::remove_all(dir, error);
	EXPECT_EQ(scan.state, BlobDirScanState::Unknown);
}
#endif

// A plain reopen asks for nothing, so nothing is decided against the family.
TEST(BlobRelocation, PlainReopenKeepsThePersistedDirectory) {
	BlobRelocationInput input = family("t1", true);
	input.persistedBlobDir = kOld;
	input.targetPersistedBlobDir = kOld;
	input.currentBlobDir = kOld;

	BlobRelocationDecision decision = decide(input, allMovedOut());
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, kOld);
}

// Changing `blobs.dir` without acknowledging the move does not relocate the
// files, it strands them.
TEST(BlobRelocation, TargetRefusesAnUnacknowledgedChange) {
	BlobRelocationInput input = family("t1", true);
	input.persistedBlobDir = kOld;
	input.targetPersistedBlobDir = kOld;
	input.requestedDir = kNew;
	input.currentBlobDir = kNew;

	BlobRelocationDecision decision = decide(input, allMovedOut());
	EXPECT_NE(decision.error.find("Cannot reopen"), std::string::npos);
	EXPECT_NE(decision.error.find(kOld), std::string::npos);
}

// The old directory still holding `.blob` files means the move did not happen.
TEST(BlobRelocation, AcknowledgementIsCheckedAgainstTheOldDirectory) {
	BlobRelocationInput input = family("t1", true);
	input.persistedBlobDir = kOld;
	input.targetPersistedBlobDir = kOld;
	input.requestedDir = kNew;
	input.allowDirChange = true;
	input.currentBlobDir = kNew;

	FakeDirs dirs{ { kDb, kOld, kNew }, { kOld } };
	BlobRelocationDecision decision = decide(input, dirs);
	EXPECT_NE(decision.error.find("still has blob files"), std::string::npos);
	EXPECT_NE(decision.error.find(kOld), std::string::npos);
}

TEST(BlobRelocation, UnknownSourceStateRefusesTheAcknowledgement) {
	BlobRelocationInput input = family("t1", true);
	input.persistedBlobDir = kOld;
	input.targetPersistedBlobDir = kOld;
	input.requestedDir = kNew;
	input.allowDirChange = true;
	input.currentBlobDir = kNew;

	FakeDirs dirs{ { kDb, kOld, kNew }, {}, { kOld } };
	BlobRelocationDecision decision = decide(input, dirs);
	EXPECT_NE(decision.error.find("Cannot inspect"), std::string::npos);
	EXPECT_NE(decision.error.find(kOld), std::string::npos);
	EXPECT_NE(decision.error.find("Permission denied"), std::string::npos);
	EXPECT_NE(decision.error.find("definitively absent"), std::string::npos);
}

TEST(BlobRelocation, AcknowledgedTargetMovesOnceTheFilesAreGone) {
	BlobRelocationInput input = family("t1", true);
	input.persistedBlobDir = kOld;
	input.targetPersistedBlobDir = kOld;
	input.requestedDir = kNew;
	input.allowDirChange = true;
	input.currentBlobDir = kNew;

	BlobRelocationDecision decision = decide(input, allMovedOut());
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, kNew);
}

// An empty persisted `blob_dir` means the database directory, not "no
// directory". Comparing the raw strings exempted every family that never had an
// external directory — i.e. the flat-to-external move, the only migration an
// untiered database can make.
TEST(BlobRelocation, FlatFamilyIsCheckedAgainstTheDatabaseDirectory) {
	BlobRelocationInput input = family("t1", true);
	input.persistedBlobDir = "";
	input.targetPersistedBlobDir = "";
	input.requestedDir = kNew;
	input.allowDirChange = true;
	input.currentBlobDir = kNew;

	FakeDirs dirs{ { kDb, kNew }, { kDb } };
	BlobRelocationDecision decision = decide(input, dirs);
	EXPECT_NE(decision.error.find("still has blob files"), std::string::npos);
	EXPECT_NE(decision.error.find(kDb), std::string::npos);
}

TEST(BlobRelocation, FlatFamilyMovesOutOnceTheDatabaseDirectoryIsClear) {
	BlobRelocationInput input = family("t1", true);
	input.persistedBlobDir = "";
	input.targetPersistedBlobDir = "";
	input.requestedDir = kNew;
	input.allowDirChange = true;
	input.currentBlobDir = kNew;

	BlobRelocationDecision decision = decide(input, allMovedOut());
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, kNew);
}

// A move is not per-family: the files sitting in one directory move together,
// so a family that shared the target's old directory is re-pointed with it.
TEST(BlobRelocation, SharingFamilyMovesWithTheTarget) {
	BlobRelocationInput input = family("t2", false);
	input.persistedBlobDir = kOld;
	input.targetPersistedBlobDir = kOld;
	input.requestedDir = kNew;
	input.allowDirChange = true;
	input.currentBlobDir = kOld;

	BlobRelocationDecision decision = decide(input, allMovedOut());
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, kNew);
}

// ...and one whose blobs were somewhere else keeps its own, because re-pointing
// it would strand files that never moved. The normal Harper layout is
// heterogeneous, so this is the common case rather than an edge.
TEST(BlobRelocation, UnrelatedFamilyKeepsItsOwnDirectory) {
	BlobRelocationInput input = family("t2", false);
	input.persistedBlobDir = "/mnt/elsewhere";
	input.targetPersistedBlobDir = kOld;
	input.requestedDir = kNew;
	input.allowDirChange = true;
	input.currentBlobDir = "/mnt/elsewhere";

	FakeDirs dirs{ { kDb, kOld, kNew, "/mnt/elsewhere" }, { "/mnt/elsewhere" } };
	BlobRelocationDecision decision = decide(input, dirs);
	// Not acknowledged for this family, so its still-populated directory is not
	// evidence of anything and must not refuse the open.
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, "/mnt/elsewhere");
}

// A flat family does not share an external target's directory, so it is left
// alone too — grouping is by the persisted directory, and "the database
// directory" is a directory.
TEST(BlobRelocation, FlatFamilyIsLeftAloneWhenAnExternalFamilyMoves) {
	BlobRelocationInput input = family("default", false);
	input.persistedBlobDir = "";
	input.targetPersistedBlobDir = kOld;
	input.requestedDir = kNew;
	input.allowDirChange = true;
	input.currentBlobDir = "";

	FakeDirs dirs{ { kDb, kOld, kNew }, { kDb } };
	BlobRelocationDecision decision = decide(input, dirs);
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, "");
}

// Omitting `dir` says the whole database was flattened into its own directory,
// which is what restoring a backup produces — so every family goes flat,
// including one this open does not name.
TEST(BlobRelocation, FlattenReachesEveryFamily) {
	BlobRelocationInput input = family("t2", false);
	input.persistedBlobDir = kOld;
	input.targetPersistedBlobDir = "";
	input.allowDirChange = true;
	input.currentBlobDir = kOld;

	BlobRelocationDecision decision = decide(input, allMovedOut());
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, "");
}

// Flatten is a claim about disk as well: a family whose recorded directory
// still holds blob files has not been flattened. Restoring a copy beside its
// source is exactly this shape.
TEST(BlobRelocation, FlattenIsCheckedAgainstEveryFamilysOldDirectory) {
	BlobRelocationInput input = family("t2", false);
	input.persistedBlobDir = kOld;
	input.targetPersistedBlobDir = "";
	input.allowDirChange = true;
	input.currentBlobDir = kOld;

	FakeDirs dirs{ { kDb, kOld }, { kOld } };
	BlobRelocationDecision decision = decide(input, dirs);
	EXPECT_NE(decision.error.find("still has blob files"), std::string::npos);
	EXPECT_NE(decision.error.find("restore where it is not reachable"), std::string::npos);
}

// The check is skipped when nothing is actually changing, so an
// `allowDirChange` left behind in a config file does not start refusing every
// open once the files are back where the family recorded them.
TEST(BlobRelocation, LeftoverAcknowledgementDoesNotRefuseAnUnchangedOpen) {
	BlobRelocationInput input = family("t1", true);
	input.persistedBlobDir = kOld;
	input.targetPersistedBlobDir = kOld;
	input.requestedDir = kOld;
	input.allowDirChange = true;
	input.currentBlobDir = kOld;

	FakeDirs dirs{ { kDb, kOld }, { kOld } };
	BlobRelocationDecision decision = decide(input, dirs);
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, kOld);
}

// Same, spelled the other way: a flat family reopened flat with the flag set.
TEST(BlobRelocation, LeftoverAcknowledgementDoesNotRefuseAFlatUnchangedOpen) {
	BlobRelocationInput input = family("t1", true);
	input.persistedBlobDir = "";
	input.targetPersistedBlobDir = "";
	input.allowDirChange = true;
	input.currentBlobDir = "";

	FakeDirs dirs{ { kDb }, { kDb } };
	BlobRelocationDecision decision = decide(input, dirs);
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, "");
}

// A directory that is gone is not a soft failure: every read of a large value
// fails and the first flush errors the whole database read-only, long after the
// open that could have named the cause. A restored database still pointing at a
// source volume that is not mounted here is this case.
TEST(BlobRelocation, MissingDirectoryRefusesTheOpen) {
	BlobRelocationInput input = family("t2", false);
	input.persistedBlobDir = "/mnt/unmounted";
	input.targetPersistedBlobDir = kOld;
	input.currentBlobDir = "/mnt/unmounted";

	FakeDirs dirs{ { kDb, kOld }, {} };
	BlobRelocationDecision decision = decide(input, dirs);
	EXPECT_NE(decision.error.find("which does not exist"), std::string::npos);
	EXPECT_NE(decision.error.find("/mnt/unmounted"), std::string::npos);
}

// The database directory is where a flat family's blobs live and it always
// exists by the time a family is opened, so a flat family never trips the
// existence check.
TEST(BlobRelocation, FlatFamilyNeverTripsTheExistenceCheck) {
	BlobRelocationInput input = family("default", false);
	input.persistedBlobDir = "";
	input.targetPersistedBlobDir = "";
	input.currentBlobDir = "";

	BlobRelocationDecision decision = decide(input, FakeDirs{ {}, {} });
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, "");
}

// A family being created by this open has nothing persisted, so none of the
// checks that compare against a recorded directory apply to it.
TEST(BlobRelocation, NewFamilyHasNothingToCompareAgainst) {
	BlobRelocationInput input = family("t3", true);
	input.targetPersistedBlobDir = std::nullopt;
	input.requestedDir = kNew;
	input.currentBlobDir = kNew;

	FakeDirs dirs{ { kDb, kNew }, { kDb } };
	BlobRelocationDecision decision = decide(input, dirs);
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, kNew);
}

// With the target not yet on disk, no other family can be said to have moved
// with it — so an untargeted family keeps what it recorded even under
// `allowDirChange` with a `dir`.
TEST(BlobRelocation, NoTargetOnDiskMeansNoFamilyMovesWithIt) {
	BlobRelocationInput input = family("t2", false);
	input.persistedBlobDir = kOld;
	input.targetPersistedBlobDir = std::nullopt;
	input.requestedDir = kNew;
	input.allowDirChange = true;
	input.currentBlobDir = kOld;

	BlobRelocationDecision decision = decide(input, allMovedOut());
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, kOld);
}

// Documented quirk rather than a rule worth having: a flat family reopened with
// `blobs.dir` spelled as the database directory is the same location under a
// different spelling, and it is refused with the recorded directory rendered as
// an empty string. The acknowledgement path resolves both sides; this one does
// not. Locked here so a change to it is a deliberate one.
TEST(BlobRelocation, FlatFamilyRespelledAsTheDatabaseDirectoryIsStillRefused) {
	BlobRelocationInput input = family("t1", true);
	input.persistedBlobDir = "";
	input.targetPersistedBlobDir = "";
	input.requestedDir = kDb;
	input.currentBlobDir = kDb;

	BlobRelocationDecision decision = decide(input, allMovedOut());
	EXPECT_NE(decision.error.find("Cannot reopen"), std::string::npos);
	EXPECT_NE(decision.error.find("its blob files were written to \"\""), std::string::npos);
}

// A tiered database's flat blob files are not in the database directory: with
// `paths` set, RocksDB derives them from `cf_paths.front()`, which falls back to
// `db_paths.front()` = `paths[0]`. Checking the database directory instead finds
// nothing, accepts an acknowledgement nobody honored, and opens a database whose
// large values are all unreadable — the exact failure the check exists to stop.
TEST(BlobRelocation, TieredFlatFamilyIsCheckedAgainstTheFirstStoragePath) {
	BlobRelocationInput input = family("t1", true);
	input.defaultBlobDir = kTier0;
	input.persistedBlobDir = "";
	input.targetPersistedBlobDir = "";
	input.requestedDir = kNew;
	input.allowDirChange = true;
	input.currentBlobDir = kNew;

	FakeDirs dirs{ { kDb, kTier0, kNew }, { kTier0 } };
	BlobRelocationDecision decision = decide(input, dirs);
	EXPECT_NE(decision.error.find("still has blob files"), std::string::npos);
	EXPECT_NE(decision.error.find(kTier0), std::string::npos);
}

TEST(BlobRelocation, TieredFlatFamilyMovesOutOnceTheStoragePathIsClear) {
	BlobRelocationInput input = family("t1", true);
	input.defaultBlobDir = kTier0;
	input.persistedBlobDir = "";
	input.targetPersistedBlobDir = "";
	input.requestedDir = kNew;
	input.allowDirChange = true;
	input.currentBlobDir = kNew;

	FakeDirs dirs{ { kDb, kTier0, kNew }, {} };
	BlobRelocationDecision decision = decide(input, dirs);
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, kNew);
}

// The database directory is not the tiered default, so blob files left there by
// something else must not be read as this family's.
TEST(BlobRelocation, TieredFlatFamilyIgnoresTheDatabaseDirectory) {
	BlobRelocationInput input = family("t1", true);
	input.defaultBlobDir = kTier0;
	input.persistedBlobDir = "";
	input.targetPersistedBlobDir = "";
	input.requestedDir = kNew;
	input.allowDirChange = true;
	input.currentBlobDir = kNew;

	FakeDirs dirs{ { kDb, kTier0, kNew }, { kDb } };
	BlobRelocationDecision decision = decide(input, dirs);
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, kNew);
}

// Flattening a tiered database sends its blob files back to `paths[0]`, so that
// is where the flatten claim is checked and where the family ends up pointed.
TEST(BlobRelocation, FlattenOnATieredDatabaseIsCheckedAgainstTheFirstStoragePath) {
	BlobRelocationInput input = family("t2", false);
	input.defaultBlobDir = kTier0;
	input.persistedBlobDir = kOld;
	input.targetPersistedBlobDir = kOld;
	input.requestedDir = "";
	input.allowDirChange = true;
	input.currentBlobDir = kOld;

	FakeDirs dirs{ { kDb, kTier0, kOld }, { kOld } };
	BlobRelocationDecision refused = decide(input, dirs);
	EXPECT_NE(refused.error.find("still has blob files"), std::string::npos);
	EXPECT_NE(refused.error.find(kOld), std::string::npos);

	FakeDirs moved{ { kDb, kTier0, kOld }, {} };
	BlobRelocationDecision decision = decide(input, moved);
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, "");
}

// An unset `defaultBlobDir` is an untiered database, where the two are the same
// directory — so the existing rules keep deciding against `dbPath`.
TEST(BlobRelocation, UnsetDefaultBlobDirStillMeansTheDatabaseDirectory) {
	BlobRelocationInput input = family("t1", true);
	input.persistedBlobDir = "";
	input.targetPersistedBlobDir = "";
	input.requestedDir = kNew;
	input.allowDirChange = true;
	input.currentBlobDir = kNew;

	FakeDirs dirs{ { kDb, kNew }, { kDb } };
	BlobRelocationDecision decision = decide(input, dirs);
	EXPECT_NE(decision.error.find("still has blob files"), std::string::npos);
	EXPECT_NE(decision.error.find(kDb), std::string::npos);
}

// `paths[0]` spelled out and an omitted `dir` are the same directory on a tiered
// database, so an acknowledgement across them is not a move and must not be
// checked as one — the files are exactly where both spellings point.
TEST(BlobRelocation, TieredFamilyRespelledAsTheFirstStoragePathIsNotAMove) {
	BlobRelocationInput input = family("t1", true);
	input.defaultBlobDir = kTier0;
	input.persistedBlobDir = kTier0;
	input.targetPersistedBlobDir = kTier0;
	input.requestedDir = "";
	input.allowDirChange = true;
	input.currentBlobDir = "";

	FakeDirs dirs{ { kDb, kTier0 }, { kTier0 } };
	BlobRelocationDecision decision = decide(input, dirs);
	EXPECT_EQ(decision.error, "");
	EXPECT_EQ(decision.blobDir, "");
}
