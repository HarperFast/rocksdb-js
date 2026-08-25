#include <gtest/gtest.h>
#include <set>
#include <string>
#include "core/blob_relocation.h"

using rocksdb_js::BlobRelocationDecision;
using rocksdb_js::BlobRelocationInput;
using rocksdb_js::decideBlobRelocation;

// The call site compiles only into a build carrying the downstream `blob_dir`
// patch, and no prebuild carries it yet — so every integration test of these
// rules skips on every build that exists today and these cases are the only
// thing that executes them. What they guard is not a soft failure: a family
// opened against the wrong directory reads every value at or above
// `min_blob_size` as missing, and a family re-pointed at a directory it never
// wrote to strands the files that are still where it left them.

namespace {

constexpr const char* kDb = "/data/db";
constexpr const char* kOld = "/mnt/old";
constexpr const char* kNew = "/mnt/new";

/** A fake filesystem: which directories exist, and which hold `.blob` files. */
struct FakeDirs {
	std::set<std::string> exists;
	std::set<std::string> withBlobFiles;

	std::function<bool(const std::string&)> holds() const {
		return [this](const std::string& dir) { return withBlobFiles.count(dir) > 0; };
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
	return decideBlobRelocation(input, dirs.holds(), dirs.present());
}

}

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

// The acknowledgement is a claim about files on disk, so it is checked against
// them: the old directory still holding `.blob` files means the move did not
// happen. This is the vitest case `should refuse to relocate before the blob
// files have moved`, which skips on every build that exists.
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
// untiered database can make. This is the vitest case `should refuse to
// relocate a flat family before its blob files have moved`, which also skips
// everywhere.
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
