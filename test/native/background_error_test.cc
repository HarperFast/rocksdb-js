#include <gtest/gtest.h>
#include <string>
#include "core/background_error.h"
#include "rocksdb/listener.h"
#include "rocksdb/status.h"

using rocksdb_js::backgroundErrorIsReadOnly;
using rocksdb_js::backgroundErrorReasonName;
using rocksdb_js::backgroundErrorSeverityName;
using rocksdb_js::BackgroundErrorInfo;
using rocksdb_js::BackgroundErrorMirror;

TEST(BackgroundError, SeverityNames) {
	EXPECT_STREQ(backgroundErrorSeverityName(rocksdb::Status::Severity::kNoError), "none");
	EXPECT_STREQ(backgroundErrorSeverityName(rocksdb::Status::Severity::kSoftError), "soft");
	EXPECT_STREQ(backgroundErrorSeverityName(rocksdb::Status::Severity::kHardError), "hard");
	EXPECT_STREQ(backgroundErrorSeverityName(rocksdb::Status::Severity::kFatalError), "fatal");
	EXPECT_STREQ(backgroundErrorSeverityName(rocksdb::Status::Severity::kUnrecoverableError), "unrecoverable");
}

TEST(BackgroundError, SeverityUnknownFallsBack) {
	EXPECT_STREQ(backgroundErrorSeverityName(42), "unknown");
	EXPECT_STREQ(backgroundErrorSeverityName(-1), "unknown");
}

TEST(BackgroundError, ReasonNames) {
	EXPECT_STREQ(backgroundErrorReasonName(static_cast<int>(rocksdb::BackgroundErrorReason::kFlush)), "flush");
	EXPECT_STREQ(backgroundErrorReasonName(static_cast<int>(rocksdb::BackgroundErrorReason::kCompaction)), "compaction");
	EXPECT_STREQ(backgroundErrorReasonName(static_cast<int>(rocksdb::BackgroundErrorReason::kWriteCallback)), "writeCallback");
	EXPECT_STREQ(backgroundErrorReasonName(static_cast<int>(rocksdb::BackgroundErrorReason::kMemTable)), "memtable");
	EXPECT_STREQ(backgroundErrorReasonName(static_cast<int>(rocksdb::BackgroundErrorReason::kManifestWrite)), "manifestWrite");
	EXPECT_STREQ(backgroundErrorReasonName(static_cast<int>(rocksdb::BackgroundErrorReason::kFlushNoWAL)), "flushNoWAL");
	EXPECT_STREQ(backgroundErrorReasonName(static_cast<int>(rocksdb::BackgroundErrorReason::kManifestWriteNoWAL)), "manifestWriteNoWAL");
	EXPECT_STREQ(backgroundErrorReasonName(static_cast<int>(rocksdb::BackgroundErrorReason::kAsyncFileOpen)), "asyncFileOpen");
}

TEST(BackgroundError, ReasonUnknownFallsBack) {
	EXPECT_STREQ(backgroundErrorReasonName(-1), "unknown");
	EXPECT_STREQ(backgroundErrorReasonName(999), "unknown");
}

// The info struct defaults to a healthy, cleared state.
TEST(BackgroundError, InfoDefaultsToHealthy) {
	BackgroundErrorInfo info;
	EXPECT_FALSE(info.latched);
	EXPECT_TRUE(info.message.empty());
	EXPECT_EQ(info.severity, 0);
	EXPECT_EQ(info.reason, -1);
}

// The #730 disk-quota case surfaces as a NoSpace IO error; its ToString() is
// what latchBackgroundError captures as the message. In production RocksDB's
// error handler classifies such an error >= hard, and backgroundErrorSeverityName
// maps that to an actionable "read-only" signal for a consumer.
TEST(BackgroundError, NoSpaceStatusLatchesWithMessage) {
	rocksdb::Status s = rocksdb::Status::NoSpace("quota exceeded");
	EXPECT_FALSE(s.ok());
	EXPECT_NE(s.ToString().find("quota exceeded"), std::string::npos);
	EXPECT_STREQ(backgroundErrorSeverityName(rocksdb::Status::Severity::kHardError), "hard");
}

// Only a hard-or-worse severity stops writes; a soft error auto-recovers.
TEST(BackgroundError, IsReadOnlyBySeverity) {
	EXPECT_FALSE(backgroundErrorIsReadOnly(rocksdb::Status::Severity::kNoError));   // 0
	EXPECT_FALSE(backgroundErrorIsReadOnly(rocksdb::Status::Severity::kSoftError)); // 1
	EXPECT_TRUE(backgroundErrorIsReadOnly(rocksdb::Status::Severity::kHardError));  // 2
	EXPECT_TRUE(backgroundErrorIsReadOnly(rocksdb::Status::Severity::kFatalError)); // 3
	EXPECT_TRUE(backgroundErrorIsReadOnly(rocksdb::Status::Severity::kUnrecoverableError)); // 4
}

// --- BackgroundErrorMirror: deterministic interleavings of the recovery
// reconciliation, which in production races across RocksDB background threads. ---

TEST(BackgroundErrorMirror, LatchAndGet) {
	BackgroundErrorMirror m;
	BackgroundErrorInfo info;
	EXPECT_FALSE(m.get(info));
	m.latch(0, rocksdb::Status::IOError("disk full"));
	ASSERT_TRUE(m.get(info));
	EXPECT_NE(info.message.find("disk full"), std::string::npos);
	EXPECT_EQ(info.reason, 0);
}

TEST(BackgroundErrorMirror, RecoverySuccessClearsTheRecoveredError) {
	BackgroundErrorMirror m;
	m.latch(0, rocksdb::Status::IOError("A"));
	BackgroundErrorInfo info;
	ASSERT_TRUE(m.get(info));
	m.onRecoveryBegin();
	m.reconcileRecoveryEnd(info.message, info.severity, /*recovered=*/true);
	EXPECT_FALSE(m.get(info));
}

// A newer error B latches between recovery A starting and A's callback; A's
// success must NOT clear B.
TEST(BackgroundErrorMirror, RecoverySuccessDoesNotClearANewerError) {
	BackgroundErrorMirror m;
	m.latch(0, rocksdb::Status::IOError("A"));
	BackgroundErrorInfo a;
	ASSERT_TRUE(m.get(a));
	m.onRecoveryBegin();
	m.latch(0, rocksdb::Status::IOError("B"));
	m.reconcileRecoveryEnd(a.message, a.severity, /*recovered=*/true);
	BackgroundErrorInfo info;
	ASSERT_TRUE(m.get(info));
	EXPECT_NE(info.message.find("B"), std::string::npos);
}

// The interleaving Codex flagged (PR #760): B carries the SAME message as the
// recovered error A. Message equality would clear B here; generation identity
// must keep it — RocksDB may still be write-stopped on B.
TEST(BackgroundErrorMirror, RecoverySuccessDoesNotClearANewerIdenticalError) {
	BackgroundErrorMirror m;
	m.latch(static_cast<int>(rocksdb::BackgroundErrorReason::kFlush), rocksdb::Status::NoSpace("No space left on device"));
	BackgroundErrorInfo a;
	uint64_t genA = 0;
	ASSERT_TRUE(m.get(a, &genA));
	m.onRecoveryBegin();
	// Identical-message error B latches during recovery A (e.g. two consecutive
	// disk-full flush failures against the same file).
	m.latch(static_cast<int>(rocksdb::BackgroundErrorReason::kFlush), rocksdb::Status::NoSpace("No space left on device"));
	uint64_t genB = 0;
	BackgroundErrorInfo b;
	ASSERT_TRUE(m.get(b, &genB));
	EXPECT_NE(genB, genA); // distinct errors → distinct generations
	m.reconcileRecoveryEnd(a.message, a.severity, /*recovered=*/true);
	BackgroundErrorInfo info;
	uint64_t genAfter = 0;
	ASSERT_TRUE(m.get(info, &genAfter)) << "identical-message B must survive A's recovery";
	EXPECT_EQ(genAfter, genB); // still B, untouched
}

// An OnErrorRecoveryEnd with no paired OnErrorRecoveryBegin (the manual-resume
// end-only path, which our JS resume() reconciles separately via clearIfUnchanged)
// must not clear on a stale recovery generation.
TEST(BackgroundErrorMirror, RecoverySuccessWithoutBeginDoesNotClear) {
	BackgroundErrorMirror m;
	m.latch(0, rocksdb::Status::IOError("A"));
	BackgroundErrorInfo info;
	ASSERT_TRUE(m.get(info));
	m.reconcileRecoveryEnd(info.message, info.severity, /*recovered=*/true);
	EXPECT_TRUE(m.get(info)) << "end without a paired begin must not clear the mirror";
}

// A recovery window is consumed by its end: a second end (or an end for a later,
// begin-less recovery) must not clear a freshly latched error on the old generation.
TEST(BackgroundErrorMirror, RecoveryWindowIsConsumedByEnd) {
	BackgroundErrorMirror m;
	m.latch(0, rocksdb::Status::IOError("A"));
	BackgroundErrorInfo a;
	ASSERT_TRUE(m.get(a));
	m.onRecoveryBegin();
	m.reconcileRecoveryEnd(a.message, a.severity, /*recovered=*/true); // clears A, consumes window
	ASSERT_FALSE(m.get(a));
	m.latch(0, rocksdb::Status::IOError("B"));
	m.reconcileRecoveryEnd(a.message, a.severity, /*recovered=*/true); // no begin → must not clear B
	BackgroundErrorInfo info;
	ASSERT_TRUE(m.get(info));
	EXPECT_NE(info.message.find("B"), std::string::npos);
}

// A failed recovery seeds old_bg_error only when nothing is latched.
TEST(BackgroundErrorMirror, FailedRecoverySeedsOnlyWhenEmpty) {
	BackgroundErrorMirror m;
	m.reconcileRecoveryEnd("stale IO error", 2, /*recovered=*/false);
	BackgroundErrorInfo info;
	ASSERT_TRUE(m.get(info));
	EXPECT_EQ(info.message, "stale IO error");
	EXPECT_EQ(info.severity, 2);
}

TEST(BackgroundErrorMirror, FailedRecoveryDoesNotClobberNewerError) {
	BackgroundErrorMirror m;
	m.latch(0, rocksdb::Status::IOError("B"));
	m.reconcileRecoveryEnd("old A", 2, /*recovered=*/false);
	BackgroundErrorInfo info;
	ASSERT_TRUE(m.get(info));
	EXPECT_NE(info.message.find("B"), std::string::npos);
}

// resume()'s generation-guarded clear: a stale clear (after a newer error
// latched) is a no-op; clearing at the observed generation succeeds.
TEST(BackgroundErrorMirror, ClearIfUnchangedRespectsGeneration) {
	BackgroundErrorMirror m;
	uint64_t gen = 0;
	m.latch(0, rocksdb::Status::IOError("A"));
	BackgroundErrorInfo info;
	ASSERT_TRUE(m.get(info, &gen));
	m.latch(0, rocksdb::Status::IOError("B")); // bumps generation
	EXPECT_FALSE(m.clearIfUnchanged(gen));
	ASSERT_TRUE(m.get(info, &gen));
	EXPECT_NE(info.message.find("B"), std::string::npos);
	EXPECT_TRUE(m.clearIfUnchanged(gen));
	EXPECT_FALSE(m.get(info));
}
