#include <gtest/gtest.h>
#include <string>
#include "core/background_error.h"
#include "rocksdb/listener.h"
#include "rocksdb/status.h"

using rocksdb_js::backgroundErrorReasonName;
using rocksdb_js::backgroundErrorSeverityName;
using rocksdb_js::BackgroundErrorInfo;

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
