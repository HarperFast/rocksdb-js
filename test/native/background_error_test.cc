#include <gtest/gtest.h>
#include <string>
#include "core/background_error.h"
#include "rocksdb/listener.h"
#include "rocksdb/status.h"

using rocksdb_js::backgroundErrorDisablesWrites;
using rocksdb_js::backgroundErrorReasonName;
using rocksdb_js::backgroundErrorSeverityName;
using rocksdb_js::backgroundErrorToJson;

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

// The #730 disk-quota case surfaces as a NoSpace IO error; its ToString() is
// what the `'error'` event carries as the message. In production RocksDB's
// error handler classifies such an error >= hard, and backgroundErrorSeverityName
// maps that to an actionable "read-only" signal for a consumer.
TEST(BackgroundError, NoSpaceStatusHasMessage) {
	rocksdb::Status s = rocksdb::Status::NoSpace("quota exceeded");
	EXPECT_FALSE(s.ok());
	EXPECT_NE(s.ToString().find("quota exceeded"), std::string::npos);
	EXPECT_STREQ(backgroundErrorSeverityName(rocksdb::Status::Severity::kHardError), "hard");
}

// backgroundErrorToJson is the serialized form stored on the descriptor and
// reconstructed into a BackgroundError on the JS thread.
TEST(BackgroundError, ToJsonIncludesAllFields) {
	std::string json = backgroundErrorToJson("disk full", 2, "hard", true, 0, "flush");
	EXPECT_NE(json.find("\"type\":\"background\""), std::string::npos);
	EXPECT_NE(json.find("\"message\":\"disk full\""), std::string::npos);
	EXPECT_NE(json.find("\"severity\":2"), std::string::npos);
	EXPECT_NE(json.find("\"severityName\":\"hard\""), std::string::npos);
	EXPECT_NE(json.find("\"writesDisabled\":true"), std::string::npos);
	EXPECT_NE(json.find("\"reason\":0"), std::string::npos);
	EXPECT_NE(json.find("\"reasonName\":\"flush\""), std::string::npos);
}

// A negative reason (no reason-bearing callback) omits reason/reasonName.
TEST(BackgroundError, ToJsonOmitsReasonWhenNegative) {
	std::string json = backgroundErrorToJson("soft hiccup", 1, "soft", false, -1, "unknown");
	EXPECT_EQ(json.find("\"reason\""), std::string::npos);
	EXPECT_EQ(json.find("\"reasonName\""), std::string::npos);
	EXPECT_NE(json.find("\"writesDisabled\":false"), std::string::npos);
}

// The message is JSON-escaped so quotes/control chars round-trip through JSON.parse.
TEST(BackgroundError, ToJsonEscapesMessage) {
	std::string json = backgroundErrorToJson("say \"hi\"\n", 2, "hard", true, 0, "flush");
	EXPECT_NE(json.find("\\\"hi\\\""), std::string::npos);
	EXPECT_NE(json.find("\\n"), std::string::npos);
}

// Only a hard-or-worse severity disables writes; a soft error auto-recovers.
TEST(BackgroundError, DisablesWritesBySeverity) {
	EXPECT_FALSE(backgroundErrorDisablesWrites(rocksdb::Status::Severity::kNoError));   // 0
	EXPECT_FALSE(backgroundErrorDisablesWrites(rocksdb::Status::Severity::kSoftError)); // 1
	EXPECT_TRUE(backgroundErrorDisablesWrites(rocksdb::Status::Severity::kHardError));  // 2
	EXPECT_TRUE(backgroundErrorDisablesWrites(rocksdb::Status::Severity::kFatalError)); // 3
	EXPECT_TRUE(backgroundErrorDisablesWrites(rocksdb::Status::Severity::kUnrecoverableError)); // 4
}
