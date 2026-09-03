#include <gtest/gtest.h>
#include <chrono>
#include <string>
#include "core/wbm_stall_watchdog.h"

using rocksdb_js::formatWriteBufferManagerStallReport;
using rocksdb_js::resolveWbmStallWarnMs;
using rocksdb_js::WbmStallWatchdogState;
using rocksdb_js::WriteBufferManagerStallReport;

namespace {

using Clock = WbmStallWatchdogState::Clock;

// A fixed base so tests can express times as millisecond offsets.
Clock::time_point at(long long ms) {
	return Clock::time_point{} + std::chrono::milliseconds(ms);
}

constexpr uint64_t kThreshold = 5000;

} // namespace

TEST(WbmStallWatchdogState, IdleReportsNothing) {
	WbmStallWatchdogState state;
	for (long long ms = 0; ms <= 60000; ms += 1000) {
		auto sample = state.onSample(false, at(ms), kThreshold);
		EXPECT_FALSE(sample.reportNow);
		EXPECT_EQ(sample.stallActiveMs, 0u);
	}
}

TEST(WbmStallWatchdogState, FirstSampleOfAStallIsTheEdgeNotADuration) {
	WbmStallWatchdogState state;
	auto sample = state.onSample(true, at(0), kThreshold);
	EXPECT_FALSE(sample.reportNow);
	// The rising edge is when the stall was first *seen*; nothing is known about
	// how long it was already running.
	EXPECT_EQ(sample.stallActiveMs, 0u);
}

TEST(WbmStallWatchdogState, ReportsOnceWhenTheThresholdIsCrossed) {
	WbmStallWatchdogState state;
	state.onSample(true, at(0), kThreshold);
	EXPECT_FALSE(state.onSample(true, at(4000), kThreshold).reportNow);

	auto crossed = state.onSample(true, at(5000), kThreshold);
	EXPECT_TRUE(crossed.reportNow);
	EXPECT_EQ(crossed.stallActiveMs, 5000u);
	state.markReported();

	// An eight-hour wedge is one line, not one per sample.
	for (long long ms = 6000; ms <= 3600000; ms += 1000) {
		EXPECT_FALSE(state.onSample(true, at(ms), kThreshold).reportNow);
	}
}

TEST(WbmStallWatchdogState, RetriesUntilTheReportIsAcknowledged) {
	WbmStallWatchdogState state;
	state.onSample(true, at(0), kThreshold);
	EXPECT_TRUE(state.onSample(true, at(5000), kThreshold).reportNow);
	// markReported() not called: a report that could not be written is retried.
	EXPECT_TRUE(state.onSample(true, at(6000), kThreshold).reportNow);
	state.markReported();
	EXPECT_FALSE(state.onSample(true, at(7000), kThreshold).reportNow);
}

TEST(WbmStallWatchdogState, KeepsReportingDurationAfterTheReport) {
	WbmStallWatchdogState state;
	state.onSample(true, at(0), kThreshold);
	state.onSample(true, at(5000), kThreshold);
	state.markReported();
	// stallActiveMs is the live gauge behind writeBufferManager.stallActiveMs, so
	// it must keep climbing after the one-shot line has been emitted.
	EXPECT_EQ(state.onSample(true, at(42000), kThreshold).stallActiveMs, 42000u);
}

TEST(WbmStallWatchdogState, RecoveryRearmsForTheNextEpisode) {
	WbmStallWatchdogState state;
	state.onSample(true, at(0), kThreshold);
	EXPECT_TRUE(state.onSample(true, at(5000), kThreshold).reportNow);
	state.markReported();

	auto recovered = state.onSample(false, at(6000), kThreshold);
	EXPECT_FALSE(recovered.reportNow);
	EXPECT_EQ(recovered.stallActiveMs, 0u);

	// A second genuine episode reports again. There is deliberately no quiet
	// window on top of the threshold: it would suppress exactly this line.
	state.onSample(true, at(7000), kThreshold);
	EXPECT_TRUE(state.onSample(true, at(12000), kThreshold).reportNow);
}

TEST(WbmStallWatchdogState, FlappingNeverAccumulatesToTheThreshold) {
	WbmStallWatchdogState state;
	for (long long ms = 0; ms < 60000; ms += 2000) {
		EXPECT_FALSE(state.onSample(true, at(ms), kThreshold).reportNow);
		EXPECT_FALSE(state.onSample(false, at(ms + 1000), kThreshold).reportNow);
	}
}

TEST(WbmStallWatchdogState, ZeroThresholdNeverReports) {
	WbmStallWatchdogState state;
	state.onSample(true, at(0), 0);
	auto sample = state.onSample(true, at(3600000), 0);
	EXPECT_FALSE(sample.reportNow);
	EXPECT_EQ(sample.stallActiveMs, 3600000u);
}

TEST(ResolveWbmStallWarnMs, DefaultsWhenUnsetOrMalformed) {
	EXPECT_EQ(resolveWbmStallWarnMs(nullptr), 5000u);
	EXPECT_EQ(resolveWbmStallWarnMs(""), 5000u);
	EXPECT_EQ(resolveWbmStallWarnMs("abc"), 5000u);
	EXPECT_EQ(resolveWbmStallWarnMs("5000ms"), 5000u);
	EXPECT_EQ(resolveWbmStallWarnMs("-1"), 5000u);
	// Out of range rather than clamped: a threshold past a day is never intended,
	// and silently accepting one disables the alarm.
	EXPECT_EQ(resolveWbmStallWarnMs("99999999999999999999"), 5000u);
	EXPECT_EQ(resolveWbmStallWarnMs("86400001"), 5000u);
}

TEST(ResolveWbmStallWarnMs, ZeroDisablesAndSubSampleClampsUp) {
	EXPECT_EQ(resolveWbmStallWarnMs("0"), 0u);
	EXPECT_EQ(resolveWbmStallWarnMs("1"), 1000u);
	EXPECT_EQ(resolveWbmStallWarnMs("999"), 1000u);
	EXPECT_EQ(resolveWbmStallWarnMs("1000"), 1000u);
	EXPECT_EQ(resolveWbmStallWarnMs("2500"), 2500u);
	EXPECT_EQ(resolveWbmStallWarnMs("86400000"), 86400000u);
}

TEST(FormatWriteBufferManagerStallReport, CarriesEveryDiagnosticValue) {
	WriteBufferManagerStallReport report;
	report.stallActiveMs = 5000;
	report.bufferSize = 693337292; // 661.2 MiB
	report.memoryUsage = 694386688;
	report.mutableMemoryUsage = 13002342;
	report.allowStall = true;
	report.costToCache = true;
	report.columnFamilies = 28;
	report.maxWriteBufferSizeToMaintain[268435456] = 28;

	std::string line = formatWriteBufferManagerStallReport(report);
	EXPECT_NE(line.find("WriteBufferManager write stall active for 5.0s"), std::string::npos);
	EXPECT_NE(line.find("budget=661.2MB"), std::string::npos);
	EXPECT_NE(line.find("usage=662.2MB (100.2%)"), std::string::npos);
	EXPECT_NE(line.find("mutable=12.4MB (1.9%)"), std::string::npos);
	EXPECT_NE(line.find("allowStall=true"), std::string::npos);
	EXPECT_NE(line.find("costToCache=true"), std::string::npos);
	EXPECT_NE(line.find("columnFamilies=28"), std::string::npos);
	EXPECT_NE(line.find("maxWriteBufferSizeToMaintain={268435456:28}"), std::string::npos);
	EXPECT_EQ(line.find('\n'), std::string::npos);
}

TEST(FormatWriteBufferManagerStallReport, GroupsAMixedRetentionInventory) {
	WriteBufferManagerStallReport report;
	report.columnFamilies = 6;
	report.maxWriteBufferSizeToMaintain[0] = 5;
	report.maxWriteBufferSizeToMaintain[268435456] = 1;
	EXPECT_NE(
		formatWriteBufferManagerStallReport(report).find(
			"maxWriteBufferSizeToMaintain={0:5,268435456:1}"
		),
		std::string::npos
	);
}

TEST(FormatWriteBufferManagerStallReport, OmitsPercentagesWhenThereIsNoBudget) {
	WriteBufferManagerStallReport report;
	report.memoryUsage = 1024;
	std::string line = formatWriteBufferManagerStallReport(report);
	EXPECT_NE(line.find("budget=0B"), std::string::npos);
	EXPECT_NE(line.find("usage=1.0KB"), std::string::npos);
	EXPECT_EQ(line.find('%'), std::string::npos);
}
