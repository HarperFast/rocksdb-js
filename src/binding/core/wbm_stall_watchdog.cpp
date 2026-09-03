#include "core/wbm_stall_watchdog.h"
#include <cerrno>
#include <cstdlib>
#include <sstream>

namespace rocksdb_js {

namespace {

void appendBytes(std::ostringstream& out, uint64_t bytes) {
	static constexpr const char* units[] = { "B", "KB", "MB", "GB", "TB" };
	double value = static_cast<double>(bytes);
	size_t unit = 0;
	while (value >= 1024.0 && unit + 1 < sizeof(units) / sizeof(units[0])) {
		value /= 1024.0;
		unit++;
	}
	std::ostringstream formatted;
	formatted.setf(std::ios::fixed);
	formatted.precision(unit == 0 ? 0 : 1);
	formatted << value;
	out << formatted.str() << units[unit];
}

void appendPercentOfBudget(std::ostringstream& out, uint64_t part, uint64_t budget) {
	if (budget == 0) {
		return;
	}
	std::ostringstream formatted;
	formatted.setf(std::ios::fixed);
	formatted.precision(1);
	formatted << (static_cast<double>(part) * 100.0 / static_cast<double>(budget));
	out << " (" << formatted.str() << "%)";
}

} // namespace

uint64_t resolveWbmStallWarnMs(const char* raw) {
	if (raw == nullptr || *raw == '\0') {
		return WBM_STALL_WARN_MS_DEFAULT;
	}
	errno = 0;
	char* end = nullptr;
	long long parsed = ::strtoll(raw, &end, 10);
	if (errno != 0 || end == raw || *end != '\0' || parsed < 0 ||
		static_cast<unsigned long long>(parsed) > WBM_STALL_WARN_MS_MAX) {
		return WBM_STALL_WARN_MS_DEFAULT;
	}
	uint64_t value = static_cast<uint64_t>(parsed);
	if (value == 0) {
		return 0;
	}
	return value < WBM_STALL_SAMPLE_INTERVAL_MS ? WBM_STALL_SAMPLE_INTERVAL_MS : value;
}

std::string formatWriteBufferManagerStallReport(const WriteBufferManagerStallReport& report) {
	std::ostringstream out;
	std::ostringstream seconds;
	seconds.setf(std::ios::fixed);
	seconds.precision(1);
	seconds << (static_cast<double>(report.stallActiveMs) / 1000.0);

	out << "[rocksdb-js] WriteBufferManager write stall active for " << seconds.str()
		<< "s - no write can complete until memtable memory drops below the budget. budget=";
	appendBytes(out, report.bufferSize);
	out << " usage=";
	appendBytes(out, report.memoryUsage);
	appendPercentOfBudget(out, report.memoryUsage, report.bufferSize);
	out << " mutable=";
	appendBytes(out, report.mutableMemoryUsage);
	appendPercentOfBudget(out, report.mutableMemoryUsage, report.bufferSize);
	out << " allowStall=" << (report.allowStall ? "true" : "false")
		<< " costToCache=" << (report.costToCache ? "true" : "false")
		<< " columnFamilies=" << report.columnFamilies << " maxWriteBufferSizeToMaintain={";
	bool first = true;
	for (const auto& [target, count] : report.maxWriteBufferSizeToMaintain) {
		if (!first) {
			out << ',';
		}
		first = false;
		out << target << ':' << count;
	}
	out << '}';
	return out.str();
}

} // namespace rocksdb_js
