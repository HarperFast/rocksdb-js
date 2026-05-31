#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <functional>
#include <thread>
#include "core/debug.h"

namespace rocksdb_js {

void debugLog(const bool showTimestamp, const char* msg, ...) {
	auto now = std::chrono::system_clock::now();
	auto time_t = std::chrono::system_clock::to_time_t(now);
	auto ms_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(
		now.time_since_epoch());
	auto ms = ms_since_epoch.count() % 1000;

	if (showTimestamp) {
		std::tm tm_buf;
#ifdef _WIN32
		::localtime_s(&tm_buf, &time_t);
#else
		::localtime_r(&time_t, &tm_buf);
#endif
		::fprintf(stderr, "%02d:%02d:%02d.%03lld ",
			tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec,
			static_cast<long long>(ms));

		::fprintf(stderr, "[%04zu] ", std::hash<std::thread::id>{}(std::this_thread::get_id()) % 10000);
	}

	::va_list args;
	va_start(args, msg);
	::vfprintf(stderr, msg, args);
	va_end(args);
	::fflush(stderr);
}

} // namespace rocksdb_js
