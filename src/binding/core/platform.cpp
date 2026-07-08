#include <cmath>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <limits>
#include <string>
#include <thread>
#include "core/debug.h"
#include "core/exception.h"
#include "core/platform.h"
#ifdef _WIN32
#include <windows.h>
#elif defined(__linux__)
#include <sys/syscall.h>
#include <sys/resource.h>
#include <pthread.h>
#elif defined(__APPLE__)
#include <pthread.h>
#include <sys/resource.h>
#include <sys/sysctl.h>
#endif

namespace rocksdb_js {

size_t getThreadId() {
#ifdef _WIN32
	return static_cast<size_t>(GetCurrentThreadId());
#elif defined(__linux__)
	return static_cast<size_t>(gettid());
#elif defined(__APPLE__)
	uint64_t tid;
	pthread_threadid_np(nullptr, &tid);
	return static_cast<size_t>(tid);
#else
	return std::hash<std::thread::id>{}(std::this_thread::get_id());
#endif
}

uint64_t getEffectiveOpenFileLimit() {
#ifdef _WIN32
	return 0;
#else
	struct rlimit limit;
	if (::getrlimit(RLIMIT_NOFILE, &limit) != 0) {
		return 0;
	}
	// An unlimited soft rlimit is "no rlimit constraint", not "unknown": it
	// still flows through the macOS kernel cap below and the derivation
	// ceiling in deriveMaxOpenFiles.
	uint64_t effectiveLimit = limit.rlim_cur == RLIM_INFINITY
		? std::numeric_limits<uint64_t>::max()
		: static_cast<uint64_t>(limit.rlim_cur);
	#ifdef __APPLE__
	// macOS enforces kern.maxfilesperproc even when the rlimit is higher
	int32_t maxFilesPerProc = 0;
	size_t size = sizeof(maxFilesPerProc);
	if (::sysctlbyname("kern.maxfilesperproc", &maxFilesPerProc, &size, nullptr, 0) == 0 &&
		maxFilesPerProc > 0 &&
		static_cast<uint64_t>(maxFilesPerProc) < effectiveLimit
	) {
		effectiveLimit = static_cast<uint64_t>(maxFilesPerProc);
	}
	#endif
	return effectiveLimit;
#endif
}

int32_t deriveMaxOpenFiles(uint64_t effectiveOpenFileLimit) {
	if (effectiveOpenFileLimit == 0) {
		return -1;
	}
	constexpr uint64_t minBudget = 1024;
	constexpr uint64_t maxBudget = 262144;
	uint64_t budget = effectiveOpenFileLimit / 8;
	if (budget < minBudget) {
		budget = minBudget;
	} else if (budget > maxBudget) {
		budget = maxBudget;
	}
	return static_cast<int32_t>(budget);
}

void setThreadName(const char* name) {
#if defined(__linux__)
	// Linux caps thread names at 16 bytes including the null terminator.
	::pthread_setname_np(::pthread_self(), name);
#elif defined(__APPLE__)
	::pthread_setname_np(name);
#elif defined(_WIN32)
	int len = ::MultiByteToWideChar(CP_UTF8, 0, name, -1, nullptr, 0);
	if (len > 0) {
		std::wstring wide(static_cast<size_t>(len), L'\0');
		::MultiByteToWideChar(CP_UTF8, 0, name, -1, wide.data(), len);
		::SetThreadDescription(::GetCurrentThread(), wide.c_str());
	}
#else
	(void)name;
#endif
}

std::chrono::system_clock::time_point convertFileTimeToSystemTime(
	const std::filesystem::file_time_type& fileTime
) {
#ifdef _WIN32
	constexpr auto epoch_diff = std::chrono::seconds(11644473600);
	return std::chrono::system_clock::time_point(
		std::chrono::duration_cast<std::chrono::system_clock::duration>(
			fileTime.time_since_epoch() - epoch_diff));
#else
	#if defined(__cpp_lib_chrono) && __cpp_lib_chrono >= 201907L
		return std::chrono::clock_cast<std::chrono::system_clock>(fileTime);
	#else
		using file_clock = std::filesystem::file_time_type::clock;
		static const auto offset = []() -> std::chrono::nanoseconds {
			auto sys_now = std::chrono::system_clock::now();
			auto file_now = file_clock::now();
			auto sys_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(sys_now.time_since_epoch());
			auto file_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(file_now.time_since_epoch());
			return sys_ns - file_ns;
		}();
		auto file_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(fileTime.time_since_epoch());
		auto sys_ns = file_ns + offset;
		return std::chrono::system_clock::time_point(
			std::chrono::duration_cast<std::chrono::system_clock::duration>(sys_ns));
	#endif
#endif
}

static std::atomic<double> lastTimestamp{0.0};

double getMonotonicTimestamp() {
	int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
		std::chrono::system_clock::now().time_since_epoch()
	).count();

	double result = static_cast<double>(now) / 1000000.0;

	double last = lastTimestamp.load(std::memory_order_acquire);
	if (result <= last) {
		result = std::nextafter(last, std::numeric_limits<double>::infinity());
	}

	while (!lastTimestamp.compare_exchange_strong(last, result, std::memory_order_acq_rel)) {
		if (result <= last) {
			result = std::nextafter(last, std::numeric_limits<double>::infinity());
		}
	}

	return result;
}

void tryCreateDirectory(const std::filesystem::path& path, std::filesystem::perms permissions, uint8_t retries) {
	if (std::filesystem::exists(path)) {
		return;
	}

	for (uint8_t i = 0; i < retries; i++) {
		try {
			std::filesystem::create_directories(path);
			std::filesystem::permissions(path, permissions);
			return;
		} catch (const std::filesystem::filesystem_error& e) {
			DEBUG_LOG("ERROR: Attempt %u to create directory failed: %s (error=%s)", i, path.string().c_str(), e.what());
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		} catch (const std::exception& e) {
			DEBUG_LOG("ERROR: Attempt %u to create directory failed: %s (error=%s)", i, path.string().c_str(), e.what());
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		} catch (...) {
			DEBUG_LOG("ERROR: Attempt %u to create directory failed: %s (unknown error)", i, path.string().c_str());
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}

	throw rocksdb_js::DBException("Failed to create directory: " + path.string());
}

} // namespace rocksdb_js
