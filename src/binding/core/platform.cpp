#include <cmath>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <limits>
#include <thread>
#include "core/debug.h"
#include "core/exception.h"
#include "core/platform.h"
#ifdef _WIN32
#include <windows.h>
#include <io.h>
#else
#include <cerrno>
#include <cstring>
#include <sys/file.h>
#endif
#ifdef __linux__
#include <sys/syscall.h>
#elif defined(__APPLE__)
#include <pthread.h>
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

bool tryLockFileExclusive(int fd) {
#ifdef _WIN32
	HANDLE handle = reinterpret_cast<HANDLE>(::_get_osfhandle(fd));
	if (handle == INVALID_HANDLE_VALUE) {
		throw DBException("tryLockFileExclusive: invalid file descriptor");
	}
	// Windows byte-range locks are mandatory: they block ReadFile/WriteFile from
	// other handles on the locked range. Lock a single byte far past any real
	// content so other processes can still read the file (e.g. the holder
	// diagnostics a lock file carries) while the lock is held.
	OVERLAPPED overlapped{};
	overlapped.Offset = 0xFFFFFFFE;
	overlapped.OffsetHigh = 0x7FFFFFFF;
	if (::LockFileEx(handle, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlapped)) {
		return true;
	}
	DWORD error = ::GetLastError();
	// A non-blocking conflict surfaces as ERROR_LOCK_VIOLATION, or as
	// ERROR_IO_PENDING on some handle/overlapped configurations — both mean
	// "already locked by someone else", not a hard failure.
	if (error == ERROR_LOCK_VIOLATION || error == ERROR_IO_PENDING) {
		return false;
	}
	throw DBException("tryLockFileExclusive: LockFileEx failed with error " + std::to_string(error));
#else
	while (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
		if (errno == EWOULDBLOCK) {
			return false;
		}
		// Some filesystems do not implement flock and fail with EOPNOTSUPP/ENOTSUP
		// — notably the network/FUSE mounts backing Docker Desktop bind mounts on
		// macOS/Windows, and some CIFS/9p setups. No advisory lock is obtainable
		// there, so degrade to a no-op "acquired" rather than making backups
		// impossible on those volumes (the transaction-log madvise path degrades
		// the same way when the kernel rejects the op). Cross-writer protection is
		// forfeited only where it was unattainable to begin with.
		if (errno == EOPNOTSUPP || errno == ENOTSUP) {
			return true;
		}
		if (errno != EINTR) {
			throw DBException(std::string("tryLockFileExclusive: flock failed: ") + std::strerror(errno));
		}
	}
	return true;
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
