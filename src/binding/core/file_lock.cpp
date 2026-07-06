#include "core/file_lock.h"
#include "core/exception.h"
#include <filesystem>
#include <mutex>
#include <string>
#include <unordered_map>
#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#endif

namespace rocksdb_js {

namespace {

#ifdef _WIN32
using NativeHandle = HANDLE;
#else
using NativeHandle = int;
#endif

// The registry is a process-global singleton shared across every Node env that
// loads this binary (worker_threads included), so its map must be mutex-guarded.
// Tokens are opaque uint32s handed to JS and passed back to release; the OS
// handle never leaves native code. 0 is never a valid token.
std::mutex g_locksMutex;
std::unordered_map<uint32_t, NativeHandle> g_locks;
uint32_t g_nextToken = 1;

uint32_t registerHandle(NativeHandle handle) {
	std::lock_guard<std::mutex> guard(g_locksMutex);
	uint32_t token = g_nextToken++;
	// Skip 0 on the (practically unreachable) wrap; backups are rare and tokens
	// are released promptly, so 2^32 live acquisitions cannot realistically occur.
	if (g_nextToken == 0) {
		g_nextToken = 1;
	}
	g_locks[token] = handle;
	return token;
}

} // namespace

uint32_t tryAcquireFileLock(const std::string& file) {
	std::filesystem::path lockPath = file;

#ifdef _WIN32
	HANDLE handle = ::CreateFileW(
		lockPath.c_str(),
		GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
		nullptr,
		OPEN_ALWAYS,
		FILE_ATTRIBUTE_NORMAL,
		nullptr
	);
	if (handle == INVALID_HANDLE_VALUE) {
		DWORD error = ::GetLastError();
		if (error == ERROR_PATH_NOT_FOUND || error == ERROR_FILE_NOT_FOUND) {
			throw DBException("Backup directory does not exist: " + backupDir);
		}
		throw DBException("tryAcquireFileLock: CreateFile failed with error " + std::to_string(error));
	}
	// Lock a single byte far past EOF: Windows range locks are mandatory and would
	// otherwise block a contender from reading the file for diagnostics.
	OVERLAPPED overlapped{};
	overlapped.Offset = 0xFFFFFFFE;
	overlapped.OffsetHigh = 0x7FFFFFFF;
	if (!::LockFileEx(handle, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlapped)) {
		DWORD error = ::GetLastError();
		::CloseHandle(handle);
		// A non-blocking conflict surfaces as ERROR_LOCK_VIOLATION, or as
		// ERROR_IO_PENDING on some configurations — both mean "already locked".
		if (error == ERROR_LOCK_VIOLATION || error == ERROR_IO_PENDING) {
			return 0;
		}
		throw DBException("tryAcquireFileLock: LockFileEx failed with error " + std::to_string(error));
	}
	return registerHandle(handle);
#else
	int fd = ::open(lockPath.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0666);
	if (fd < 0) {
		// O_CREAT can't create the file when the file itself is missing; surface a
		// clear error rather than a raw ENOENT.
		if (errno == ENOENT) {
			throw DBException("File does not exist: " + file);
		}
		throw DBException(std::string("tryAcquireFileLock: open failed: ") + std::strerror(errno));
	}
	while (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
		if (errno == EWOULDBLOCK) {
			::close(fd);
			return 0;
		}
		// Some filesystems don't implement flock and fail with EOPNOTSUPP/ENOTSUP
		// — notably the FUSE/9p mounts behind Docker Desktop bind mounts, and some
		// CIFS setups. No advisory lock is obtainable there, so degrade to a no-op
		// "acquired" (still tracked so release closes the fd) rather than making
		// backups impossible. Cross-writer protection is forfeited only where it
		// was unattainable to begin with.
		if (errno == EOPNOTSUPP || errno == ENOTSUP) {
			break;
		}
		if (errno != EINTR) {
			int saved = errno;
			::close(fd);
			throw DBException(std::string("tryAcquireFileLock: flock failed: ") + std::strerror(saved));
		}
	}
	return registerHandle(fd);
#endif
}

void releaseFileLock(uint32_t token) {
	if (token == 0) {
		return;
	}
	NativeHandle handle;
	{
		std::lock_guard<std::mutex> guard(g_locksMutex);
		auto it = g_locks.find(token);
		if (it == g_locks.end()) {
			return;
		}
		handle = it->second;
		g_locks.erase(it);
	}
	// Closing the handle releases the kernel lock.
#ifdef _WIN32
	::CloseHandle(handle);
#else
	::close(handle);
#endif
}

} // namespace rocksdb_js
