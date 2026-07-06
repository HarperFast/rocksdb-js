#ifndef __CORE_PLATFORM_H__
#define __CORE_PLATFORM_H__

#include <chrono>
#include <cstddef>
#include <filesystem>

namespace rocksdb_js {

size_t getThreadId();

/**
 * Attempts to take a non-blocking exclusive advisory lock on an open file
 * descriptor (`flock` on POSIX, `LockFileEx` on Windows). Returns `true` if the
 * lock was acquired, `false` if another open file description holds it —
 * including one in another process, container, or `worker_threads` worker.
 *
 * The kernel owns the lock: it is released when the descriptor is closed,
 * including implicitly when the holding process dies, so a crashed holder can
 * never leave a stale lock. There is no unlock function — close the descriptor.
 *
 * Throws `DBException` on real failures (bad descriptor, unsupported
 * filesystem).
 */
bool tryLockFileExclusive(int fd);

std::chrono::system_clock::time_point convertFileTimeToSystemTime(const std::filesystem::file_time_type& fileTime);

double getMonotonicTimestamp();

void tryCreateDirectory(
	const std::filesystem::path& path,
	std::filesystem::perms permissions =
		std::filesystem::perms::owner_read |
		std::filesystem::perms::owner_write |
		std::filesystem::perms::owner_exec |
		std::filesystem::perms::group_read |
		std::filesystem::perms::group_exec,
	uint8_t retries = 3
);

} // namespace rocksdb_js

#endif
