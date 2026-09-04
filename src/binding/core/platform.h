#ifndef __CORE_PLATFORM_H__
#define __CORE_PLATFORM_H__

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>

namespace rocksdb_js {

size_t getThreadId();

/**
 * The effective per-process open-file limit: the soft `RLIMIT_NOFILE`, further
 * capped by `kern.maxfilesperproc` on macOS (the kernel enforces it even when
 * the rlimit is higher). Returns `0` when the limit cannot be determined
 * (e.g. Windows, where no comparable per-process fd limit applies).
 */
uint64_t getEffectiveOpenFileLimit();

/**
 * Derives a bounded RocksDB `max_open_files` budget from the effective
 * per-process open-file limit: an eighth of the limit — the limit is a
 * process-wide budget shared by every database opened in the process (each
 * derives independently) plus sockets, transaction logs, WAL, and everything
 * else — clamped to [1024, 262144]. Returns `-1` (unlimited) when the limit
 * is unknown (`0`).
 */
int32_t deriveMaxOpenFiles(uint64_t effectiveOpenFileLimit);

/**
 * Sets the current thread's name for diagnostics (visible in top, gdb, etc.).
 * Best-effort and platform-guarded; names longer than the OS limit (15 chars
 * on Linux) are truncated to fit (manually on Linux, where the OS errors
 * instead of truncating).
 */
void setThreadName(const char* name);

std::chrono::system_clock::time_point convertFileTimeToSystemTime(const std::filesystem::file_time_type& fileTime);

/**
 * Exclusive upper bound of the millisecond-timestamp domain (the largest
 * JavaScript `Date`, 8.64e15 ms). Transaction timestamps, transaction-log batch
 * keys and the clock floor all live below it.
 */
constexpr double MAX_TIMESTAMP_MS = 8.64e15;

/**
 * How far ahead of the wall clock a clock-floor seed may sit before it is
 * refused as corruption rather than honored as a rollback to recover from: ten
 * years. A durable key can only be ahead of the wall clock by the size of the
 * rollback that followed it, while a seed beyond this would move every
 * timestamp the process issues, for every database, that far into the future
 * for the life of the process.
 */
constexpr double MAX_CLOCK_FLOOR_SKEW_MS = 10.0 * 365.25 * 24.0 * 3600.0 * 1000.0;

/** The wall clock in milliseconds, without the monotonic floor applied. */
double getWallClockTimestamp();

double getMonotonicTimestamp();

/**
 * Raises the floor `getMonotonicTimestamp()` issues above, to `floor`. Raise-only
 * and refused outside the timestamp domain or more than MAX_CLOCK_FLOOR_SKEW_MS
 * ahead of the wall clock; returns whether the floor moved. Seeded at open from
 * the transaction log the caller named as locally originated (see
 * `TransactionLogStoreRegistry::SeedTimestampFloor`).
 */
bool raiseMonotonicTimestampFloor(double floor);

/** Same, against a caller-supplied plausible bound, so one seed pass classifies
 * every candidate against one clock reading. */
bool raiseMonotonicTimestampFloor(double floor, double plausibleBound);

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
