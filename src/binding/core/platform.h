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
 * Process-wide wall-clock ratchet: Unix-epoch milliseconds, strictly increasing
 * across every call in the process (`nextafter` on a backward step or a tie).
 * This is the transaction timestamp and log batch key, so it must stay in the
 * epoch domain; after a backward wall step it can only advance one ulp per call
 * until the wall catches up, so it does not measure elapsed time. Use
 * `getSteadyClockNow()` for that.
 */
double getMonotonicTimestamp();

/**
 * Converts a `steady_clock` duration since its origin to fractional
 * milliseconds. Positive scaling followed by IEEE round-to-nearest is
 * non-decreasing, so distinct readings never invert order; they can collapse to
 * equality once the double's spacing exceeds the clock's period (about 2 ns of
 * spacing at 100 days from the origin, 60 ns at 10 years).
 */
double steadyClockMilliseconds(std::chrono::steady_clock::duration sinceOrigin);

/**
 * `std::chrono::steady_clock::now()` as fractional milliseconds from an
 * unspecified origin that is fixed for the life of the process. Comparable
 * across every thread and env in the process; not comparable across processes
 * or restarts, and unrelated to the epoch (never compare with
 * `getMonotonicTimestamp()` or wall time). Non-decreasing, not unique, and
 * unaffected by wall-clock steps. Whether time spent in host suspend counts is
 * platform-defined. Stateless: one clock read, no addon lock.
 */
double getSteadyClockNow();

/**
 * Resolves a path to the one spelling this process uses to identify it:
 * symlinks and `..`/`.` components collapsed, relative paths made absolute.
 * Falls back to a purely lexical absolute path when the filesystem cannot be
 * consulted (a missing parent, a permission error) so it never throws.
 *
 * Identity comparisons — the registry key for a secondary workspace, the
 * primary/secondary nesting check, the advisory lock file — must go through
 * this, or two spellings of one directory read as two directories.
 */
std::filesystem::path resolveIdentityPath(const std::string& path);

/**
 * True when `child` is `parent` itself or lives underneath it. Both arguments
 * should already be resolved (see `resolveIdentityPath`). Existing paths are
 * compared by filesystem identity (`equivalent()` over the child's ancestors),
 * which is correct on case-sensitive and case-insensitive volumes alike —
 * case sensitivity is a volume property, not an OS one. A path that does not
 * exist falls back to a lexical, case-preserving prefix test, where a `parent`
 * that already ends in a separator (a filesystem root such as `/` or `C:/`) is
 * handled: the separator is not required twice.
 */
bool isPathWithin(const std::filesystem::path& parent, const std::filesystem::path& child);

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
