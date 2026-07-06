#ifndef __CORE_PLATFORM_H__
#define __CORE_PLATFORM_H__

#include <chrono>
#include <cstddef>
#include <filesystem>

namespace rocksdb_js {

size_t getThreadId();

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
