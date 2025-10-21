#ifndef __TRANSACTION_LOG_FILE_H__
#define __TRANSACTION_LOG_FILE_H__

#include <chrono>
#include <filesystem>
#include <atomic>
#include <mutex>
#include <condition_variable>

#ifdef _WIN32
	#define PLATFORM_WINDOWS
#else
	#define PLATFORM_POSIX
#endif

#ifdef PLATFORM_WINDOWS
	// prevent Windows macros from interfering with our function names
	#define WIN32_LEAN_AND_MEAN
	#define NOMINMAX
	#include <windows.h>
	#include <io.h>
#else
	#include <fcntl.h>
	#include <unistd.h>
#endif
#include <sys/stat.h>

namespace rocksdb_js {

struct TransactionLogFile final {
	std::filesystem::path path;
	uint32_t sequenceNumber;

#ifdef PLATFORM_WINDOWS
	HANDLE fileHandle;
#else
	int fd;
#endif

	/**
	 * The size of the file in bytes. This needs to be atomic because write is
	 * async and doesn't block other threads from writing and thus size needs to
	 * be updated atomically.
	 */
	std::atomic<size_t> size;

	/**
	 * The number of active operations on the file.
	 */
	std::atomic<int> activeOperations;

	/**
	 * The mutex used to protect the file.
	 */
	std::mutex fileMutex;

	/**
	 * The condition variable used to wait for all active operations to
	 * complete.
	 */
	std::condition_variable closeCondition;

	TransactionLogFile(const std::filesystem::path& p, const uint32_t seq);

	// prevent copying
	TransactionLogFile(const TransactionLogFile&) = delete;
	TransactionLogFile& operator=(const TransactionLogFile&) = delete;

	~TransactionLogFile();

	void close();
	void open();
	std::chrono::system_clock::time_point getLastWriteTime();
	int64_t readFromFile(void* buffer, size_t size, int64_t offset = -1);
	int64_t writeToFile(const void* buffer, size_t size, int64_t offset = -1);
};

} // namespace rocksdb_js

#endif