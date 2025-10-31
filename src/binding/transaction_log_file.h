#ifndef __TRANSACTION_LOG_FILE_H__
#define __TRANSACTION_LOG_FILE_H__

#include <chrono>
#include <filesystem>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include "transaction_log_entry.h"

#ifdef _WIN32
	#define PLATFORM_WINDOWS
#else
	#define PLATFORM_POSIX
#endif

#ifdef PLATFORM_WINDOWS
	// prevent Windows macros from interfering with our function names
	#define WIN32_LEAN_AND_MEAN
	#ifndef NOMINMAX
		#define NOMINMAX
	#endif
	#include <windows.h>
	#include <io.h>

	// define iovec for Windows compatibility
	struct iovec {
		void* iov_base;
		size_t iov_len;
	};
#else
	#include <fcntl.h>
	#include <unistd.h>
	#include <sys/uio.h>
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
	 * The version of the file format.
	 */
	uint32_t version = 1;

	/**
	 * The size of the block in bytes.
	 */
	uint32_t blockSize = 4096;

	/**
	 * The size of the current block in bytes.
	 * Protected by fileMutex.
	 */
	uint32_t currentBlockSize = 0;

	/**
	 * The number of blocks in the file.
	 * Protected by fileMutex.
	 */
	uint32_t blockCount = 0;

	/**
	 * The size of the file in bytes.
	 * Protected by fileMutex.
	 */
	uint32_t size = 0;

	/**
	 * The mutex used to protect the file and its metadata
	 * (currentBlockSize, blockCount, size).
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

	/**
	 * Closes the log file.
	 */
	void close();

	/**
	 * Opens the log file for reading and writing.
	 */
 	void open();

	/**
	 * Gets the last write time of the log file or throws an error if the file
	 * does not exist.
	 */
	std::chrono::system_clock::time_point getLastWriteTime();

	/**
	 * Writes a batch of transaction log entries to the log file.
	 */
	void writeEntries(const uint64_t timestamp, const std::vector<std::unique_ptr<TransactionLogEntry>>& entries);

private:
	/**
	 * Platform specific function that opens the log file for reading and writing.
	 */
	void openFile();

	/**
	 * Platform specific function that reads data from the log file.
	 */
	int64_t readFromFile(void* buffer, uint32_t size, int64_t offset = -1);

	/**
	 * Platform specific function that writes multiple buffers to the log file.
	 */
	int64_t writeBatchToFile(const iovec* iovecs, int iovcnt);

	/**
	 * Platform specific function that writes data to the log file.
	 */
	int64_t writeToFile(const void* buffer, uint32_t size, int64_t offset = -1);

	/**
	 * Writes a batch of transaction log entries to the log file using version 1
	 * of the transaction log file format.
	 */
	void writeEntriesV1(const uint64_t timestamp, const std::vector<std::unique_ptr<TransactionLogEntry>>& entries);
};

} // namespace rocksdb_js

#endif
