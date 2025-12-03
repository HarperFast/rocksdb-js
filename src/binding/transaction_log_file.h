#ifndef __TRANSACTION_LOG_FILE_H__
#define __TRANSACTION_LOG_FILE_H__

#include <chrono>
#include <filesystem>
#include <mutex>

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

#define TRANSACTION_LOG_TOKEN 0x574f4f46
#define TRANSACTION_LOG_FILE_HEADER_SIZE 13
#define TRANSACTION_LOG_ENTRY_HEADER_SIZE 13
#define TRANSACTION_LOG_ENTRY_LAST_FLAG 0x01

namespace rocksdb_js {

// forward declarations
struct TransactionLogEntryBatch;

struct TransactionLogFile final {
	/**
	 * The path to the transaction log file.
	 */
	std::filesystem::path path;

	/**
	 * The sequence number of the transaction log file.
	 */
	uint32_t sequenceNumber;

#ifdef PLATFORM_WINDOWS
	/**
	 * The Windows file handle for the transaction log file.
	 */
	HANDLE fileHandle = INVALID_HANDLE_VALUE;
#else
	/**
	 * The POSIX file descriptor for the transaction log file.
	 */
	int fd = -1;
#endif

	/**
	 * The version of the file format.
	 */
	uint8_t version = 1;

	/**
	 * The timestamp of the most recent transaction log batch that has been
	 * written to the file.
	 */
	double timestamp;

	/**
	 * The size of the file in bytes.
	 */
	uint32_t size = 0;

	/**
	 * The mutex used to protect the file (open/close, read/write, etc).
	 */
	std::mutex fileMutex;

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
	 * Gets the last write time of the log file or throws an error if the file
	 * does not exist.
	 */
	std::chrono::system_clock::time_point getLastWriteTime();

	/**
	 * Opens the log file for reading and writing.
	 */
 	void open(const double latestTimestamp);

	/**
	 * Closes the log file and removes it.
	 *
	 * @returns `true` if the file was removed, `false` if it did not exist.
	 */
	bool removeFile();

	/**
	 * Writes a batch of transaction log entries to the log file.
	 *
	 * @param batch The batch of entries to write with state tracking.
	 * @param maxFileSize The maximum file size limit (0 = no limit).
	 */
	void writeEntries(TransactionLogEntryBatch& batch, const uint32_t maxFileSize = 0);

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
	 * Writes a batch of transaction log entries to the log file using version 1
	 * of the transaction log file format.
	 *
	 * @param batch The batch of entries to write with state tracking.
	 * @param maxFileSize The maximum file size limit (0 = no limit).
	 */
	void writeEntriesV1(TransactionLogEntryBatch& batch, const uint32_t maxFileSize);

	/**
	 * Platform specific function that writes data to the log file.
	 */
	int64_t writeToFile(const void* buffer, uint32_t size, int64_t offset = -1);
};

} // namespace rocksdb_js

#endif
