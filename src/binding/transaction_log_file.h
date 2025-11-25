#ifndef __TRANSACTION_LOG_FILE_H__
#define __TRANSACTION_LOG_FILE_H__

#include <chrono>
#include <filesystem>
#include <mutex>
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

#define TRANSACTION_LOG_TOKEN 0x574f4f46
#define TRANSACTION_LOG_FILE_HEADER_SIZE 6
#define TRANSACTION_LOG_ENTRY_HEADER_SIZE 22

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
	 * The size of the file in bytes.
	 */
	uint32_t size = 0;

	/**
	 * The mutex used to protect the file and its metadata
	 * (currentBlockSize, blockCount, size).
	 */
	std::mutex fileMutex;

	/**
	 * The earliest active transaction timestamp.
	 */
	double earliestActiveTransactionTimestamp = 0;

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
 	void open();

	void removeFile();

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
	 * Platform specific function that writes data to the log file.
	 */
	int64_t writeToFile(const void* buffer, uint32_t size, int64_t offset = -1);

	/**
	 * Writes a batch of transaction log entries to the log file using version 1
	 * of the transaction log file format.
	 *
	 * @param batch The batch of entries to write with state tracking.
	 * @param maxFileSize The maximum file size limit (0 = no limit).
	 */
	void writeEntriesV1(TransactionLogEntryBatch& batch, const uint32_t maxFileSize);

	/**
	 * Calculates the available space in the file.
	 *
	 * @param batch The batch of entries to write with state tracking.
	 * @param maxFileSize The maximum file size limit (0 = no limit).
	 * @param availableSpaceInCurrentBlock The available space in the current block.
	 * @return The available space in the file.
	 */
	// int64_t getAvailableSpaceInFile(
	// 	const TransactionLogEntryBatch& batch,
	// 	const uint32_t maxIndexFileSize,
	// 	const uint32_t availableSpaceInCurrentBlock
	// );

	/**
	 * Calculates the entries to write to the file.
	 *
	 * @param batch The batch of entries to write with state tracking.
	 * @param availableSpaceInCurrentBlock The available space in the current block.
	 * @param availableSpaceInFile The available space in the file.
	 * @param totalTxnSize The total transaction size.
	 * @param numEntriesToWrite The number of entries to write.
	 * @param dataForCurrentBlock The data for the current block.
	 * @param dataForNewBlocks The data for the new blocks.
	 * @param numNewBlocks The number of new blocks.
	 */
	// void calculateEntriesToWrite(
	// 	const TransactionLogEntryBatch& batch,
	// 	const uint32_t availableSpaceInCurrentBlock,
	// 	const int64_t availableSpaceInFile,
	// 	uint32_t& totalTxnSize, // out
	// 	uint32_t& numEntriesToWrite, // out
	// 	uint32_t& dataForCurrentBlock, // out
	// 	uint32_t& dataForNewBlocks, // out
	// 	uint32_t& numNewBlocks // out
	// );
};

} // namespace rocksdb_js

#endif
