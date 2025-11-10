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

#define WOOF_TOKEN 0x574F4F46
#define FILE_HEADER_SIZE 10
#define BLOCK_HEADER_SIZE 14
#define TXN_HEADER_SIZE 12
#define CONTINUATION_FLAG ((uint16_t)0x0001)

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
	 * The size of the block body in bytes.
	 */
	uint32_t blockBodySize = 4096 - BLOCK_HEADER_SIZE;

	/**
	 * The size of the current block in bytes.
	 */
	uint32_t currentBlockSize = 0;

	/**
	 * The number of blocks in the file.
	 */
	uint32_t blockCount = 0;

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
	 *
	 * @param batch The batch of entries to write with state tracking.
	 * @param maxFileSize The maximum file size limit (0 = no limit).
	 */
	void writeEntries(TransactionLogEntryBatch& batch, uint32_t maxFileSize = 0);

private:
	/**
	 * Platform specific function that flushes file buffers to disk.
	 */
	void flushFile();

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
	void writeEntriesV1(TransactionLogEntryBatch& batch, uint32_t maxFileSize);

	uint32_t getAvailableSpaceInFile(
		const TransactionLogEntryBatch& batch,
		uint32_t maxFileSize,
		uint32_t availableSpaceInCurrentBlock
	);

	void calculateEntriesToWrite(
		const TransactionLogEntryBatch& batch,
		uint32_t availableSpaceInCurrentBlock,
		uint32_t availableSpaceInFile,
		uint32_t& totalTxnSize,
		uint32_t& numEntriesToWrite,
		uint32_t& dataForCurrentBlock,
		uint32_t& dataForNewBlocks,
		uint32_t& numNewBlocks
	);

	/**
	 * Helper struct to track write position across blocks.
	 */
	struct WriteContext {
		char* writePtr;
		uint32_t bytesWritten;
		uint32_t totalBytesProcessed;
		uint32_t currentBlockIdx;
		uint32_t currentBlockOffset;
		uint32_t dataForCurrentBlock;
		uint32_t blockBodySize;

		inline uint32_t getAvailableInBlock() const {
			if (currentBlockIdx == 0) {
				return dataForCurrentBlock - currentBlockOffset;
			}
			return blockBodySize - currentBlockOffset;
		}

		inline uint32_t getBlockCapacity() const {
			return (currentBlockIdx == 0) ? dataForCurrentBlock : blockBodySize;
		}

		inline void advanceToNextBlock() {
			currentBlockIdx++;
			currentBlockOffset = 0;
		}

		inline bool needsBlockAdvance() const {
			return currentBlockOffset >= getBlockCapacity();
		}
	};

	/**
	 * Writes data across block boundaries, handling splits automatically.
	 *
	 * @param ctx Write context tracking position
	 * @param data Source data pointer
	 * @param size Number of bytes to write
	 * @param totalTxnSize Total transaction size limit
	 * @return Number of bytes actually written
	 */
	uint32_t writeDataAcrossBlocks(
		WriteContext& ctx,
		const char* data,
		uint32_t size,
		uint32_t totalTxnSize
	);

	/**
	 * Helper struct to represent block distribution for data.
	 */
	struct BlockDistribution {
		uint32_t dataForCurrentBlock;
		uint32_t dataForNewBlocks;
		uint32_t numNewBlocks;
		uint32_t bytesOnDisk;
	};

	/**
	 * Calculates how data would be distributed across blocks.
	 *
	 * @param dataSize Total data size to distribute
	 * @param availableSpaceInCurrentBlock Space available in current block
	 * @return BlockDistribution struct with calculated values
	 */
	BlockDistribution calculateBlockDistribution(
		uint32_t dataSize,
		uint32_t availableSpaceInCurrentBlock
	) const;
};

} // namespace rocksdb_js

#endif
