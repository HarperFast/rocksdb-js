#include <cmath>
#include "transaction_log_file.h"

// include platform-specific implementation
#ifdef PLATFORM_WINDOWS
	#include "transaction_log_file_windows.cpp"
#else
	#include "transaction_log_file_posix.cpp"
#endif

#define WOOF_TOKEN 0x574F4F46
#define FILE_HEADER_SIZE 10
#define BLOCK_HEADER_SIZE 14
#define TXN_HEADER_SIZE 12

namespace rocksdb_js {

TransactionLogFile::~TransactionLogFile() {
	this->close();
}

std::chrono::system_clock::time_point TransactionLogFile::getLastWriteTime() {
	std::lock_guard<std::mutex> fileLock(this->fileMutex);

	if (!std::filesystem::exists(this->path)) {
		throw std::filesystem::filesystem_error(
			"File does not exist",
			this->path,
			std::make_error_code(std::errc::no_such_file_or_directory)
		);
	}

	auto mtime = std::filesystem::last_write_time(this->path);
	return convertFileTimeToSystemTime(mtime);
}

void TransactionLogFile::open() {
	std::lock_guard<std::mutex> fileLock(this->fileMutex);
	this->openFile();

	// read the file header
	char buffer[4];
	if (this->size == 0) {
		// file is empty, initialize it
		DEBUG_LOG("%p TransactionLogFile::open Initializing empty file: %s\n", this, this->path.string().c_str())
		writeUint32BE(buffer, WOOF_TOKEN);
		this->writeToFile(buffer, 4);
		writeUint16BE(buffer, this->version);
		this->writeToFile(buffer, 2);
		writeUint32BE(buffer, this->blockSize);
		this->writeToFile(buffer, 4);
		this->size = FILE_HEADER_SIZE;
	} else if (this->size < 8) {
		throw std::runtime_error("File is too small to be a valid transaction log file: " + this->path.string());
	} else {
		// try to read the WOOF token, version and block size from the file
		int64_t result = this->readFromFile(buffer, sizeof(buffer), 0);
		if (result < 0) {
			throw std::runtime_error("Failed to read version from file: " + this->path.string());
		}
		uint32_t token = readUint32BE(buffer);
		if (token != WOOF_TOKEN) {
			throw std::runtime_error("Invalid transaction log file: " + this->path.string());
		}

		result = this->readFromFile(buffer, sizeof(buffer), 4);
		if (result < 0) {
			throw std::runtime_error("Failed to read version from file: " + this->path.string());
		}
		this->version = readUint16BE(buffer);

		if (this->version != 1) {
			throw std::runtime_error("Unsupported transaction log file version: " + std::to_string(this->version));
		}

		result = this->readFromFile(buffer, sizeof(buffer), 6);
		if (result < 0) {
			throw std::runtime_error("Failed to block size from file: " + this->path.string());
		}
		this->blockSize = readUint32BE(buffer);
	}

	uint32_t blockCount = static_cast<uint32_t>(std::ceil(static_cast<double>(this->size - 8) / this->blockSize));
	this->blockCount = blockCount;

	DEBUG_LOG("%p TransactionLogFile::open Opened %s (fd=%d, version=%u, size=%zu, blockSize=%u, blockCount=%u)\n",
		this, this->path.string().c_str(), this->fd, this->version, this->size, this->blockSize, blockCount)
}

void TransactionLogFile::writeEntries(const uint64_t timestamp, const std::vector<std::unique_ptr<TransactionLogEntry>>& entries) {
	DEBUG_LOG("%p TransactionLogFile::writeEntries Writing batch with %zu entries (timestamp=%llu)\n",
		this, entries.size(), timestamp)

	// Branch based on file format version
	if (this->version == 1) {
		this->writeEntriesV1(timestamp, entries);
	} else {
		throw std::runtime_error("Unsupported transaction log file version: " + std::to_string(this->version));
	}
}

void TransactionLogFile::writeEntriesV1(const uint64_t timestamp, const std::vector<std::unique_ptr<TransactionLogEntry>>& entries) {
	std::lock_guard<std::mutex> fileLock(this->fileMutex);

	const uint32_t BLOCK_BODY_SIZE = this->blockSize - BLOCK_HEADER_SIZE;

	// calculate total transaction size (all entry data + transaction headers)
	uint32_t totalTxnSize = 0;
	for (auto& entry : entries) {
		totalTxnSize += TXN_HEADER_SIZE + static_cast<uint32_t>(entry->size);
	}

	// check if there's a partially-filled block we can append to (read under lock)
	uint32_t currentBlockSize = this->currentBlockSize;
	bool hasCurrentBlock = this->blockCount > 0;
	uint32_t availableInCurrentBlock = hasCurrentBlock ? (BLOCK_BODY_SIZE - currentBlockSize) : 0;

	// calculate how much data will go into new blocks vs existing block
	uint32_t dataForCurrentBlock = std::min(totalTxnSize, availableInCurrentBlock);
	uint32_t dataForNewBlocks = totalTxnSize - dataForCurrentBlock;

	// calculate number of new blocks needed
	uint32_t numNewBlocks = dataForNewBlocks > 0
		? static_cast<uint32_t>(std::ceil(static_cast<double>(dataForNewBlocks) / BLOCK_BODY_SIZE))
		: 0;

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Total transaction size=%u, hasCurrentBlock=%d, currentBlockSize=%u, availableInCurrentBlock=%u, dataForCurrentBlock=%u, dataForNewBlocks=%u, numNewBlocks=%u\n",
		this, totalTxnSize, hasCurrentBlock, currentBlockSize, availableInCurrentBlock, dataForCurrentBlock, dataForNewBlocks, numNewBlocks)

	// allocate buffers: one for appending to current block (if any) + buffers for new blocks
	auto blockHeaders = std::make_unique<char[]>(numNewBlocks * BLOCK_HEADER_SIZE);
	auto blockBodies = std::make_unique<char[]>(
		(dataForCurrentBlock > 0 ? dataForCurrentBlock : 0) +
		(totalTxnSize > availableInCurrentBlock ? totalTxnSize - availableInCurrentBlock : 0u) // (numNewBlocks * BLOCK_BODY_SIZE);
	);

	// total iovecs needed: 1 (current block data) + numNewBlocks * 2 (header + body per new block)
	auto iovecsCount = (dataForCurrentBlock > 0 ? 1 : 0) + (numNewBlocks * 2);
	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Allocating %u iovecs\n", this, iovecsCount)
	auto iovecs = std::make_unique<iovec[]>(iovecsCount);
	size_t iovecsIndex = 0;

	// pointer to where we write transaction data
	char* dataWritePtr = blockBodies.get();

	// if appending to existing block, add iovec for that data (no header)
	if (dataForCurrentBlock > 0) {
		iovecs[iovecsIndex++] = {dataWritePtr, dataForCurrentBlock};
		dataWritePtr += dataForCurrentBlock;

		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Appending %u bytes to existing block\n",
			this, dataForCurrentBlock)
	}

	// build headers and iovecs for new blocks
	for (uint32_t blockIdx = 0; blockIdx < numNewBlocks; ++blockIdx) {
		// build block header
		char* blockHeader = blockHeaders.get() + (blockIdx * BLOCK_HEADER_SIZE);

		// timestamp (8 bytes)
		writeUint64BE(blockHeader, timestamp);

		// flags (2 bytes) - first new block is a continuation if we appended to existing block
		uint16_t flags = (dataForCurrentBlock > 0 || blockIdx > 0) ? 0x0001 : 0x0000; // CONTINUATION flag
		writeUint16BE(blockHeader + 8, flags);

		// data offset (2 bytes) - where next transaction starts in this block
		// for the first new block (when not appending to existing block):
		//   - If transaction fits entirely in this block: BLOCK_HEADER_SIZE + totalTxnSize + 1
		//   - If transaction overflows to next block: 0
		// for continuation blocks: 0
		uint32_t dataOffset = 0;
		if (dataForCurrentBlock == 0 && blockIdx == 0) {
			// First new block
			if (totalTxnSize < BLOCK_BODY_SIZE) {
				// Transaction fits entirely in this block
				dataOffset = BLOCK_HEADER_SIZE + totalTxnSize + 1;
			} else {
				// Transaction overflows to continuation blocks
				dataOffset = 0;
			}
		}
		writeUint32BE(blockHeader + 10, dataOffset);

		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 New block %u: timestamp=%llu, flags=%u, dataOffset=%u\n",
			this, blockIdx, timestamp, flags, dataOffset)

		// Add block header to iovec
		iovecs[iovecsIndex++] = {blockHeader, BLOCK_HEADER_SIZE};

		// Add block body to iovec (full size for now, we'll adjust last one later)
		char* blockBody = dataWritePtr + (blockIdx * BLOCK_BODY_SIZE);
		iovecs[iovecsIndex++] = {blockBody, BLOCK_BODY_SIZE};
	}

	// now fill the buffer with transaction data
	// we have a unified buffer with: [current block data] + [new block bodies]
	// track our position as we write across this buffer
	char* writePtr = blockBodies.get();
	uint32_t bytesWrittenTotal = 0;
	uint32_t currentLogicalBlock = dataForCurrentBlock > 0 ? 0 : 1; // 0 = existing block, 1+ = new blocks
	uint32_t currentBlockOffset = 0; // offset within the current logical block

	// helper to get available space in current logical block
	auto getAvailableInBlock = [&]() -> uint32_t {
		if (currentLogicalBlock == 0) {
			// writing to existing block
			return dataForCurrentBlock - currentBlockOffset;
		} else {
			// writing to new blocks
			return BLOCK_BODY_SIZE - currentBlockOffset;
		}
	};

	// helper to advance to next logical block
	auto advanceToNextBlock = [&]() {
		currentLogicalBlock++;
		currentBlockOffset = 0;
	};

	for (auto& entry : entries) {
		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Processing entry (size=%zu)\n", this, entry->size)

		// write transaction header (timestamp + length)
		char txnHeader[TXN_HEADER_SIZE];
		writeUint64BE(txnHeader, timestamp);
		writeUint32BE(txnHeader + 8, static_cast<uint32_t>(entry->size));

		// copy transaction header to buffer
		uint32_t txnHeaderBytesToCopy = TXN_HEADER_SIZE;
		uint32_t txnHeaderOffset = 0;

		// the header could get split across multiple blocks, so we need to
		// carefully write it
		while (txnHeaderBytesToCopy > 0) {
			uint32_t available = getAvailableInBlock();
			uint32_t toCopy = std::min(txnHeaderBytesToCopy, available);

			std::memcpy(
				writePtr + bytesWrittenTotal,
				txnHeader + txnHeaderOffset,
				toCopy
			);

			bytesWrittenTotal += toCopy;
			currentBlockOffset += toCopy;
			txnHeaderOffset += toCopy;
			txnHeaderBytesToCopy -= toCopy;

			// move to next block if current is full
			if (currentBlockOffset >= (currentLogicalBlock == 0 ? dataForCurrentBlock : BLOCK_BODY_SIZE)) {
				advanceToNextBlock();
			}
		}

		// copy transaction data to buffer
		uint32_t dataBytesToCopy = static_cast<uint32_t>(entry->size);
		uint32_t dataOffset = 0;

		while (dataBytesToCopy > 0) {
			uint32_t available = getAvailableInBlock();
			uint32_t toCopy = std::min(dataBytesToCopy, available);

			std::memcpy(
				writePtr + bytesWrittenTotal,
				entry->data + dataOffset,
				toCopy
			);

			bytesWrittenTotal += toCopy;
			currentBlockOffset += toCopy;
			dataOffset += toCopy;
			dataBytesToCopy -= toCopy;

			// move to next block if current is full
			if (currentBlockOffset >= (currentLogicalBlock == 0 ? dataForCurrentBlock : BLOCK_BODY_SIZE)) {
				advanceToNextBlock();
			}
		}
	}

	// adjust the last iovec size to only write actual data, not padding
	// the last iovec could be:
	// - the current block data (if we only appended to existing block)
	// - the last new block body (if we created new blocks)
	if (iovecsIndex > 0) {
		size_t lastIovecIdx = iovecsIndex - 1;

		if (dataForCurrentBlock > 0 && numNewBlocks == 0) {
			// only appended to existing block - already sized correctly in iovec
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Last iovec (current block) size: %zu bytes\n",
				this, iovecs[lastIovecIdx].iov_len)
		} else if (numNewBlocks > 0) {
			// created new blocks - adjust last block body size
			uint32_t lastBlockBodySize = dataForNewBlocks % BLOCK_BODY_SIZE;
			if (lastBlockBodySize == 0) {
				lastBlockBodySize = BLOCK_BODY_SIZE; // last block is exactly full
			}
			iovecs[lastIovecIdx].iov_len = lastBlockBodySize;

			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Last block body size adjusted to %u bytes\n",
				this, lastBlockBodySize)
		}
	}

	// write all data using writev
	int64_t bytesWritten = this->writeBatchToFile(iovecs.get(), static_cast<int>(iovecsIndex));

	if (bytesWritten < 0) {
		throw std::runtime_error("Failed to write transaction log entries to file");
	}

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Wrote %lld bytes to log file\n", this, bytesWritten)

	// update file tracking: currentBlockSize and blockCount
	// calculate the new currentBlockSize (how many bytes in the last block)
	uint32_t newBlockSize;
	if (dataForCurrentBlock > 0 && numNewBlocks == 0) {
		// only appended to existing block
		newBlockSize = currentBlockSize + dataForCurrentBlock;
	} else if (numNewBlocks > 0) {
		// created new blocks
		uint32_t lastBlockBodySize = dataForNewBlocks % BLOCK_BODY_SIZE;
		newBlockSize = (lastBlockBodySize == 0) ? BLOCK_BODY_SIZE : lastBlockBodySize;
	} else {
		// no data written (shouldn't happen)
		newBlockSize = currentBlockSize;
	}

	// if the last block is full, reset to 0 (ready for next block)
	if (newBlockSize >= BLOCK_BODY_SIZE) {
		newBlockSize = 0;
	}

	// update file metadata atomically (all under fileMutex)
	this->currentBlockSize = newBlockSize;
	this->size += static_cast<uint32_t>(bytesWritten);

	// update block count (add numNewBlocks, plus 1 if we filled the current block)
	uint32_t blocksAdded = numNewBlocks;
	if (dataForCurrentBlock > 0 && newBlockSize == 0) {
		// We filled the current block
		blocksAdded++;
	}
	if (blocksAdded > 0) {
		this->blockCount += blocksAdded;
	}

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Updated currentBlockSize=%u, added %u blocks, total blockCount=%u\n",
		this, newBlockSize, blocksAdded, this->blockCount)
}

} // namespace rocksdb_js
