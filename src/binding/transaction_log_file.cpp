#include <cmath>
#include "transaction_log_file.h"

// include platform-specific implementation
#ifdef PLATFORM_WINDOWS
	#include "transaction_log_file_windows.cpp"
#else
	#include "transaction_log_file_posix.cpp"
#endif

namespace rocksdb_js {

TransactionLogFile::~TransactionLogFile() {
	this->close();
}

void TransactionLogFile::open() {
	std::lock_guard<std::mutex> lock(this->fileMutex);
	this->openFile();

	// read the file header
	char buffer[4];
	if (this->size == 0) {
		// file is empty, initialize it
		DEBUG_LOG("%p TransactionLogFile::open Initializing empty file: %s\n", this, this->path.string().c_str())
		writeUint32BE(buffer, this->version);
		this->writeToFile(buffer, sizeof(buffer));
		writeUint32BE(buffer, this->blockSize);
		this->writeToFile(buffer, sizeof(buffer));
		this->size = 8;
	} else if (this->size < 8) {
		throw std::runtime_error("File is too small to be a valid transaction log file: " + this->path.string());
	} else {
		// try to read the version and block size from the file
		int64_t result = this->readFromFile(buffer, sizeof(buffer), 0);
		if (result < 0) {
			throw std::runtime_error("Failed to read version from file: " + this->path.string());
		}
		this->version = readUint32BE(buffer);

		result = this->readFromFile(buffer, sizeof(buffer), 4);
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
	std::unique_lock<std::mutex> fileLock(this->fileMutex);

	const uint32_t BLOCK_HEADER_SIZE = 12;
	const uint32_t BLOCK_SIZE = this->blockSize;
	const uint32_t BLOCK_BODY_SIZE = BLOCK_SIZE - BLOCK_HEADER_SIZE;
	const uint32_t TXN_HEADER_SIZE = 12;

	// calculate total transaction size (all entry data + transaction headers)
	uint32_t totalTxnSize = 0;
	for (auto& entry : entries) {
		totalTxnSize += TXN_HEADER_SIZE + static_cast<uint32_t>(entry->size);
	}

	// check if there's a partially-filled block we can append to (read under lock)
	uint32_t currentBlockSize = this->currentBlockSize;
	uint32_t availableInCurrentBlock = (currentBlockSize > 0) ? BLOCK_BODY_SIZE - currentBlockSize : 0;

	// calculate how much data will go into new blocks vs existing block
	uint32_t dataForCurrentBlock = std::min(totalTxnSize, availableInCurrentBlock);
	uint32_t dataForNewBlocks = totalTxnSize - dataForCurrentBlock;

	// calculate number of new blocks needed
	uint32_t numNewBlocks = dataForNewBlocks > 0
		? static_cast<uint32_t>(std::ceil(static_cast<double>(dataForNewBlocks) / BLOCK_BODY_SIZE))
		: 0;

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Total transaction size=%u, currentBlockSize=%u, availableInCurrentBlock=%u, dataForCurrentBlock=%u, dataForNewBlocks=%u, numNewBlocks=%u\n",
		this, totalTxnSize, currentBlockSize, availableInCurrentBlock, dataForCurrentBlock, dataForNewBlocks, numNewBlocks)

	/*
	// build the write buffers
	// allocate buffers: one for appending to current block (if any) + buffers for new blocks
	// maximum iovecs needed: 1 (current block data) + numNewBlocks * 2 (header + body per new block)
	uint32_t maxIovecs = (dataForCurrentBlock > 0 ? 1 : 0) + (numNewBlocks * 2);
	auto blockHeaders = std::make_unique<char[]>(numNewBlocks * BLOCK_HEADER_SIZE);
	auto blockBodies = std::make_unique<char[]>((dataForCurrentBlock > 0 ? dataForCurrentBlock : 0) + (numNewBlocks * BLOCK_BODY_SIZE));
	auto iovecs = std::make_unique<iovec[]>(maxIovecs);
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
	for (uint32_t blockIdx = 0; blockIdx < numNewBlocks; blockIdx++) {
		// Build block header
		char* blockHeader = blockHeaders.get() + (blockIdx * BLOCK_HEADER_SIZE);

		// Timestamp (8 bytes)
		writeUint64BE(blockHeader, timestamp);

		// Flags (2 bytes) - first new block is a continuation if we appended to existing block
		uint16_t flags = (dataForCurrentBlock > 0 || blockIdx > 0) ? 0x0001 : 0x0000; // CONTINUATION flag
		writeUint16BE(blockHeader + 8, flags);

		// Data offset (2 bytes) - where next transaction starts in this block
		// For first block (when not appending): 12 (right after block header)
		// For continuation blocks: 0 (TODO: handle properly for mid-transaction continuations)
		uint16_t dataOffset = (dataForCurrentBlock == 0 && blockIdx == 0) ? BLOCK_HEADER_SIZE : 0;
		writeUint16BE(blockHeader + 10, dataOffset);

		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 New block %u: timestamp=%llu, flags=%u, dataOffset=%u\n",
			this, blockIdx, timestamp, flags, dataOffset)

		// Add block header to iovec
		iovecs[iovecsIndex++] = {blockHeader, BLOCK_HEADER_SIZE};

		// Add block body to iovec (full size for now, we'll adjust last one later)
		char* blockBody = dataWritePtr + (blockIdx * BLOCK_BODY_SIZE);
		iovecs[iovecsIndex++] = {blockBody, BLOCK_BODY_SIZE};
	}

	// Now fill the buffer with transaction data
	// We have a unified buffer with: [current block data] + [new block bodies]
	// Track our position as we write across this buffer
	char* writePtr = blockBodies.get();
	uint32_t bytesWrittenTotal = 0;
	uint32_t currentLogicalBlock = dataForCurrentBlock > 0 ? 0 : 1; // 0 = existing block, 1+ = new blocks
	uint32_t currentBlockOffset = 0; // offset within the current logical block

	// Helper to get available space in current logical block
	auto getAvailableInBlock = [&]() -> uint32_t {
		if (currentLogicalBlock == 0) {
			// writing to existing block
			return dataForCurrentBlock - currentBlockOffset;
		} else {
			// writing to new blocks
			return BLOCK_BODY_SIZE - currentBlockOffset;
		}
	};

	// Helper to advance to next logical block
	auto advanceToNextBlock = [&]() {
		currentLogicalBlock++;
		currentBlockOffset = 0;
	};

	for (auto& entry : entries) {
		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Writing entry to store \"%s\" (size=%zu)\n",
			this, entry->store->name.c_str(), entry->size)

		// Write transaction header (timestamp + length)
		char txnHeader[TXN_HEADER_SIZE];
		writeUint64BE(txnHeader, timestamp);
		writeUint32BE(txnHeader + 8, static_cast<uint32_t>(entry->size));

		// Copy transaction header to buffer
		uint32_t txnHeaderBytesToCopy = TXN_HEADER_SIZE;
		uint32_t txnHeaderOffset = 0;

		while (txnHeaderBytesToCopy > 0) {
			uint32_t available = getAvailableInBlock();
			uint32_t toCopy = std::min(txnHeaderBytesToCopy, available);

			std::memcpy(writePtr + bytesWrittenTotal,
			           txnHeader + txnHeaderOffset,
			           toCopy);

			bytesWrittenTotal += toCopy;
			currentBlockOffset += toCopy;
			txnHeaderOffset += toCopy;
			txnHeaderBytesToCopy -= toCopy;

			// Move to next block if current is full
			if (currentBlockOffset >= (currentLogicalBlock == 0 ? dataForCurrentBlock : BLOCK_BODY_SIZE)) {
				advanceToNextBlock();
			}
		}

		// Copy transaction data to buffer
		uint32_t dataBytesToCopy = static_cast<uint32_t>(entry->size);
		uint32_t dataOffset = 0;

		while (dataBytesToCopy > 0) {
			uint32_t available = getAvailableInBlock();
			uint32_t toCopy = std::min(dataBytesToCopy, available);

			std::memcpy(writePtr + bytesWrittenTotal,
			           entry->data + dataOffset,
			           toCopy);

			bytesWrittenTotal += toCopy;
			currentBlockOffset += toCopy;
			dataOffset += toCopy;
			dataBytesToCopy -= toCopy;

			// Move to next block if current is full
			if (currentBlockOffset >= (currentLogicalBlock == 0 ? dataForCurrentBlock : BLOCK_BODY_SIZE)) {
				advanceToNextBlock();
			}
		}
	}

	// Adjust the last iovec size to only write actual data, not padding
	// The last iovec could be:
	// - The current block data (if we only appended to existing block)
	// - The last new block body (if we created new blocks)
	if (iovecsIndex > 0) {
		size_t lastIovecIdx = iovecsIndex - 1;

		if (dataForCurrentBlock > 0 && numNewBlocks == 0) {
			// Only appended to existing block - already sized correctly in iovec
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Last iovec (current block) size: %zu bytes\n",
				this, iovecs[lastIovecIdx].iov_len)
		} else if (numNewBlocks > 0) {
			// Created new blocks - adjust last block body size
			uint32_t lastBlockBodySize = dataForNewBlocks % BLOCK_BODY_SIZE;
			if (lastBlockBodySize == 0) {
				lastBlockBodySize = BLOCK_BODY_SIZE; // Last block is exactly full
			}
			iovecs[lastIovecIdx].iov_len = lastBlockBodySize;

			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Last block body size adjusted to %u bytes\n",
				this, lastBlockBodySize)
		}
	}

	// Release the lock during I/O operation since writeBatchToFile will acquire it briefly
	// to check if file is open and manage activeOperations
	fileLock.unlock();

	// Write all data using writev
	int64_t bytesWritten = this->writeBatchToFile(iovecs.get(), static_cast<int>(iovecsIndex));

	if (bytesWritten < 0) {
		throw std::runtime_error("Failed to write transaction log entries to file");
	}

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Wrote %lld bytes to log file\n", this, bytesWritten)

	// Re-acquire the lock to update metadata
	fileLock.lock();

	// Update file tracking: currentBlockSize and blockCount
	// Calculate the new currentBlockSize (how many bytes in the last block)
	uint32_t newBlockSize;
	if (dataForCurrentBlock > 0 && numNewBlocks == 0) {
		// Only appended to existing block
		newBlockSize = currentBlockSize + dataForCurrentBlock;
	} else if (numNewBlocks > 0) {
		// Created new blocks
		uint32_t lastBlockBodySize = dataForNewBlocks % BLOCK_BODY_SIZE;
		newBlockSize = (lastBlockBodySize == 0) ? BLOCK_BODY_SIZE : lastBlockBodySize;
	} else {
		// No data written (shouldn't happen)
		newBlockSize = currentBlockSize;
	}

	// If the last block is full, reset to 0 (ready for next block)
	if (newBlockSize >= BLOCK_BODY_SIZE) {
		newBlockSize = 0;
	}

	// Update file metadata atomically (all under fileMutex)
	this->currentBlockSize = newBlockSize;
	this->size += static_cast<uint32_t>(bytesWritten);

	// Update block count (add numNewBlocks, plus 1 if we filled the current block)
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
	*/
}

} // namespace rocksdb_js
