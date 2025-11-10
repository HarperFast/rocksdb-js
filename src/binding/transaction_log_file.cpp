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
		this->flushFile();
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
		if (this->blockSize <= BLOCK_HEADER_SIZE) {
			throw std::runtime_error("Block size is too small to be a valid transaction log file: " + this->path.string());
		}
		this->blockBodySize = this->blockSize - BLOCK_HEADER_SIZE;
	}

	uint32_t blockCount = static_cast<uint32_t>(std::ceil(static_cast<double>(this->size - FILE_HEADER_SIZE) / this->blockSize));
	this->blockCount = blockCount;

	DEBUG_LOG("%p TransactionLogFile::open Opened %s (version=%u, size=%zu, blockSize=%u, blockCount=%u)\n",
		this, this->path.string().c_str(), this->version, this->size, this->blockSize, blockCount)
}

void TransactionLogFile::writeEntries(TransactionLogEntryBatch& batch, uint32_t maxFileSize) {
	DEBUG_LOG("%p TransactionLogFile::writeEntries Writing batch with %zu entries, current entry index=%zu, bytes written=%zu (timestamp=%llu, maxFileSize=%u, currentSize=%u)\n",
		this, batch.entries.size(), batch.currentEntryIndex, batch.currentEntryBytesWritten, batch.timestamp, maxFileSize, this->size)

	// Branch based on file format version
	if (this->version == 1) {
		this->writeEntriesV1(batch, maxFileSize);
	} else {
		throw std::runtime_error("Unsupported transaction log file version: " + std::to_string(this->version));
	}
}

void TransactionLogFile::writeEntriesV1(TransactionLogEntryBatch& batch, uint32_t maxFileSize) {
	/**
	 * Strategy:
	 * 1. Calculate available space and entries to write
	 * 2. Allocate buffers for block headers and bodies
	 * 3. Build block headers and iovec array
	 * 4. Write transaction headers and data using helper method
	 * 5. Write to file and update metadata
	 */

	std::lock_guard<std::mutex> fileLock(this->fileMutex);

	uint32_t currentBlockSize = this->currentBlockSize;
	uint32_t availableSpaceInCurrentBlock = this->blockCount > 0 ? (this->blockBodySize - currentBlockSize) : 0;
	uint32_t availableSpaceInFile = this->getAvailableSpaceInFile(batch, maxFileSize, availableSpaceInCurrentBlock);

	if (availableSpaceInFile == 0) {
		return;
	}

	// calculate what to write
	uint32_t totalTxnSize = 0;
	uint32_t numEntriesToWrite = 0;
	uint32_t dataForCurrentBlock = 0;
	uint32_t dataForNewBlocks = 0;
	uint32_t numNewBlocks = 0;
	this->calculateEntriesToWrite(batch, availableSpaceInCurrentBlock, availableSpaceInFile, totalTxnSize, numEntriesToWrite, dataForCurrentBlock, dataForNewBlocks, numNewBlocks);

	if (numEntriesToWrite == 0 || totalTxnSize == 0) {
		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 No entries to write\n", this)
		return;
	}

	// calculate initial data offset for continuation across files
	uint32_t initialDataOffset = 0;
	if (numNewBlocks > 0 && batch.currentEntryBytesWritten > 0 && dataForCurrentBlock == 0) {
		auto& currentEntry = batch.entries[batch.currentEntryIndex];
		initialDataOffset = currentEntry->size - batch.currentEntryBytesWritten;
		if (initialDataOffset > this->blockBodySize) {
			initialDataOffset = 0;
		}
	}

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Will write %u entries (totalTxnSize=%u, currentBlockSize=%u, availableSpaceInCurrentBlock=%u, dataForCurrentBlock=%u, numNewBlocks=%u, dataForNewBlocks=%u, availableSpaceInFile=%u, initialDataOffset=%u)\n",
		this, numEntriesToWrite, totalTxnSize, currentBlockSize, availableSpaceInCurrentBlock, dataForCurrentBlock, numNewBlocks, dataForNewBlocks, availableSpaceInFile, initialDataOffset)

	// allocate buffers
	auto blockHeaders = std::make_unique<char[]>(numNewBlocks * BLOCK_HEADER_SIZE);
	uint32_t totalBodySize = dataForCurrentBlock + dataForNewBlocks;
	auto blockBodies = std::make_unique<char[]>(totalBodySize);

	// build iovecs: current block (optional) + (header + body) for each new block
	auto iovecsCount = (dataForCurrentBlock > 0 ? 1 : 0) + (numNewBlocks * 2);
	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Allocating %u iovecs\n", this, iovecsCount)
	auto iovecs = std::make_unique<iovec[]>(iovecsCount);
	size_t iovecsIndex = 0;

	char* dataWritePtr = blockBodies.get();

	// add iovec for existing block if appending
	if (dataForCurrentBlock > 0) {
		iovecs[iovecsIndex++] = {dataWritePtr, dataForCurrentBlock};
		dataWritePtr += dataForCurrentBlock;
		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Appending %u bytes to existing block\n", this, dataForCurrentBlock)
	}

	// build block headers and iovecs for new blocks
	for (uint32_t blockIdx = 0; blockIdx < numNewBlocks; ++blockIdx) {
		char* blockHeader = blockHeaders.get() + (blockIdx * BLOCK_HEADER_SIZE);

		// timestamp (8 bytes)
		writeUint64BE(blockHeader, batch.timestamp);

		// flags (2 bytes) - continuation if: appending to current block, not first new block, or continuing from previous file
		bool isContinuation = (dataForCurrentBlock > 0) || (blockIdx > 0) || (batch.currentEntryBytesWritten > 0);
		uint16_t flags = isContinuation ? CONTINUATION_FLAG : 0x0000;
		writeUint16BE(blockHeader + 8, flags);

		// data offset (4 bytes)
		uint32_t dataOffset = (blockIdx == 0 && initialDataOffset > 0) ? initialDataOffset : 0;
		writeUint32BE(blockHeader + 10, dataOffset);

		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 New block %u: timestamp=%llu, flags=%u, dataOffset=%u\n",
			this, blockIdx, batch.timestamp, flags, dataOffset)

		iovecs[iovecsIndex++] = {blockHeader, BLOCK_HEADER_SIZE};
		iovecs[iovecsIndex++] = {dataWritePtr + (blockIdx * this->blockBodySize), this->blockBodySize};
	}

	// initialize write context
	WriteContext ctx = {
		blockBodies.get(),
		0,  // bytesWritten
		0,  // totalBytesProcessed
		dataForCurrentBlock > 0 ? 0u : 1u,  // currentBlockIdx (0 = existing block, 1+ = new blocks)
		0,  // currentBlockOffset
		dataForCurrentBlock,
		this->blockBodySize
	};

	// write transaction data
	size_t entriesProcessed = 0;
	for (size_t entryIdx = batch.currentEntryIndex; entryIdx < batch.entries.size() && entriesProcessed < numEntriesToWrite; ++entryIdx, ++entriesProcessed) {
		auto& entry = batch.entries[entryIdx];
		uint32_t entryStartOffset = (entryIdx == batch.currentEntryIndex) ? static_cast<uint32_t>(batch.currentEntryBytesWritten) : 0;
		uint32_t entryRemainingSize = static_cast<uint32_t>(entry->size) - entryStartOffset;

		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Processing entry %zu (size=%zu, startOffset=%u, remainingSize=%u)\n",
			this, entryIdx, entry->size, entryStartOffset, entryRemainingSize)

		// write transaction header for new entries
		if (entryStartOffset == 0) {
			char txnHeader[TXN_HEADER_SIZE];
			writeUint64BE(txnHeader, batch.timestamp);
			writeUint32BE(txnHeader + 8, static_cast<uint32_t>(entry->size));
			this->writeDataAcrossBlocks(ctx, txnHeader, TXN_HEADER_SIZE, totalTxnSize);
		}

		// write transaction data
		uint32_t entryBytesWritten = this->writeDataAcrossBlocks(ctx, entry->data + entryStartOffset, entryRemainingSize, totalTxnSize);

		// update batch state
		if (entryStartOffset + entryBytesWritten >= entry->size) {
			batch.currentEntryIndex = entryIdx + 1;
			batch.currentEntryBytesWritten = 0;
		} else {
			batch.currentEntryBytesWritten = entryStartOffset + entryBytesWritten;
		}
	}

	// adjust last iovec to actual size (not full block padding)
	uint32_t lastNewBlockBodySize = 0;
	if (numNewBlocks > 0 && ctx.currentBlockIdx > 0) {
		lastNewBlockBodySize = ctx.currentBlockOffset == 0 ? this->blockBodySize : ctx.currentBlockOffset;
	}

	if (numNewBlocks > 0 && iovecsIndex > 0) {
		size_t lastIovecIdx = iovecsIndex - 1;
		iovecs[lastIovecIdx].iov_len = lastNewBlockBodySize;
		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Last block body size adjusted to %u bytes\n", this, lastNewBlockBodySize)
	}

	// write to file
	int64_t bytesWritten = this->writeBatchToFile(iovecs.get(), static_cast<int>(iovecsIndex));
	if (bytesWritten < 0) {
		throw std::runtime_error("Failed to write transaction log entries to file");
	}

	this->flushFile();
	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Wrote %lld bytes to log file\n", this, bytesWritten)

	// update file metadata
	uint32_t newBlockSize;
	if (dataForCurrentBlock > 0 && numNewBlocks == 0) {
		newBlockSize = currentBlockSize + dataForCurrentBlock;
	} else if (numNewBlocks > 0) {
		newBlockSize = lastNewBlockBodySize;
	} else {
		newBlockSize = currentBlockSize;
	}

	if (newBlockSize >= this->blockBodySize) {
		newBlockSize = 0;
	}

	this->currentBlockSize = newBlockSize;
	this->size += static_cast<uint32_t>(bytesWritten);

	// update block count: add new blocks plus one if we filled current block
	uint32_t blocksAdded = numNewBlocks + ((dataForCurrentBlock > 0 && newBlockSize == 0) ? 1 : 0);
	this->blockCount += blocksAdded;

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Updated currentBlockSize=%u, added %u blocks, total blockCount=%u, batch state: entryIndex=%zu, bytesWritten=%zu\n",
		this, newBlockSize, blocksAdded, this->blockCount, batch.currentEntryIndex, batch.currentEntryBytesWritten)
}

uint32_t TransactionLogFile::getAvailableSpaceInFile(
	const TransactionLogEntryBatch& batch,
	uint32_t maxFileSize,
	uint32_t availableSpaceInCurrentBlock
) {
	uint32_t availableSpace = 0;

	// calculate how many bytes we can write before hitting the max file size limit
	if (maxFileSize > 0) {
		if (this->size >= maxFileSize) {
			// already at or over the max file size limit
			DEBUG_LOG("%p TransactionLogFile::getAvailableSpaceInFile File already at max size (%u >= %u), deferring to next file\n",
				this, this->size, maxFileSize)
			return 0;
		}
		availableSpace = maxFileSize - this->size;
	}

	// early check optimization: do we have minimum space to write anything?
	if (availableSpace > 0) {
		uint32_t minimumRequired = 0;

		if (availableSpaceInCurrentBlock > 0) {
			// can append to current block
			// if we haven't written any of the current entry yet, we need space for the header
			if (batch.currentEntryBytesWritten == 0) {
				// we haven't written any of the entry, so we need to write the
				// header and at least 1 byte of data
				minimumRequired = TXN_HEADER_SIZE + 1;
			} else {
				// already wrote header, just need 1 byte of data
				minimumRequired = 1;
			}
		} else {
			// need to create a new block
			minimumRequired = BLOCK_HEADER_SIZE + TXN_HEADER_SIZE + 1;
		}

		if (availableSpace < minimumRequired) {
			DEBUG_LOG("%p TransactionLogFile::getAvailableSpaceInFile Not enough space to write anything (need %u, have %u), deferring to next file\n",
				this, minimumRequired, availableSpace)
			return 0;
		}
	}

	return availableSpace;
}

void TransactionLogFile::calculateEntriesToWrite(
	const TransactionLogEntryBatch& batch,
	uint32_t availableSpaceInCurrentBlock,
	uint32_t availableSpaceInFile,
	uint32_t& totalTxnSize,
	uint32_t& numEntriesToWrite,
	uint32_t& dataForCurrentBlock,
	uint32_t& dataForNewBlocks,
	uint32_t& numNewBlocks
) {
	// start from the current entry in the batch (which could be a continuation)
	for (size_t i = batch.currentEntryIndex; i < batch.entries.size(); ++i) {
		auto& entry = batch.entries[i];

		// calculate entry size including header for new entries
		bool isContinuation = (i == batch.currentEntryIndex && batch.currentEntryBytesWritten > 0);
		uint32_t entrySize = isContinuation
			? (entry->size - batch.currentEntryBytesWritten)
			: (entry->size + TXN_HEADER_SIZE);

		uint32_t candidateSize = totalTxnSize + entrySize;

		// calculate block distribution for this candidate size
		BlockDistribution dist = this->calculateBlockDistribution(candidateSize, availableSpaceInCurrentBlock);

		if (dist.bytesOnDisk <= availableSpaceInFile) {
			// entry fits completely
			totalTxnSize = candidateSize;
			dataForCurrentBlock = dist.dataForCurrentBlock;
			dataForNewBlocks = dist.dataForNewBlocks;
			numNewBlocks = dist.numNewBlocks;
			numEntriesToWrite++;
		} else {
			// entry would exceed limit - write partial entry if possible
			uint32_t overage = dist.bytesOnDisk - availableSpaceInFile;
			uint32_t adjustedSize = candidateSize - overage;

			// recalculate distribution with adjusted size
			BlockDistribution adjustedDist = this->calculateBlockDistribution(adjustedSize, availableSpaceInCurrentBlock);

			// only write partial entry if we're continuing or have space for header + data
			if (isContinuation || adjustedSize > TXN_HEADER_SIZE) {
				totalTxnSize = adjustedSize;
				dataForCurrentBlock = adjustedDist.dataForCurrentBlock;
				dataForNewBlocks = adjustedDist.dataForNewBlocks;
				numNewBlocks = adjustedDist.numNewBlocks;
				numEntriesToWrite++;
			}
			break;
		}
	}
}

TransactionLogFile::BlockDistribution TransactionLogFile::calculateBlockDistribution(
	uint32_t dataSize,
	uint32_t availableSpaceInCurrentBlock
) const {
	BlockDistribution dist;
	dist.dataForCurrentBlock = std::min(dataSize, availableSpaceInCurrentBlock);
	dist.dataForNewBlocks = dataSize - dist.dataForCurrentBlock;
	dist.numNewBlocks = dist.dataForNewBlocks > 0
		? static_cast<uint32_t>(std::ceil(static_cast<double>(dist.dataForNewBlocks) / this->blockBodySize))
		: 0;
	dist.bytesOnDisk = dist.dataForCurrentBlock + (dist.numNewBlocks * BLOCK_HEADER_SIZE) + dist.dataForNewBlocks;
	return dist;
}

uint32_t TransactionLogFile::writeDataAcrossBlocks(
	WriteContext& ctx,
	const char* data,
	uint32_t size,
	uint32_t totalTxnSize
) {
	uint32_t remainingSpace = totalTxnSize - ctx.totalBytesProcessed;
	uint32_t bytesToWrite = std::min(size, remainingSpace);
	uint32_t bytesWritten = 0;
	uint32_t srcOffset = 0;

	while (bytesToWrite > 0) {
		uint32_t available = ctx.getAvailableInBlock();
		uint32_t toCopy = std::min(bytesToWrite, available);

		std::memcpy(ctx.writePtr + ctx.bytesWritten, data + srcOffset, toCopy);

		ctx.bytesWritten += toCopy;
		ctx.currentBlockOffset += toCopy;
		srcOffset += toCopy;
		bytesToWrite -= toCopy;
		ctx.totalBytesProcessed += toCopy;
		bytesWritten += toCopy;

		if (ctx.needsBlockAdvance()) {
			ctx.advanceToNextBlock();
		}
	}

	return bytesWritten;
}

} // namespace rocksdb_js
