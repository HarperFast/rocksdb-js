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

uint32_t TransactionLogFile::getAvailableSpaceInFile(
	const TransactionLogEntryBatch& batch,
	uint32_t maxFileSize,
	uint32_t availableSpaceInCurrentBlock
) {
	uint32_t availableSpace = 0;

	// calculate how many bytes we can write before hitting the limit
	if (maxFileSize > 0) {
		if (this->size >= maxFileSize) {
			// already at or over the limit
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 File already at max size (%u >= %u), deferring to next file\n",
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
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Not enough space to write anything (need %u, have %u), deferring to next file\n",
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
	uint32_t bytesOnDisk = 0;

	// start from the current entry in the batch (which could be a continuation)
	for (size_t i = batch.currentEntryIndex; i < batch.entries.size(); ++i) {
		auto& entry = batch.entries[i];

		uint32_t entrySize = entry->size;
		if (i == batch.currentEntryIndex && batch.currentEntryBytesWritten > 0) {
			entrySize -= batch.currentEntryBytesWritten;
		} else {
			entrySize += TXN_HEADER_SIZE;
		}

		uint32_t newSize = totalTxnSize + entrySize;

		// check if adding this entry would exceed available space (including block headers)
		dataForCurrentBlock = std::min(newSize, availableSpaceInCurrentBlock);
		dataForNewBlocks = newSize - dataForCurrentBlock;
		numNewBlocks = dataForNewBlocks > 0
			? static_cast<uint32_t>(std::ceil(static_cast<double>(dataForNewBlocks) / this->blockBodySize))
			: 0;
		bytesOnDisk = dataForCurrentBlock + (numNewBlocks * BLOCK_HEADER_SIZE) + dataForNewBlocks;

		if (bytesOnDisk <= availableSpaceInFile) {
			// fits completely, add it
			totalTxnSize += entrySize;
			numEntriesToWrite++;
		} else {
			// `entrySize` would exceed limit, reduce the size until it fits
			uint32_t overage = bytesOnDisk - availableSpaceInFile;
			if (dataForNewBlocks == 0) {
				dataForCurrentBlock -= overage;
			} else if (dataForNewBlocks > overage) {
				dataForNewBlocks -= overage;
			} else {
				overage -= dataForNewBlocks;
				dataForNewBlocks = 0;
				dataForCurrentBlock -= overage;
			}
			newSize -= overage;
			if ((i == batch.currentEntryIndex && batch.currentEntryBytesWritten > 0) || newSize > TXN_HEADER_SIZE) {
				// continuing an entry or enough space for the header
				totalTxnSize += newSize;
				numEntriesToWrite++;
			}
			break;
		}
	}
}

void TransactionLogFile::writeEntriesV1(TransactionLogEntryBatch& batch, uint32_t maxFileSize) {
	std::lock_guard<std::mutex> fileLock(this->fileMutex);

	uint32_t currentBlockSize = this->currentBlockSize;
	uint32_t availableSpaceInCurrentBlock = this->blockCount > 0 ? (this->blockBodySize - currentBlockSize) : 0;
	uint32_t availableSpaceInFile = this->getAvailableSpaceInFile(batch, maxFileSize, availableSpaceInCurrentBlock);

	if (availableSpaceInFile == 0) {
		return;
	}

	// calculate total transaction size for entries we want to write
	// check against availableSpace, accounting for block headers
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

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Will write %u entries (totalTxnSize=%u, currentBlockSize=%u, availableSpaceInCurrentBlock=%u, dataForCurrentBlock=%u, numNewBlocks=%u, dataForNewBlocks=%u, availableSpaceInFile=%u)\n",
		this, numEntriesToWrite, totalTxnSize, currentBlockSize, availableSpaceInCurrentBlock, dataForCurrentBlock, numNewBlocks, dataForNewBlocks, availableSpaceInFile)

	// calculate initial data offset for first new block (if it's a continuation)
	// this tells us where the first transaction header starts in the first new block
	uint32_t initialDataOffset = 0;
	if (numNewBlocks > 0 && batch.currentEntryBytesWritten > 0 && dataForCurrentBlock == 0) {
		auto& currentEntry = batch.entries[batch.currentEntryIndex];
		initialDataOffset = currentEntry->size - batch.currentEntryBytesWritten;
		if (initialDataOffset > this->blockBodySize) {
			initialDataOffset = 0;
		}
	}

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Initial data offset for first new block: %u\n",
		this, initialDataOffset)

	// allocate buffers: one for appending to current block (if any) + buffers for new blocks
	auto blockHeaders = std::make_unique<char[]>(numNewBlocks * BLOCK_HEADER_SIZE);
	auto blockBodies = std::make_unique<char[]>(
		(dataForCurrentBlock > 0 ? dataForCurrentBlock : 0) +
		(totalTxnSize > availableSpaceInCurrentBlock ? totalTxnSize - availableSpaceInCurrentBlock : 0u)
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

		// start timestamp (8 bytes)
		writeUint64BE(blockHeader, batch.timestamp);

		// flags (2 bytes) - set continuation flag if:
		// 1. we appended to existing block in this file, OR
		// 2. this is not the first new block, OR
		// 3. we're continuing an entry from a previous file
		bool isContinuation = (dataForCurrentBlock > 0) ||
		                      (blockIdx > 0) ||
		                      (blockIdx == 0 && batch.currentEntryBytesWritten > 0);
		uint16_t flags = isContinuation ? CONTINUATION_FLAG : 0x0000;
		writeUint16BE(blockHeader + 8, flags);

		// data offset (4 bytes) - where first transaction header starts in this
		// block (relative to block body)
		uint32_t dataOffset = 0;
		if (blockIdx == 0 && initialDataOffset > 0) {
			// first new block with continuation from previous file
			dataOffset = initialDataOffset;
		}
		writeUint32BE(blockHeader + 10, dataOffset);

		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 New block %u: start timestamp=%llu, flags=%u, dataOffset=%u\n",
			this, blockIdx, batch.timestamp, flags, dataOffset)

		// add block header to iovec
		iovecs[iovecsIndex++] = {blockHeader, BLOCK_HEADER_SIZE};

		// add block body to iovec (full size for now, we'll adjust last one later)
		char* blockBody = dataWritePtr + (blockIdx * this->blockBodySize);
		iovecs[iovecsIndex++] = {blockBody, this->blockBodySize};
	}

	// now fill the buffer with transaction data
	// we have a unified buffer with: [current block data] + [new block bodies]
	// track our position as we write across this buffer
	char* writePtr = blockBodies.get();
	uint32_t bytesWrittenTotal = 0;
	uint32_t currentLogicalBlock = dataForCurrentBlock > 0 ? 0 : 1; // 0 = existing block, 1+ = new block(s)
	uint32_t currentBlockOffset = 0; // offset within the current logical block
	uint32_t lastNewBlockBodySize = 0; // track actual bytes written to last new block body

	// helper to get available space in current logical block
	auto getAvailableInBlock = [&]() -> uint32_t {
		if (currentLogicalBlock == 0) {
			// writing to the existing block
			return dataForCurrentBlock - currentBlockOffset;
		} else {
			// writing to the new block(s)
			return this->blockBodySize - currentBlockOffset;
		}
	};

	// helper to advance to next logical block
	auto advanceToNextBlock = [&]() {
		currentLogicalBlock++;
		currentBlockOffset = 0;
	};

	// track how much data we've written to know when to stop
	uint32_t totalBytesProcessed = 0;
	size_t entriesProcessed = 0;

	for (size_t entryIdx = batch.currentEntryIndex; entryIdx < batch.entries.size() && entriesProcessed < numEntriesToWrite; ++entryIdx, ++entriesProcessed) {
		auto& entry = batch.entries[entryIdx];

		// determine starting offset in this entry's data
		uint32_t entryStartOffset = (entryIdx == batch.currentEntryIndex) ? static_cast<uint32_t>(batch.currentEntryBytesWritten) : 0;
		uint32_t entryRemainingSize = static_cast<uint32_t>(entry->size) - entryStartOffset;

		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Processing entry %zu (size=%zu, startOffset=%u, remainingSize=%u)\n",
			this, entryIdx, entry->size, entryStartOffset, entryRemainingSize)

		// write transaction header (timestamp + length) - only for new entries or first partial entry
		if (entryStartOffset == 0) {
			char txnHeader[TXN_HEADER_SIZE];
			writeUint64BE(txnHeader, batch.timestamp);
			writeUint32BE(txnHeader + 8, static_cast<uint32_t>(entry->size));

			// copy transaction header to buffer
			uint32_t txnHeaderBytesToCopy = TXN_HEADER_SIZE;
			uint32_t txnHeaderOffset = 0;

			// check if we have space for header
			uint32_t remainingSpace = totalTxnSize - totalBytesProcessed;
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Remaining space: %u\n", this, remainingSpace)
			txnHeaderBytesToCopy = std::min(txnHeaderBytesToCopy, remainingSpace);

			// the header could get split across multiple blocks
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
				totalBytesProcessed += toCopy;

				// move to next block if current is full
				if (currentBlockOffset >= (currentLogicalBlock == 0 ? dataForCurrentBlock : this->blockBodySize)) {
					advanceToNextBlock();
				}
			}
		}

		// copy transaction data to buffer
		uint32_t dataBytesToCopy = entryRemainingSize;
		uint32_t dataOffset = entryStartOffset;

		// respect the availableSpaceInFile limit
		uint32_t remainingSpace = totalTxnSize - totalBytesProcessed;
		dataBytesToCopy = std::min(dataBytesToCopy, remainingSpace);

		uint32_t entryBytesWritten = 0;

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
			totalBytesProcessed += toCopy;
			entryBytesWritten += toCopy;

			// move to next block if current is full
			if (currentBlockOffset >= (currentLogicalBlock == 0 ? dataForCurrentBlock : this->blockBodySize)) {
				advanceToNextBlock();
			}
		}

		// update batch state
		if (entryStartOffset + entryBytesWritten >= entry->size) {
			// completed this entry, move to next
			batch.currentEntryIndex = entryIdx + 1;
			batch.currentEntryBytesWritten = 0;
		} else {
			// partially wrote this entry
			batch.currentEntryBytesWritten = entryStartOffset + entryBytesWritten;
		}
	}

	// capture the final block's size if we ended in a new block
	if (numNewBlocks > 0 && currentLogicalBlock > 0) {
		lastNewBlockBodySize = currentBlockOffset;
		// if we perfectly filled the last block and advanced, currentBlockOffset is 0
		// but the last block is actually full
		if (lastNewBlockBodySize == 0) {
			lastNewBlockBodySize = this->blockBodySize;
		}
	}

	// adjust the last iovec size to only write actual data, not padding
	if (iovecsIndex > 0) {
		size_t lastIovecIdx = iovecsIndex - 1;

		if (dataForCurrentBlock > 0 && numNewBlocks == 0) {
			// only appended to existing block - already sized correctly in iovec
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Last iovec (current block) size: %zu bytes\n",
				this, iovecs[lastIovecIdx].iov_len)
		} else if (numNewBlocks > 0) {
			// created new blocks - use tracked size from actual writes
			iovecs[lastIovecIdx].iov_len = lastNewBlockBodySize;

			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Last block body size adjusted to %u bytes\n",
				this, lastNewBlockBodySize)
		}
	}

	// write all data using writev
	int64_t bytesWritten = this->writeBatchToFile(iovecs.get(), static_cast<int>(iovecsIndex));

	if (bytesWritten < 0) {
		throw std::runtime_error("Failed to write transaction log entries to file");
	}

	// flush to ensure data is written to disk
	this->flushFile();

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Wrote %lld bytes to log file\n", this, bytesWritten)

	// update file tracking: currentBlockSize and blockCount
	// calculate the new currentBlockSize (how many bytes in the last block)
	uint32_t newBlockSize;
	if (dataForCurrentBlock > 0 && numNewBlocks == 0) {
		// only appended to existing block
		newBlockSize = currentBlockSize + dataForCurrentBlock;
	} else if (numNewBlocks > 0) {
		// created new blocks - use tracked size from actual writes
		newBlockSize = lastNewBlockBodySize;
	} else {
		// no data written (shouldn't happen)
		newBlockSize = currentBlockSize;
	}

	// if the last block is full, reset to 0 (ready for next block)
	if (newBlockSize >= this->blockBodySize) {
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

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Updated currentBlockSize=%u, added %u blocks, total blockCount=%u, batch state: entryIndex=%zu, bytesWritten=%zu\n",
		this, newBlockSize, blocksAdded, this->blockCount, batch.currentEntryIndex, batch.currentEntryBytesWritten)
}

} // namespace rocksdb_js
