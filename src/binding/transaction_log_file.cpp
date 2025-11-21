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
		DEBUG_LOG("%p TransactionLogFile::getLastWriteTime ERROR: File does not exist: %s\n", this, this->path.string().c_str())
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
	char buffer[4];/*
	if (this->size == 0) {
		// file is empty, initialize it
		DEBUG_LOG("%p TransactionLogFile::open Initializing empty file: %s\n", this, this->path.string().c_str())
		writeUint32BE(buffer, WOOF_TOKEN);
		this->writeToFile(buffer, 4);
		writeUint16BE(buffer, this->version);
		this->writeToFile(buffer, 2);
		writeUint16BE(buffer, this->blockSize);
		this->writeToFile(buffer, 2);
		this->size = FILE_HEADER_SIZE;
	} else if (this->size < 8) {
		DEBUG_LOG("%p TransactionLogFile::open ERROR: File is too small to be a valid transaction log file: %s\n", this, this->path.string().c_str())
		throw std::runtime_error("File is too small to be a valid transaction log file: " + this->path.string());
	} else {
		// try to read the WOOF token, version and block size from the file
		int64_t result = this->readFromFile(buffer, sizeof(buffer), 0);
		if (result < 0) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Failed to read version from file: %s\n", this, this->path.string().c_str())
			throw std::runtime_error("Failed to read version from file: " + this->path.string());
		}
		uint32_t token = readUint32BE(buffer);
		if (token != WOOF_TOKEN) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Invalid transaction log file: %s\n", this, this->path.string().c_str())
			throw std::runtime_error("Invalid transaction log file: " + this->path.string());
		}

		result = this->readFromFile(buffer, sizeof(buffer), 4);
		if (result < 0) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Failed to read version from file: %s\n", this, this->path.string().c_str())
			throw std::runtime_error("Failed to read version from file: " + this->path.string());
		}
		this->version = readUint16BE(buffer);

		if (this->version != 1) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Unsupported transaction log file version: %s\n", this, this->path.string().c_str())
			throw std::runtime_error("Unsupported transaction log file version: " + std::to_string(this->version));
		}

		result = this->readFromFile(buffer, sizeof(buffer), 6);
		if (result < 0) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Failed to read block size from file: %s\n", this, this->path.string().c_str())
			throw std::runtime_error("Failed to block size from file: " + this->path.string());
		}
		this->blockSize = readUint16BE(buffer);
		if (this->blockSize <= BLOCK_HEADER_SIZE) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Block size is too small to be a valid transaction log file: %s\n", this, this->path.string().c_str())
			throw std::runtime_error("Block size is too small to be a valid transaction log file: " + this->path.string());
		}
		this->blockBodySize = this->blockSize - BLOCK_HEADER_SIZE;
	}*/

	uint32_t blockCount = static_cast<uint32_t>(std::ceil(static_cast<double>(this->size - FILE_HEADER_SIZE) / this->blockSize));
	this->blockCount = blockCount;

	DEBUG_LOG("%p TransactionLogFile::open Opened %s (version=%u, size=%zu, blockSize=%u, blockCount=%u)\n",
		this, this->path.string().c_str(), this->version, this->size, this->blockSize, blockCount)
}

void TransactionLogFile::writeEntries(TransactionLogEntryBatch& batch, const uint32_t maxFileSize) {
	DEBUG_LOG("%p TransactionLogFile::writeEntries Writing batch with %zu entries, current entry index=%zu, bytes written=%zu (timestamp=%f, maxFileSize=%u, currentSize=%u)\n",
		this, batch.entries.size(), batch.currentEntryIndex, batch.currentEntryBytesWritten, batch.timestamp, maxFileSize, this->size)

	// Branch based on file format version
	if (this->version == 1) {
		this->writeEntriesV1(batch, maxFileSize);
	} else {
		DEBUG_LOG("%p TransactionLogFile::writeEntries Unsupported transaction log file version: %s\n", this, this->path.string().c_str())
		throw std::runtime_error("Unsupported transaction log file version: " + std::to_string(this->version));
	}
}

void TransactionLogFile::writeEntriesV1(TransactionLogEntryBatch& batch, const uint32_t maxFileSize) {
	/**
	 * Strategy (zero-copy optimized):
	 * 1. Calculate available space and entries to write
	 * 2. Build iovecs pointing directly to source buffers
	 * 3. Write using scatter-gather I/O
	 * 4. Update metadata
	 */

	std::lock_guard<std::mutex> fileLock(this->fileMutex);

	uint32_t currentBlockSize = this->currentBlockSize;
	uint32_t availableSpaceInCurrentBlock = this->blockCount > 0 ? (this->blockBodySize - currentBlockSize) : 0;
	int64_t availableSpaceInFile = this->getAvailableSpaceInFile(batch, maxFileSize, availableSpaceInCurrentBlock);

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
		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 No entries to write (availableSpaceInFile=%lli)\n", this, availableSpaceInFile)
		return;
	}

	// calculate initial data offset for continuation across files
	uint32_t initialDataOffset = 0;
	if (numNewBlocks > 0 && batch.currentEntryHeaderWritten && dataForCurrentBlock == 0) {
		auto& currentEntry = batch.entries[batch.currentEntryIndex];
		uint32_t remainingEntrySize = currentEntry->size - batch.currentEntryBytesWritten;
		initialDataOffset = remainingEntrySize;
	}

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Will write %u entries (totalTxnSize=%u, currentBlockSize=%u, availableSpaceInCurrentBlock=%u, dataForCurrentBlock=%u, numNewBlocks=%u, dataForNewBlocks=%u, availableSpaceInFile=%lli, initialDataOffset=%u)\n",
		this, numEntriesToWrite, totalTxnSize, currentBlockSize, availableSpaceInCurrentBlock, dataForCurrentBlock, numNewBlocks, dataForNewBlocks, availableSpaceInFile, initialDataOffset)

	// allocate buffers for headers
	// Note: allocate extra block headers to account for blocks we might skip to avoid splitting transaction headers
	// In the worst case, each entry could require skipping to a new block, so we allocate numNewBlocks + numEntriesToWrite
	uint32_t maxBlockHeaders = numNewBlocks + numEntriesToWrite;
	auto blockHeaders = std::make_unique<char[]>(maxBlockHeaders * BLOCK_HEADER_SIZE);
	auto txnHeaders = std::make_unique<char[]>(numEntriesToWrite * TXN_HEADER_SIZE);

	// allocate iovec tracker for blocks + block headers + entries + transaction headers + padding
	// Use maxBlockHeaders instead of numNewBlocks to account for potential block skips
	// Each block might need padding, so add maxBlockHeaders for padding iovecs
	auto maxIovecs = (dataForCurrentBlock > 0 ? 1 : 0) + (maxBlockHeaders * 3) + (numEntriesToWrite * 2);
	auto iovecs = std::make_unique<iovec[]>(maxIovecs);
	size_t iovecsIndex = 0;

	// track position as we build iovecs
	uint32_t currentBlockIdx = dataForCurrentBlock > 0 ? 0u : 1u;  // 0 = existing block, 1+ = new blocks
	uint32_t currentBlockOffset = 0;
	uint32_t totalBytesProcessed = 0;
	uint32_t txnHeaderIdx = 0;
	uint32_t nextBlockHeaderIdx = 0;

	auto getBlockCapacity = [&]() -> uint32_t {
		return (currentBlockIdx == 0) ? dataForCurrentBlock : this->blockBodySize;
	};

	auto addIovec = [&](void* base, size_t len) {
		if (len > 0) {
			if (iovecsIndex >= maxIovecs) {
				DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 ERROR: iovec overflow! index=%zu, max=%u\n",
					this, iovecsIndex, maxIovecs)
				throw std::runtime_error("iovec array overflow in writeEntriesV1");
			}
			iovecs[iovecsIndex++] = {base, len};
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Added iovec[%zu]: base=%p, len=%zu\n",
				this, iovecsIndex - 1, base, len)
		}
	};

	auto createBlockHeader = [&](bool isContinuation, uint16_t dataOffset) -> char* {
		if (nextBlockHeaderIdx >= maxBlockHeaders) {
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 ERROR: block header overflow! index=%u, max=%u\n",
				this, nextBlockHeaderIdx, maxBlockHeaders)
			throw std::runtime_error("block header overflow in writeEntriesV1");
		}
		char* blockHeader = blockHeaders.get() + (nextBlockHeaderIdx * BLOCK_HEADER_SIZE);
		writeDoubleBE(blockHeader, batch.timestamp);
		uint16_t flags = isContinuation ? CONTINUATION_FLAG : 0;
		writeUint16BE(blockHeader + 8, flags);
		writeUint16BE(blockHeader + 10, dataOffset);
		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Block %u header: timestamp=%f, flags=%u, dataOffset=%u\n",
			this, nextBlockHeaderIdx, batch.timestamp, flags, dataOffset)
		nextBlockHeaderIdx++;
		return blockHeader;
	};

	// static padding buffer for filling blocks
	static const char paddingBytes[4096] = {0};

	auto advanceToNextBlock = [&](bool isContinuation) {
		// write padding to fill the rest of the current block and update
		// `dataOffset` if padding is written (to indicate actual data size)
		if (currentBlockIdx >= 0 && currentBlockOffset > 0) {
			uint32_t blockCapacity = getBlockCapacity();
			if (currentBlockOffset < blockCapacity) {
				uint32_t paddingSize = blockCapacity - currentBlockOffset;
				addIovec(const_cast<char*>(paddingBytes), paddingSize);
				DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Added %u bytes of padding to block %u\n",
					this, paddingSize, currentBlockIdx)

				// update the `dataOffset` for this block's header to indicate actual data size (before padding)
				// this allows the parser to exclude the padding from the transaction data buffer
				if (nextBlockHeaderIdx > 0) {
					char* currentBlockHeader = blockHeaders.get() + ((nextBlockHeaderIdx - 1) * BLOCK_HEADER_SIZE);
					writeUint16BE(currentBlockHeader + 10, static_cast<uint16_t>(currentBlockOffset));
					DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Updated block %u dataOffset to %u (excluding %u bytes of padding)\n",
						this, nextBlockHeaderIdx - 1, currentBlockOffset, paddingSize)
				}
			}
		}

		currentBlockIdx++;
		currentBlockOffset = 0;
		// create and add block header iovec for the new block
		// use nextBlockHeaderIdx to check if we have space allocated, not currentBlockIdx
		if (currentBlockIdx > 0 && nextBlockHeaderIdx < maxBlockHeaders) {
			uint16_t dataOffset = (nextBlockHeaderIdx == 0 && initialDataOffset > 0) ? static_cast<uint16_t>(initialDataOffset) : 0;
			char* blockHeader = createBlockHeader(isContinuation, dataOffset);
			addIovec(blockHeader, BLOCK_HEADER_SIZE);
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Advanced to block %u (continuation=%d)\n", this, currentBlockIdx, isContinuation)
		}
	};

	// if starting with new blocks (not appending to current), add first block header
	if (numNewBlocks > 0 && dataForCurrentBlock == 0) {
		bool isContinuation = batch.currentEntryHeaderWritten;
		uint16_t dataOffset = initialDataOffset > 0 ? static_cast<uint16_t>(initialDataOffset) : 0;
		char* blockHeader = createBlockHeader(isContinuation, dataOffset);
		addIovec(blockHeader, BLOCK_HEADER_SIZE);
		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Added initial block header 0 (continuation=%d)\n", this, isContinuation)
	}

	// process entries and build iovecs pointing directly to source data
	size_t entriesProcessed = 0;
	for (size_t entryIdx = batch.currentEntryIndex; entryIdx < batch.entries.size() && entriesProcessed < numEntriesToWrite; ++entryIdx, ++entriesProcessed) {
		auto& entry = batch.entries[entryIdx];

		// validate entry
		if (!entry) {
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 ERROR: null entry at index %zu\n", this, entryIdx)
			throw std::runtime_error("null entry in writeEntriesV1");
		}
		if (entry->data == nullptr) {
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 ERROR: null entry data at index %zu\n", this, entryIdx)
			throw std::runtime_error("null entry data in writeEntriesV1");
		}

		bool isCurrentEntry = (entryIdx == batch.currentEntryIndex);
		uint32_t entryStartOffset = isCurrentEntry ? static_cast<uint32_t>(batch.currentEntryBytesWritten) : 0;
		uint32_t entryRemainingSize = static_cast<uint32_t>(entry->size) - entryStartOffset;

		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Processing entry %zu (size=%zu, startOffset=%u, remainingSize=%u, data=%p)\n",
			this, entryIdx, entry->size, entryStartOffset, entryRemainingSize, entry->data)

		// write transaction header if needed
		if (!isCurrentEntry || !batch.currentEntryHeaderWritten) {
			if (txnHeaderIdx >= numEntriesToWrite) {
				DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 ERROR: txnHeaderIdx overflow! index=%u, max=%u\n",
					this, txnHeaderIdx, numEntriesToWrite)
				throw std::runtime_error("txnHeader index overflow in writeEntriesV1");
			}
			char* txnHeader = txnHeaders.get() + (txnHeaderIdx * TXN_HEADER_SIZE);
			writeDoubleBE(txnHeader, batch.timestamp);
			writeUint32BE(txnHeader + 8, static_cast<uint32_t>(entry->size));
			txnHeaderIdx++;

			// ensure transaction header doesn't split across blocks
			// if header won't fit in current block, advance to next block
			uint32_t available = getBlockCapacity() - currentBlockOffset;
			if (available < TXN_HEADER_SIZE) {
				// not enough space for header, skip to next block
				// the new block is NOT a continuation since we're starting a fresh transaction header
				DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Transaction header won't fit (available=%u, needed=%u), advancing to next block\n",
					this, available, TXN_HEADER_SIZE)
				// Override initialDataOffset to 0 for this block since we're not continuing data
				uint32_t savedInitialDataOffset = initialDataOffset;
				initialDataOffset = 0;
				advanceToNextBlock(false);
				initialDataOffset = savedInitialDataOffset;  // restore for subsequent blocks if needed
			}

			// write entire header in one block
			addIovec(txnHeader, TXN_HEADER_SIZE);
			currentBlockOffset += TXN_HEADER_SIZE;
			totalBytesProcessed += TXN_HEADER_SIZE;

			if (isCurrentEntry) {
				batch.currentEntryHeaderWritten = true;
			}
		}

		// write entry data directly (zero-copy)
		uint32_t entryBytesWritten = 0;
		uint32_t remainingSpace = totalTxnSize - totalBytesProcessed;
		uint32_t bytesToWrite = std::min(entryRemainingSize, remainingSpace);

		while (bytesToWrite > 0) {
			if (currentBlockOffset >= getBlockCapacity()) {
				// advancing because we're out of space while writing data - this IS a continuation
				advanceToNextBlock(true);
			}

			uint32_t available = getBlockCapacity() - currentBlockOffset;
			uint32_t toWrite = std::min(bytesToWrite, available);

			// point directly to entry data
			addIovec(entry->data + entryStartOffset + entryBytesWritten, toWrite);

			entryBytesWritten += toWrite;
			currentBlockOffset += toWrite;
			totalBytesProcessed += toWrite;
			bytesToWrite -= toWrite;
		}

		// update batch state
		if (entryStartOffset + entryBytesWritten >= entry->size) {
			batch.currentEntryIndex = entryIdx + 1;
			batch.currentEntryBytesWritten = 0;
			batch.currentEntryHeaderWritten = false;
		} else {
			batch.currentEntryBytesWritten = entryStartOffset + entryBytesWritten;
		}
	}

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Built %zu iovecs for writing\n", this, iovecsIndex)

	// write to file using scatter-gather I/O
	int64_t bytesWritten = this->writeBatchToFile(iovecs.get(), static_cast<int>(iovecsIndex));
	if (bytesWritten < 0) {
		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 ERROR: Failed to write transaction log entries to file: %s\n", this, this->path.string().c_str())
		throw std::runtime_error("Failed to write transaction log entries to file: " + this->path.string());
	}

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Wrote %lld bytes to log file\n", this, bytesWritten)

	// update file metadata
	uint32_t newBlockSize;
	if (dataForCurrentBlock > 0 && numNewBlocks == 0) {
		newBlockSize = currentBlockSize + dataForCurrentBlock;
	} else if (numNewBlocks > 0) {
		newBlockSize = currentBlockOffset;
	} else {
		newBlockSize = currentBlockSize;
	}

	if (newBlockSize >= this->blockBodySize) {
		newBlockSize = 0;
	}

	this->currentBlockSize = newBlockSize;
	this->size += static_cast<uint32_t>(bytesWritten);

	uint32_t blocksAdded = numNewBlocks + ((dataForCurrentBlock > 0 && newBlockSize == 0) ? 1 : 0);
	this->blockCount += blocksAdded;

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Updated currentBlockSize=%u, added %u blocks, total blockCount=%u, batch state: entryIndex=%zu, bytesWritten=%zu\n",
		this, newBlockSize, blocksAdded, this->blockCount, batch.currentEntryIndex, batch.currentEntryBytesWritten)
}

int64_t TransactionLogFile::getAvailableSpaceInFile(
	const TransactionLogEntryBatch& batch,
	const uint32_t maxFileSize,
	const uint32_t availableSpaceInCurrentBlock
) {
	if (maxFileSize == 0) {
		// unlimited space, return -1 to indicate no limit
		return -1;
	}

	int64_t availableSpace = -1;

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

		if (availableSpaceInCurrentBlock == 0) {
			// need to create a new block
			// if header already written, just need block header + 1 byte data
			// otherwise need block header + txn header
			minimumRequired = BLOCK_HEADER_SIZE + (batch.currentEntryHeaderWritten ? 1 : TXN_HEADER_SIZE);
		} else if (!batch.currentEntryHeaderWritten) {
			// can append to current block
			// header not yet written, need space for it
			minimumRequired = TXN_HEADER_SIZE;
		}

		if (availableSpace < minimumRequired) {
			DEBUG_LOG("%p TransactionLogFile::getAvailableSpaceInFile Not enough space to write anything (need %u, have %lli), deferring to next file\n",
				this, minimumRequired, availableSpace)
			return 0;
		}
	}

	return availableSpace;
}

void TransactionLogFile::calculateEntriesToWrite(
	const TransactionLogEntryBatch& batch,
	const uint32_t availableSpaceInCurrentBlock,
	const int64_t availableSpaceInFile,
	uint32_t& totalTxnSize, // out
	uint32_t& numEntriesToWrite, // out
	uint32_t& dataForCurrentBlock, // out
	uint32_t& dataForNewBlocks, // out
	uint32_t& numNewBlocks // out
) {
	// start from the current entry in the batch (which could be a continuation)
	for (size_t i = batch.currentEntryIndex; i < batch.entries.size(); ++i) {
		auto& entry = batch.entries[i];

		// calculate entry size including header if not already written
		bool isCurrentEntry = (i == batch.currentEntryIndex);
		bool needsHeader = !isCurrentEntry || !batch.currentEntryHeaderWritten;
		uint32_t headerSize = needsHeader ? TXN_HEADER_SIZE : 0;
		uint32_t dataOffset = isCurrentEntry ? batch.currentEntryBytesWritten : 0;
		uint32_t entrySize = headerSize + (entry->size - dataOffset);
		uint32_t candidateSize = totalTxnSize + entrySize;

		// calculate block distribution for this candidate size
		BlockDistribution dist = this->calculateBlockDistribution(candidateSize, availableSpaceInCurrentBlock);

		// check if entry fits completely
		// when availableSpaceInFile == -1 (unlimited), always fits
		bool fitsCompletely = (availableSpaceInFile == -1) ||
		                      (availableSpaceInFile >= 0 && dist.bytesOnDisk <= static_cast<uint64_t>(availableSpaceInFile));

		if (fitsCompletely) {
			// entry fits completely
			totalTxnSize = candidateSize;
			dataForCurrentBlock = dist.dataForCurrentBlock;
			dataForNewBlocks = dist.dataForNewBlocks;
			numNewBlocks = dist.numNewBlocks;
			numEntriesToWrite++;
		} else {
			// entry would exceed limit - write partial entry if possible
			// at this point, availableSpaceInFile must be >= 0 (not -1)
			uint32_t overage = dist.bytesOnDisk - static_cast<uint32_t>(availableSpaceInFile);
			uint32_t adjustedSize = candidateSize - overage;

			// recalculate distribution with adjusted size
			BlockDistribution adjustedDist = this->calculateBlockDistribution(adjustedSize, availableSpaceInCurrentBlock);

			// only write partial entry if we have space for at least header (if needed) + some data
			// or if header is already written and we're continuing data
			bool canWritePartial = (isCurrentEntry && batch.currentEntryHeaderWritten) || adjustedSize >= headerSize;
			if (canWritePartial) {
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


} // namespace rocksdb_js
