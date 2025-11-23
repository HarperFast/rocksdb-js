import { closeSync, openSync, readSync, statSync, type Stats } from 'node:fs';
import { constants } from './load-binding.js';

const { BLOCK_HEADER_SIZE, CONTINUATION_FLAG, WOOF_TOKEN } = constants;

interface Block {
	dataOffset: number;
	flags: number;
	startTimestamp: number;
}

interface LogEntry {
	continuation?: boolean;
	data: Buffer;
	length: number;
	partial?: boolean;
	timestamp?: number;
}

interface TransactionLog {
	blockCount: number;
	blockSize: number;
	blocks: Block[];
	entries: LogEntry[];
	size: number;
	version: number;
}

/**
 * Loads an entire transaction log file into memory.
 * @param path - The path to the transaction log file.
 * @returns The transaction log.
 */
export function parseTransactionLog(path: string): TransactionLog {
	let stats: Stats;
	try {
		stats = statSync(path);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
			throw new Error('Transaction log file does not exist');
		}
		throw error;
	}

	const { size } = stats;
	if (size === 0) {
		throw new Error('Transaction log file is too small');
	}

	const fileHandle = openSync(path, 'r');
	let fileOffset = 0;

	const read = (numBytes: number) => {
		const buffer = Buffer.allocUnsafe(numBytes);
		const bytesRead = readSync(fileHandle, buffer, 0, numBytes, fileOffset);
		fileOffset += bytesRead;
		if (bytesRead !== numBytes) {
			throw new Error(`Expected to read ${numBytes} bytes but only read ${bytesRead}`);
		}
		return buffer;
	};

	try {
		/*
		// read the file header
		const token = read(4).readUInt32BE(0);
		if (token !== WOOF_TOKEN) {
			throw new Error('Invalid token');
		}

		const version = read(2).readUInt16BE(0);
		if (version !== 1) {
			throw new Error('Unsupported transaction log file version');
		}
*/
		const version = 1;
		const blockSize = 4096;//read(2).readUInt16BE(0);
		const blockCount = Math.ceil((size - fileOffset) / blockSize);
		const blocks: Block[] = Array.from({ length: blockCount });
		const entries: LogEntry[] = [];
		const transactionSize = size - (blockCount * BLOCK_HEADER_SIZE);
		const transactionData = Buffer.allocUnsafe(transactionSize);
		let transactionDataLength = 0;
		let transactionOffset = 0;

		// read all of the blocks and fill the transaction data buffer
		for (let i = 0; fileOffset < size; i++) {
			const buffer = Buffer.allocUnsafe(blockSize);
			let byteCount = readSync(fileHandle, buffer, 0, blockSize, fileOffset);
			fileOffset += byteCount;

			if (byteCount < 8) {
				throw new Error(`Invalid block ${i}: expected at least 8 bytes for start timestamp but only read ${byteCount}`);
			}
			const startTimestamp = buffer.readDoubleBE(0);
			byteCount -= 8;

			if (byteCount < 2) {
				throw new Error(`Invalid block ${i}: expected at least 2 bytes for flags but only read ${byteCount}`);
			}
			const flags = buffer.readUInt16BE(8);
			byteCount -= 2;

			if (byteCount < 2) {
				throw new Error(`Invalid block ${i}: expected at least 2 bytes for data offset but only read ${byteCount}`);
			}
			const dataOffset = buffer.readUInt16BE(10);
			byteCount -= 2;

			blocks[i] = { startTimestamp, flags, dataOffset };

			if (byteCount > 0) {
				if (transactionDataLength + byteCount > transactionSize) {
					throw new Error(`Invalid block ${i}: expected at most ${transactionSize} bytes for transaction data but read ${transactionDataLength}`);
				}
				// If `dataOffset` is set and this is NOT a continuation block (first block with CONTINUATION_FLAG),
				// then it indicates padding - use it to exclude padding from the data
				// For continuation blocks, dataOffset indicates where continuation ends, but we copy all data
				let actualDataBytes = byteCount;
				if (dataOffset > 0 && !(i === 0 && (flags & CONTINUATION_FLAG))) {
					// This block has padding at the end, use dataOffset to exclude it
					actualDataBytes = dataOffset;
				}
				actualDataBytes = Math.min(actualDataBytes, byteCount);

				const data = buffer.subarray(BLOCK_HEADER_SIZE, BLOCK_HEADER_SIZE + actualDataBytes);
				transactionData.set(data, transactionDataLength);
				transactionDataLength += actualDataBytes;
			}
		}

		const length = Math.min(blocks[0]?.dataOffset, transactionDataLength);
		if ((blocks[0].flags & CONTINUATION_FLAG) && length > 0) {
			// The first block is a continuation from the previous file
			entries.push({
				data: transactionData.subarray(0, length),
				continuation: true,
				length
			});
			transactionOffset += blocks[0].dataOffset;
		}

		for (let i = 0; transactionOffset < transactionDataLength; i++) {
			if (transactionOffset + 8 > transactionDataLength) {
				throw new Error(`Invalid transaction ${i}: expected at least 8 bytes for start timestamp but only read ${transactionDataLength - transactionOffset}`);
			}
			const timestamp = transactionData.readDoubleBE(transactionOffset);
			transactionOffset += 8;

			if (transactionOffset + 4 > transactionDataLength) {
				throw new Error(`Invalid transaction ${i}: expected at least 4 bytes for length but only read ${transactionDataLength - transactionOffset}`);
			}
			const length = transactionData.readUInt32BE(transactionOffset);
			transactionOffset += 4;

			const end = Math.min(transactionOffset + length, transactionDataLength);
			const data = transactionData.subarray(transactionOffset, end);
			const entry: LogEntry = {
				data,
				length: end - transactionOffset,
				timestamp
			};

			if (transactionOffset + length > transactionDataLength) {
				// if we're at the end of the file, then we don't have all of
				// the data and we mark the entry as partial
				if (fileOffset === size) {
					entry.partial = true;
				} else {
					throw new Error(`Invalid transaction ${i}: expected at least ${length} bytes for data but only read ${transactionDataLength - transactionOffset}`);
				}
			}

			entries.push(entry);

			transactionOffset += length;
		}

		return {
			size,
			version,
			blockSize,
			blockCount,
			blocks,
			entries
		};
	} catch (error) {
		if (error instanceof Error) {
			error.message = `Invalid transaction log file: ${error.message}`;
		}
		throw error;
	} finally {
		closeSync(fileHandle);
	}
}
