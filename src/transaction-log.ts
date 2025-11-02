import { closeSync, openSync, readSync, statSync, type Stats } from 'node:fs';

export const CONTINUATION_FLAG = 0x0001;
export const FILE_HEADER_SIZE = 10;
export const BLOCK_HEADER_SIZE = 14;
export const TRANSACTION_HEADER_SIZE = 12;

interface Block {
	readonly startTimestamp: number;
	readonly flags: number;
	readonly dataOffset: number;
}

interface LogEntry {
	readonly timestamp: number;
	readonly length: number;
	readonly data: Buffer;
}

interface TransactionLog {
	readonly size: number;
	readonly version: number;
	readonly blockSize: number;
	readonly blockCount: number;
	readonly blocks: Block[];
	readonly entries: LogEntry[];
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
		// read the file header
		const token = read(4).readUInt32BE(0);
		if (token !== 0x574F4F46) {
			throw new Error('Invalid token');
		}

		const version = read(2).readUInt16BE(0);
		if (version !== 1) {
			throw new Error('Unsupported transaction log file version');
		}

		const blockSize = read(4).readUInt32BE(0);
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
			const startTimestamp = Number(buffer.readBigUInt64BE(0));
			byteCount -= 8;

			if (byteCount < 2) {
				throw new Error(`Invalid block ${i}: expected at least 2 bytes for flags but only read ${byteCount}`);
			}
			const flags = buffer.readUInt16BE(8);
			byteCount -= 2;

			if (byteCount < 4) {
				throw new Error(`Invalid block ${i}: expected at least 4 bytes for data offset but only read ${byteCount}`);
			}
			const dataOffset = buffer.readUInt32BE(10);
			byteCount -= 4;

			blocks[i] = { startTimestamp, flags, dataOffset };

			if (byteCount > 0) {
				if (transactionDataLength + byteCount > transactionSize) {
					throw new Error(`Invalid block ${i}: expected at most ${transactionSize} bytes for transaction data but read ${transactionDataLength}`);
				}
				transactionData.set(buffer.subarray(BLOCK_HEADER_SIZE, BLOCK_HEADER_SIZE + byteCount), transactionDataLength);
				transactionDataLength += byteCount;
			}
		}

		for (let i = 0; transactionOffset < transactionDataLength; i++) {
			if (transactionOffset + 8 > transactionDataLength) {
				throw new Error(`Invalid transaction ${i}: expected at least 8 bytes for start timestamp but only read ${transactionDataLength - transactionOffset}`);
			}
			const timestamp = Number(transactionData.readBigUInt64BE(transactionOffset));
			transactionOffset += 8;

			if (transactionOffset + 4 > transactionDataLength) {
				throw new Error(`Invalid transaction ${i}: expected at least 4 bytes for length but only read ${transactionDataLength - transactionOffset}`);
			}
			const length = transactionData.readUInt32BE(transactionOffset);
			transactionOffset += 4;
			if (transactionOffset + length > transactionDataLength) {
				throw new Error(`Invalid transaction ${i}: expected at least ${length} bytes for data but only read ${transactionDataLength - transactionOffset}`);
			}

			entries.push({
				timestamp,
				length,
				data: transactionData.subarray(transactionOffset, transactionOffset + length)
			});

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
