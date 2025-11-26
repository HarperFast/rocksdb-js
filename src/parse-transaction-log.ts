import { closeSync, openSync, readSync, statSync, type Stats } from 'node:fs';
import { constants } from './load-binding.js';

const {
	TRANSACTION_LOG_TOKEN,
	TRANSACTION_LOG_FILE_HEADER_SIZE,
	TRANSACTION_LOG_ENTRY_HEADER_SIZE
} = constants;

interface LogEntry {
	data: Buffer;
	flags: number;
	length: number;
	timestamp?: number;
}

interface TransactionLog {
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
		// read the file header
		const token = read(4).readUInt32BE(0);
		if (token !== TRANSACTION_LOG_TOKEN) {
			throw new Error('Invalid token');
		}

		const version = read(1).readUInt8(0);
		if (version !== 1) {
			throw new Error(`Unsupported transaction log file version: ${version}`);
		}

		// read the entries
		const entries: LogEntry[] = [];

		while (fileOffset < size) {
			const timestamp = read(8).readDoubleBE(0);
			const length = read(4).readUInt32BE(0);
			const flags = read(1).readUInt8(0);
			const data = read(length);
			entries.push({ timestamp, length, flags, data });
		}

		return { size, version, entries };
	} catch (error) {
		if (error instanceof Error) {
			error.message = `Invalid transaction log file: ${error.message}`;
		}
		throw error;
	} finally {
		closeSync(fileHandle);
	}
}
