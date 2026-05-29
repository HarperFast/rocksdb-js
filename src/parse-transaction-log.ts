import { constants } from './load-binding.js';
import { closeSync, openSync, readSync, type Stats, statSync } from 'node:fs';

const { TRANSACTION_LOG_TOKEN } = constants;

// Transaction log files did not exist before this date, so any timestamp that
// predates it indicates corruption.
const MIN_VALID_TIMESTAMP = Date.UTC(2026, 0, 27); // 2026-01-27

// Currently only bit 0 (TRANSACTION_LOG_ENTRY_LAST_FLAG) is defined.
const VALID_FLAGS_MASK = 0x01;

interface LogEntry {
	anomalies?: string[];
	data?: Buffer;
	flags: number;
	length: number;
	timestamp?: number;
}

interface TransactionLog {
	anomalies: string[];
	entries: LogEntry[];
	entryAnomalyCount: number;
	timestamp: number;
	size: number;
	version: number;
}

/**
 * Loads an entire transaction log file into memory.
 * @param path - The path to the transaction log file.
 * @returns The transaction log.
 */
export function parseTransactionLog(
	path: string,
	options: { skipData?: boolean } = {}
): TransactionLog {
	let stats: Stats;
	try {
		stats = statSync(path);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
			throw new Error('Transaction log file does not exist');
		}
		throw error;
	}

	let { size } = stats;
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
			throw new Error(
				`Expected to read ${numBytes} bytes but only read ${bytesRead}, file offset: ${fileOffset}, file size: ${size}, file path: ${path}, buffer: ${buffer.toString(
					'hex'
				)}`
			);
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

		const timestamp = read(8).readDoubleBE(0);
		const anomalies: string[] = [];
		if (!Number.isFinite(timestamp) || timestamp < MIN_VALID_TIMESTAMP) {
			anomalies.push(`Header timestamp ${timestamp} predates 2026-01-27 (possible corruption)`);
		}

		// read the entries
		const entries: LogEntry[] = [];
		let entryAnomalyCount = 0;

		while (fileOffset < size) {
			const timestamp = read(8).readDoubleBE(0);
			if (timestamp === 0) {
				// if we encounter zero padding, we can stop reading the entries since the next entry will start at the next 8-byte boundary, which is the same as the current file offset.
				size = fileOffset - 8;
				break;
			}
			const length = read(4).readUInt32BE(0);
			const flags = read(1).readUInt8(0);
			const data = read(length);

			const entryAnomalies: string[] = [];
			if (!Number.isFinite(timestamp) || timestamp < MIN_VALID_TIMESTAMP) {
				entryAnomalies.push(`timestamp ${timestamp} predates 2026-01-27 (possible corruption)`);
			}
			if ((flags & ~VALID_FLAGS_MASK) !== 0) {
				entryAnomalies.push(
					`flags 0x${flags.toString(16).padStart(2, '0')} contains undefined bits (expected 0x00 or 0x01)`
				);
			}
			const entry: LogEntry = {
				timestamp,
				length,
				flags,
				data: options.skipData ? undefined : Buffer.from(data),
			};
			if (entryAnomalies.length > 0) {
				entry.anomalies = entryAnomalies;
				entryAnomalyCount += entryAnomalies.length;
			}
			entries.push(entry);
		}

		return { anomalies, entries, entryAnomalyCount, timestamp, size, version };
	} catch (error) {
		if (error instanceof Error) {
			error.message = `Invalid transaction log file: ${error.message}`;
		}
		throw error;
	} finally {
		closeSync(fileHandle);
	}
}
