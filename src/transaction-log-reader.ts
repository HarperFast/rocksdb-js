import { type NativeDatabase, TransactionLog, type LogBuffer } from './load-binding';
import { readdirSync, statSync } from 'node:fs';

type TransactionEntry = {
	timestamp: number;
	data: Buffer;
}

const BLOCK_SIZE_BITS = 12;
const BLOCK_SIZE = 2**BLOCK_SIZE_BITS; // 4kb
const MAX_LOG_FILE_SIZE = 2**24; // 16mb maximum size of a log file
export class TransactionLogReader {
	#log: TransactionLog;
	#currentLogBuffer?: LogBuffer; // current log buffer that we are reading from
	// cache of log buffers
	#logBuffers = new Map<number, LogBuffer>();

	constructor(log: TransactionLog) {
		this.#log = log;
	}

	/**
	 * Returns an iterator for transaction entries within the specified range of timestamps
	 * @param start
	 * @param end
	 */
	query(start: number, end: number): Iterator<TransactionEntry> {
		//
		const transactionLog = this;
		let logBuffer: LogBuffer | undefined = this.#currentLogBuffer ?? getNextLogFile();
		if (!logBuffer) {
			return [][Symbol.iterator]();
		}

		let dataView: DataView = logBuffer.dataView;
		let firstTimestamp = dataView.getFloat64(0);
		while (firstTimestamp > start) {
			// if we have an earlier timestamp than available in this log file, find an earlier log file
			const logId = logBuffer.logId - 1;
			logBuffer = this.#logBuffers.get(logId);
			if (!logBuffer) {
				logBuffer = getLogFile(logId);
				if (!logBuffer) {
					return [][Symbol.iterator]();
				}
			}
			dataView = logBuffer.dataView;
			firstTimestamp = dataView.getFloat64(0);
		}
		// Now do a binary search in the log buffer to find the first block that contains the start timestamp
		const size: number = 0;// = statSync(this.#currentLogFile);
		let position = 0;
		let low = 0;
		let high = size >>> BLOCK_SIZE_BITS;
		while (low < high) {
			const mid = (low + high) >>> 1;
			position = mid << BLOCK_SIZE_BITS;
			const timestamp = dataView.getFloat64(position);
			if (timestamp < start) {
				low = mid + 1;
			} else {
				high = mid;
			}
		}
		do {
			position += 12; // skip the timestamp of 4 bytes descriptor
			const timestamp = dataView.getFloat64(position);
			if (timestamp >= start) break;
		} while (true);
		return {
			next() {
				let timestamp: number;
				do {
					// advance to the next entry, reading the timestamp and the data
					timestamp = dataView.getFloat64(position);
					if (timestamp > end) {
						return { done: true, value: undefined };
					}
				} while (timestamp < start || timestamp > end);

				const length = dataView.getUint32(position + 8);
				position += 12;
				let entryEnd = position + length;
				let data: Buffer;
				if (entryEnd >>> BLOCK_SIZE_BITS === (position >>> BLOCK_SIZE_BITS)) {
					// fits in the same block, just subarray the data out
					data = logBuffer.subarray(position, entryEnd);
					position = entryEnd;
				} else {
					// the entry data is split into multiple blocks, need to collect and concatenate it
					let parts: Buffer[] = [];
					do {
						if (entryEnd >>> BLOCK_SIZE_BITS === (position >>> BLOCK_SIZE_BITS)) {
							parts.push(logBuffer.subarray(position, entryEnd));
							position = entryEnd;
						} else {
							const nextBlock = ((position >>> BLOCK_SIZE_BITS) + 1) << BLOCK_SIZE_BITS;
							parts.push(logBuffer.subarray(position, nextBlock - 8));
							position = nextBlock + 12;
						}
						if (position >= MAX_LOG_FILE_SIZE) {
							getNextLogFile();
						}
					} while (position < entryEnd);
					data = Buffer.concat(parts, length);
				}
				return {
					done: false,
					value: {
						timestamp,
						data
					}
				}
			}
		};
		function getNextLogFile() {
			let logId = 0;
			if (transactionLog.#currentLogBuffer) {
				logId = transactionLog.#currentLogBuffer.logId + 1;
			} else {
				for (let entry of readdirSync('transaction-log-path')) {
					const logIndex = +entry.split('.')[0];
					if (logIndex > logId) {
						logId = logIndex;
					}
				}
			}
			const logBuffer = getLogFile(logId);
			if (logBuffer) {
				transactionLog.#currentLogBuffer = logBuffer;
			}
			return logBuffer;
		}
		function getLogFile(logId: number) {
			const logBuffer = transactionLog.#log.getMemoryMapOfFile(logId);
			if (!logBuffer) return;
			logBuffer.logId = logId;
			logBuffer.dataView = new DataView(logBuffer.buffer);
			transactionLog.#logBuffers.set(logId, logBuffer); // add to cache
			return logBuffer;
		}
	}
}
