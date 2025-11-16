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
	#lastPositionDV: DataView;
	#currentLogBuffer?: LogBuffer; // current log buffer that we are reading from
	// cache of log buffers
	#logBuffers = new Map<number, LogBuffer>();

	constructor(log: TransactionLog) {
		this.#log = log;
		const lastCommittedPosition = log.lastCommittedPosition || (log.lastCommittedPosition = log.getLastCommittedPosition());
		this.#lastPositionDV = new DataView(lastCommittedPosition.buffer);

	}

	/**
	 * Returns an iterator for transaction entries within the specified range of timestamps
	 * @param start
	 * @param end
	 * @param exactStart - if this is true, the function will try to find the transaction that
	 * exactly matches the start timestamp, and then return all subsequent transactions in the log
	 * regardless of whether their timestamp is before or after the start
	 */
	query(start: number, end: number, exactStart?: boolean): Iterable<TransactionEntry> {
		//
		const transactionLog = this;
		let logBuffer: LogBuffer | undefined = this.#currentLogBuffer ?? getNextLogFile();
		if (!logBuffer) {
			return [][Symbol.iterator]();
		}

		let dataView: DataView = logBuffer.dataView;
		//let blockTimestamp = dataView.getFloat64(0);
		let blockTimestamp = Number(dataView.getBigInt64(10));
		while (blockTimestamp > start) {
			// if we have an earlier timestamp than available in this log file, find an earlier log file
			const logId = logBuffer.logId - 1;
			let previousLogBuffer = this.#logBuffers.get(logId);
			if (!previousLogBuffer) {
				const previousLogBuffer = getLogMemoryMap(logId);
				if (previousLogBuffer) logBuffer = previousLogBuffer;
				else break;
			}
			dataView = logBuffer.dataView;
			//blockTimestamp = dataView.getFloat64(0);
			blockTimestamp = Number(dataView.getBigInt64(10));
		}
		// Now do a binary search in the log buffer to find the first block that precedes the start timestamp
		const size: number = logBuffer.size;
		let position = 10;
		let low = 0;
		let high = (size >>> BLOCK_SIZE_BITS) - 1;
		while (low < high) {
			const mid = (low + high) >>> 1;
			position = mid << BLOCK_SIZE_BITS;
			//const timestamp = dataView.getFloat64(position);
			blockTimestamp = Number(dataView.getBigInt64(position));
			if (blockTimestamp < start) {
				low = mid + 1;
			} else {
				high = mid;
			}
		}
		// if there are multiple blocks with identifcal timestamps, they can have overlapping transactions that may include the start timestamp
		while(position > 10) { // don't try to iterate past the beginning
			let previousPosition = position - BLOCK_SIZE;
			const previousBlockTimestamp = Number(dataView.getBigInt64(previousPosition));
			if (previousBlockTimestamp !== blockTimestamp) break;
			position = previousPosition;
		}

		position += 14 + dataView.getUint16(position + 12); // skip past the block descriptor and the offset to the first transaction
		return {
			[Symbol.iterator](): Iterator<TransactionEntry> {
				return {
					next() {
						let timestamp: number;
						while(position < logBuffer.size) {
							// advance to the next entry, reading the timestamp and the data
							//timestamp = dataView.getFloat64(position);
							timestamp = Number(dataView.getBigInt64(position));
							if (!timestamp) {
								// we have gone beyond the last transaction and reached the end
								return { done: true, value: undefined };
							}

							const length = dataView.getUint32(position + 8);
							position += 12;
							let matchesRange = false;
							if (exactStart) {
								// in exact start mode, we are look for the exact identifying timestamp of the first transaction
								if (timestamp === start) {
									matchesRange = true;
									// after finding this transaction, match all remaining
									start = 0;
									exactStart = false;
								}
							} else {
								matchesRange = timestamp >= start && timestamp < end;
							}
							let entryEnd = position + length;
							let firstBlock = position >>> BLOCK_SIZE_BITS;
							let lastBlock = entryEnd >>> BLOCK_SIZE_BITS;
							let spanBlockHeaders = lastBlock - firstBlock;
							let lastSpanBlockHeaders = 0;
							while(spanBlockHeaders > lastSpanBlockHeaders) {
								lastSpanBlockHeaders = spanBlockHeaders;
								// recompute the end of the entry based on the number of block headers we have to traverse
								entryEnd = position + length + spanBlockHeaders * 14;
								lastBlock = entryEnd >>> BLOCK_SIZE_BITS;
								spanBlockHeaders = lastBlock - firstBlock;
							}
							if (matchesRange) {
								let data: Buffer;
								if (lastBlock === firstBlock) {
									// fits in the same block, just subarray the data out
									data = logBuffer.subarray(position, entryEnd);
									position = entryEnd;
								} else {
									// the entry data is split into multiple blocks, need to collect and concatenate it
									let parts: Buffer[] = [];
									do {
										if (lastBlock === firstBlock) {
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
							} else if (lastBlock !== firstBlock) {
								const blockTimestamp = Number(dataView.getBigInt64(lastBlock << BLOCK_SIZE_BITS));
								if (blockTimestamp >= end) {
									return { done: true, value: undefined };
								}
							}
							position = entryEnd;
						}
						return { done: true, value: undefined }
					}
				};
			}
		};
		function getNextLogFile() {
			let logId = 0;
			let sizeOfNext = 0;
			if (transactionLog.#currentLogBuffer) {
				logId = transactionLog.#currentLogBuffer.logId + 1;
			} else {
				/*for (let [sequenceNumber, size] of transactionLog.#log.getSequencedLogs()) {
					if (sequenceNumber > logId) {
						logId = sequenceNumber;
						sizeOfNext = size;
					}
				}*/
			}
			if (logId === 0) return;
			const logBuffer = getLogMemoryMap(logId);
			if (logBuffer) {
				transactionLog.#currentLogBuffer = logBuffer;
				logBuffer.size = sizeOfNext;
			}
			return logBuffer;
		}
		function getLogMemoryMap(logId: number) {
			if (logId > 0) {
				const logBuffer = transactionLog.#log.getMemoryMapOfFile(logId);
				if (!logBuffer) return;
				logBuffer.logId = logId;
				logBuffer.dataView = new DataView(logBuffer.buffer);
				transactionLog.#logBuffers.set(logId, logBuffer); // add to cache
				return logBuffer;
			} // else return undefined
		}
	}
}
