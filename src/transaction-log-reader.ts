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
	#lastPosition: Uint32Array;
	#currentLogBuffer?: LogBuffer; // current log buffer that we are reading from
	// cache of log buffers
	#logBuffers = new Map<number, LogBuffer>();

	constructor(log: TransactionLog) {
		this.#log = log;
		const lastCommittedPosition = log.lastCommittedPosition || (log.lastCommittedPosition = log.getLastCommittedPosition());
		this.#lastPosition = new Uint32Array(lastCommittedPosition.buffer);

	}

	/**
	 * Returns an iterable for transaction entries within the specified range of timestamps
	 * This iterable can be iterated over multiple times, and subsequent iterations will continue
	 * from where the last iteration left off, allowing for iteration through the log file
	 * to resume after more transactions have been committed.
	 * @param start
	 * @param end
	 * @param exactStart - if this is true, the function will try to find the transaction that
	 * exactly matches the start timestamp, and then return all subsequent transactions in the log
	 * regardless of whether their timestamp is before or after the start
	 */
	query(start: number, end: number, exactStart?: boolean): Iterable<TransactionEntry> {
		const transactionLog = this;
		let logBuffer: LogBuffer | undefined = this.#currentLogBuffer ?? getNextLogFile();
		if (logBuffer === undefined) {
			return [][Symbol.iterator]();
		}
		let size = this.#lastPosition[0];

		let dataView: DataView = logBuffer.dataView;
		//let blockTimestamp = dataView.getFloat64(0);
		let blockTimestamp = Number(dataView.getBigInt64(0));
		while (blockTimestamp > start) {
			// if we have an earlier timestamp than available in this log file, find an earlier log file
			const logId = logBuffer.logId - 1;
			let previousLogBuffer = this.#logBuffers.get(logId);
			if (!previousLogBuffer) {
				const previousLogBuffer = getLogMemoryMap(logId);
				if (previousLogBuffer) logBuffer = previousLogBuffer;
				else break;
			}
			size = logBuffer.size;
			if (size === 0) {
				logBuffer.size = this.#log.getLogFileSize(logId);
			}
			dataView = logBuffer.dataView;
			//blockTimestamp = dataView.getFloat64(0);
			blockTimestamp = Number(dataView.getBigInt64(0));
		}
		// Now do a binary search in the log buffer to find the first block that precedes the start timestamp
		let position = 0;
		// we do a stable positioning binary search, for better cache locality
		for (let shift = 23; shift >= BLOCK_SIZE_BITS; shift--) {
			const pivotSize = 1 << shift;
			position += pivotSize;
			if (position < size) {
				blockTimestamp = Number(dataView.getBigInt64(position));
				if (blockTimestamp < start) {
					// take the upper block
					continue;
				}
			}
			// otherwise, take the lower block
			position -= pivotSize;
		}
		// if there are multiple blocks with identifcal timestamps, they can have overlapping transactions that may include the start timestamp
		while(position > 0) { // don't try to iterate past the beginning
			let previousPosition = position - BLOCK_SIZE;
			// TODO: we can end up needing to iterate back into a previous log file
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
						if (position >= size) {
							// our position is beyond the size limit, lets get the updated
							// size in case we can keep reading further
							if (logBuffer!.size > 0) {
								// if the log buffer is done and a known size, update our size
								size = logBuffer!.size;
							} else {
								// get the very latest size and position
								size = transactionLog.#lastPosition[0];
							}
						}
						while(position < size) {
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
									data = logBuffer!.subarray(position, entryEnd);
									position = entryEnd;
								} else {
									// the entry data is split into multiple blocks, need to collect and concatenate it
									let blockIndex = firstBlock + 1;
									let parts: Buffer[] = [logBuffer.subarray(position, blockIndex << BLOCK_SIZE_BITS)];
									do {
										const start = (blockIndex << BLOCK_SIZE_BITS) + 14;
										blockIndex++;
										position = Math.min(blockIndex << BLOCK_SIZE_BITS, entryEnd);
										parts.push(logBuffer.subarray(start, position));
										if (position >= MAX_LOG_FILE_SIZE) {
											getNextLogFile();
										}
									} while (position < entryEnd);
									data = Buffer.concat(parts, length);
								}
								if (++lastBlock << BLOCK_SIZE_BITS < position + 12) {
									position = (lastBlock << BLOCK_SIZE_BITS) + 14;
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
						return { done: true, value: undefined };
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
				logId = transactionLog.#lastPosition[1];
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
