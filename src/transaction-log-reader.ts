import { type NativeDatabase, TransactionLog, type LogBuffer } from './load-binding';
import { readdirSync, statSync } from 'node:fs';

type TransactionEntry = {
	timestamp: number;
	data: Buffer;
}

const FLOAT_TO_UINT32 = new Float64Array(1);
const UINT32_FROM_FLOAT = new Uint32Array(FLOAT_TO_UINT32.buffer);

const BLOCK_SIZE_BITS = 12;
const BLOCK_SIZE = 2**BLOCK_SIZE_BITS; // 4kb
const MAX_LOG_FILE_SIZE = 2**24; // 16mb maximum size of a log file
export class TransactionLogReader {
	#log: TransactionLog;
	#lastPosition: Float64Array;
	#currentLogBuffer?: LogBuffer; // latest log buffer, this is cached for quick access and to pin it in memory (older buffers are weakly referenced)
	// cache of log buffers
	#logBuffers = new Map<number, WeakRef<LogBuffer>>();

	constructor(log: TransactionLog) {
		this.#log = log;
		const lastCommittedPosition = log.lastCommittedPosition || (log.lastCommittedPosition = log.getLastCommittedPosition());
		this.#lastPosition = new Float64Array(lastCommittedPosition.buffer);
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
	query({ start, end, exactStart, readUncommitted }: { start?: number, end?: number, exactStart?: boolean, readUncommitted?: boolean }): Iterable<TransactionEntry> {
		const transactionLogReader = this;
		let size = 0;
		start ??= 0;
		end ??= Number.MAX_VALUE;
		let latestLogId = loadLastPosition();
		let logBuffer: LogBuffer = this.#currentLogBuffer!; // try the current one first
		if (logBuffer?.logId !== latestLogId) {
			// if the current log buffer is not the one we want, load the memory map
			logBuffer = getLogMemoryMap(latestLogId)!;
			if (!readUncommitted) { // if we are reading uncommitted, we might be a log file ahead of the committed transaction
				this.#currentLogBuffer = logBuffer;
			}
		}
		if (logBuffer === undefined) {
			return [][Symbol.iterator]();
		}

		let dataView: DataView = logBuffer.dataView;
		let blockTimestamp = dataView.getFloat64(0);
		while (blockTimestamp > start) {
			// if we have an earlier timestamp than available in this log file, find an earlier log file
			let previousLogBuffer = getLogMemoryMap(logBuffer.logId - 1)!;
			if (!previousLogBuffer) {
				break;
			}
			logBuffer = previousLogBuffer;
			size = logBuffer.size;
			if (size === 0) {
				logBuffer.size = this.#log.getLogFileSize(logBuffer.logId);
			}
			dataView = logBuffer.dataView;
			blockTimestamp = dataView.getFloat64(0);
		}
		let position = 0;
		// Now do a binary search in the log buffer to find the first block that precedes the start timestamp.
		// We do a stable positioning binary search instead of a traditional iterative split binary search, which means
		// that instead of taking the array and splitting it in half (and continue to split each part in half), we choose
		// pivot points based on powers of two. This has a couple of significant advantages:
		// - As a log grows in size, rather than constantly choosing different pivot points based on length, we are heavily
		// reusing the same pivot points for each search (starting pivot point only changes when a log grows past the next 2^n, for example).
		// This significantly improves the probability of cache hits, whether that is for pages swapped out of memory
		// or even just L1/L2 cache usage.
		// - This binary search also "favors" newer entries. For most recent entries, 50% of pivot point comparisons can be
		// skipped because the next (pivot + 2^n) is outside the size bounds, so this provides a performance/acceleration bias
		// towards newer entries, which are expected to be searched much more frequently.
		for (let shift = 23; shift >= BLOCK_SIZE_BITS; shift--) {
			const pivotSize = 1 << shift;
			position += pivotSize;
			if (position < size) {
				blockTimestamp = dataView.getFloat64(position);
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
			const previousBlockTimestamp = dataView.getFloat64(previousPosition);
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
							// our position is beyond the size limit, get the updated
							// size in case we can keep reading further from the same block
							let latestLogId = loadLastPosition();
							let latestSize = size;
							if (latestLogId > logBuffer.logId) {
								// if it is not the latest log, get the file size
								size = logBuffer.size || (logBuffer.size = transactionLogReader.#log.getLogFileSize(logBuffer.logId));
								if (position >= size) {
									// we can't read any further in this block, go to the next block
									logBuffer = getLogMemoryMap(logBuffer.logId + 1)!;
									if (latestLogId > logBuffer.logId) {
										// it is non-current log file, we can safely use or cache the size
										size = logBuffer.size || (logBuffer.size = transactionLogReader.#log.getLogFileSize(logBuffer.logId));
									} else {
										size = latestSize; // use the latest position from loadLastPosition
									}
									position = 0;
								}
							}
						}
						while(position < size) {
							// advance to the next entry, reading the timestamp and the data
							timestamp = dataView.getFloat64(position);
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
								matchesRange = timestamp >= start! && timestamp < end!;
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
										if (position >= size) {
											// move to the next block
											logBuffer = getLogMemoryMap(logBuffer.logId + 1)!;
											let latestLogId = loadLastPosition();
											if (latestLogId > logBuffer.logId) {
												size = logBuffer.size || (logBuffer.size = transactionLogReader.#log.getLogFileSize(logBuffer.logId));
											}
											position = 0;
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
								const blockTimestamp = dataView.getFloat64(lastBlock << BLOCK_SIZE_BITS);
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
		function getLogMemoryMap(logId: number) {
			if (logId > 0) {
				let logBuffer = transactionLogReader.#logBuffers.get(logId)?.deref();
				if (logBuffer) { // if we have a cached buffer, return it
					return logBuffer;
				}
				logBuffer = transactionLogReader.#log.getMemoryMapOfFile(logId);
				if (!logBuffer) return;
				logBuffer.logId = logId;
				logBuffer.dataView = new DataView(logBuffer.buffer);
				transactionLogReader.#logBuffers.set(logId, new WeakRef(logBuffer)); // add to cache
				return logBuffer;
			} // else return undefined
		}
		function loadLastPosition() {
			// atomically copy the full 64-bit last committed position word to a local variable so we can read it without memory tearing
			FLOAT_TO_UINT32[0] = transactionLogReader.#lastPosition[0]
			let logId = UINT32_FROM_FLOAT[1];
			if (readUncommitted) {
				// if we are reading uncommitted transactions, we need to read the entire log file to find the latest position
				let nextSize = 0;
				let nextLogId = logId || 1;
				while(true) {
					nextSize = transactionLogReader.#log.getLogFileSize(nextLogId);
					if (nextSize === 0) { // if the size is zero, there is no next log file, we are done
						break;
					} else {
						size = nextSize;
						logId = nextLogId++;
					}
				}
			} else {
				// otherwise, just use the last committed position, which indicates the latest committed transaction in the log
				size = UINT32_FROM_FLOAT[0];
			}
			return logId;
		}
	}
}
