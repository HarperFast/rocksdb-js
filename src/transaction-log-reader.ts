import { type NativeDatabase, TransactionLog, type LogBuffer, constants } from './load-binding';
import { readdirSync, statSync } from 'node:fs';

type TransactionEntry = {
	timestamp: number;
	data: Buffer;
}

const FLOAT_TO_UINT32 = new Float64Array(1);
const UINT32_FROM_FLOAT = new Uint32Array(FLOAT_TO_UINT32.buffer);

const { TRANSACTION_LOG_TOKEN, TRANSACTION_LOG_FILE_HEADER_SIZE, TRANSACTION_LOG_ENTRY_HEADER_SIZE } = constants;
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
		FLOAT_TO_UINT32[0] = transactionLogReader.#log.findPosition(start);
		let logId = UINT32_FROM_FLOAT[1];
		let position = UINT32_FROM_FLOAT[0];
		let logBuffer: LogBuffer = this.#currentLogBuffer!; // try the current one first
		if (logBuffer?.logId !== logId) {
			// if the current log buffer is not the one we want, load the memory map
			logBuffer = getLogMemoryMap(logId)!;
			if (latestLogId === logId && !readUncommitted) { // if we are reading uncommitted, we might be a log file ahead of the committed transaction
				this.#currentLogBuffer = logBuffer;
			}
		}
		if (logBuffer === undefined) {
			return [][Symbol.iterator]();
		}

		let dataView: DataView = logBuffer.dataView;
		if (latestLogId !== logId) {
			size = logBuffer.size;
			if (!size) {
				logBuffer.size = this.#log.getLogFileSize(logId);
			}
		}
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
							do {
								timestamp = dataView.getFloat64(position);
								// skip past any leading zeros (which leads to a tiny float that is < 1e-303)
							} while (timestamp < 1 && ++position < size);
							if (!timestamp) {
								// we have gone beyond the last transaction and reached the end
								return { done: true, value: undefined };
							}

							const length = dataView.getUint32(position + 8);
							position += 13;
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
							let entryStart = position;
							position += length;
							if (matchesRange) {
								// fits in the same block, just subarray the data out
								return {
									done: false,
									value: {
										timestamp,
										data: logBuffer!.subarray(entryStart, position)
									}
								};
							}
							if (position >= size) {
								// move to the next log file
								logBuffer = getLogMemoryMap(logBuffer.logId + 1)!;
								let latestLogId = loadLastPosition();
								if (latestLogId > logBuffer.logId) {
									size = logBuffer.size || (logBuffer.size = transactionLogReader.#log.getLogFileSize(logBuffer.logId));
								}
								position = 0;
							}
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
				let maxMisses = 3;
				for (let [ logId, reference ] of transactionLogReader.#logBuffers) {
					// clear out any references that have been collected
					if (reference.deref() === undefined) {
						transactionLogReader.#logBuffers.delete(logId);
					} else if (--maxMisses === 0) {
						break;
					}
				}
				return logBuffer;
			} // else return undefined
		}
		function loadLastPosition() {
			// atomically copy the full 64-bit last committed position word to a local variable so we can read it without memory tearing
			FLOAT_TO_UINT32[0] = transactionLogReader.#lastPosition[0];
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
