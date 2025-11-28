import {
	type NativeDatabase,
	TransactionLog,
	type LogBuffer,
	constants,
	TransactionLogQueryOptions,
	TransactionEntry,
} from './load-binding';


const FLOAT_TO_UINT32 = new Float64Array(1);
const UINT32_FROM_FLOAT = new Uint32Array(FLOAT_TO_UINT32.buffer);

const { TRANSACTION_LOG_TOKEN, TRANSACTION_LOG_FILE_HEADER_SIZE, TRANSACTION_LOG_ENTRY_HEADER_SIZE } = constants;

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
Object.defineProperty(TransactionLog.prototype, 'query', {
	enumerable: false,
	value: function({ start, end, exactStart, readUncommitted }: TransactionLogQueryOptions): Iterable<TransactionEntry> {
		const transactionLog = this;
		if (!this._lastCommittedPosition) {
			const lastCommittedPosition = this._getLastCommittedPosition();
			this._lastCommittedPosition = new Float64Array(lastCommittedPosition.buffer);
			this._logBuffers = new Map<number, WeakRef<LogBuffer>>();
		}
		let size = 0;
		end ??= Number.MAX_VALUE;
		let latestLogId = loadLastPosition();
		let logId = latestLogId;
		let position = 0;
		if (start === undefined) {
			// if no start timestamp is specified, start from the last committed position
			position = size;
			start = 0;
		} else {
			// otherwise, find the log file that contains the start timestamp, and find the position within that file
			FLOAT_TO_UINT32[0] = transactionLog._findPosition(start);
			logId = UINT32_FROM_FLOAT[1];
			position = UINT32_FROM_FLOAT[0];
		}
		let logBuffer: LogBuffer = this._currentLogBuffer!; // try the current one first
		if (logBuffer?.logId !== logId) {
			// if the current log buffer is not the one we want, load the memory map
			logBuffer = getLogMemoryMap(logId)!;
			if (latestLogId === logId && !readUncommitted) { // if we are reading uncommitted, we might be a log file ahead of the committed transaction
				this._currentLogBuffer = logBuffer;
			}
		}
		if (logBuffer === undefined) {
			return [][Symbol.iterator]();
		}

		let dataView: DataView = logBuffer.dataView;
		if (latestLogId !== logId) {
			size = logBuffer.size;
			if (!size) {
				size = logBuffer.size = this._getLogFileSize(logId);
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
								size = logBuffer.size || (logBuffer.size = transactionLog._getLogFileSize(logBuffer.logId));
								if (position >= size) {
									// we can't read any further in this block, go to the next block
									logBuffer = getLogMemoryMap(logBuffer.logId + 1)!;
									if (latestLogId > logBuffer.logId) {
										// it is non-current log file, we can safely use or cache the size
										size = logBuffer.size || (logBuffer.size = transactionLog._getLogFileSize(logBuffer.logId));
									} else {
										size = latestSize; // use the latest position from loadLastPosition
									}
									position = TRANSACTION_LOG_FILE_HEADER_SIZE;
								}
							}
						}
						while(position < size) {
							console.log('iterating from ', position, ' to ', size);
							// advance to the next entry, reading the timestamp and the data
							do {
								timestamp = dataView.getFloat64(position);
								// skip past any leading zeros (which leads to a tiny float that is < 1e-303)
							} while (timestamp < 1 && ++position < size);
							if (!timestamp) {
								console.log('no timestamp, reached end');
								// we have gone beyond the last transaction and reached the end
								return { done: true, value: undefined };
							}

							const length = dataView.getUint32(position + 8);
							position += TRANSACTION_LOG_ENTRY_HEADER_SIZE;
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
							console.log('matches', start, end, timestamp, matchesRange);
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
								let latestLogId = loadLastPosition();
								if (latestLogId > logBuffer.logId) {
									logBuffer = getLogMemoryMap(logBuffer.logId + 1)!;
									size = logBuffer.size;
									if (!size) {
										size = transactionLog._getLogFileSize(logBuffer.logId);
										if (!readUncommitted) {
											logBuffer.size = size;
										}
									}
									position = TRANSACTION_LOG_FILE_HEADER_SIZE;
								}
							}
						}
						return { done: true, value: undefined };
					}
				};
			}
		};
		function getLogMemoryMap(logId: number) {
			if (logId > 0) {
				let logBuffer = transactionLog._logBuffers.get(logId)?.deref();
				if (logBuffer) { // if we have a cached buffer, return it
					return logBuffer;
				}
				logBuffer = transactionLog._getMemoryMapOfFile(logId);
				if (!logBuffer) return;
				logBuffer.logId = logId;
				logBuffer.dataView = new DataView(logBuffer.buffer);
				transactionLog._logBuffers.set(logId, new WeakRef(logBuffer)); // add to cache
				let maxMisses = 3;
				for (let [ logId, reference ] of transactionLog._logBuffers) {
					// clear out any references that have been collected
					if (reference.deref() === undefined) {
						transactionLog._logBuffers.delete(logId);
					} else if (--maxMisses === 0) {
						break;
					}
				}
				return logBuffer;
			} // else return undefined
		}
		function loadLastPosition() {
			// atomically copy the full 64-bit last committed position word to a local variable so we can read it without memory tearing
			FLOAT_TO_UINT32[0] = transactionLog._lastCommittedPosition[0];
			let logId = UINT32_FROM_FLOAT[1];
			if (readUncommitted) {
				// if we are reading uncommitted transactions, we need to read the entire log file to find the latest position
				let nextSize = 0;
				let nextLogId = logId || 1;
				while(true) {
					nextSize = transactionLog._getLogFileSize(nextLogId);
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
});
