import {
	TransactionLog,
	type LogBuffer,
	constants,
	TransactionLogQueryOptions,
	TransactionEntry,
} from './load-binding';

const FLOAT_TO_UINT32 = new Float64Array(1);
const UINT32_FROM_FLOAT = new Uint32Array(FLOAT_TO_UINT32.buffer);

const { TRANSACTION_LOG_FILE_HEADER_SIZE, TRANSACTION_LOG_ENTRY_HEADER_SIZE } = constants;

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
	value: function({ start, end, exactStart, readUncommitted, exclusiveStart }: TransactionLogQueryOptions = {}): IterableIterator<TransactionEntry> {
		const transactionLog = this;
		if (!this._lastCommittedPosition) {
			// if this is the first time we are querying the log, initialize the last committed position and memory map cache
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
			if (readUncommitted) {
				// if no start timestamp is specified and readUncommitted, go to last flushed position

			} else {
				// otherwise if no start timestamp is specified, start from the last committed position
				position = size;
				start = 0;
			}
		} else {
			// otherwise, find the log file that contains the start timestamp, and find the position within that file
			FLOAT_TO_UINT32[0] = this._findPosition(start);
			// extract the log file ID from the 64-bit float returned by _findPosition, which is stored in the high 32 bits of the float
			logId = UINT32_FROM_FLOAT[1];
			// and position from the low 32 bits of the float
			position = UINT32_FROM_FLOAT[0];
		}
		let dataView: DataView;
		let logBuffer: LogBuffer = this._currentLogBuffer!; // try the current one first
		if (logBuffer?.logId !== logId) {
			// if the current log buffer is not the one we want, load the memory map
			logBuffer = getLogMemoryMap(logId)!;
			// if this is the latest, cache for easy access, unless...
			// if we are reading uncommitted, we might be a log file ahead of the committed transaction
			// also, it is pointless to cache the latest log file in a memory map on Windows, because it is not growable
			if (latestLogId === logId && !readUncommitted) {
				this._currentLogBuffer = logBuffer;
			}
			if (logBuffer === undefined) {
				// create a fake log buffer if we don't have any log buffer yet
				logBuffer = Buffer.alloc(0) as unknown as LogBuffer;
				logBuffer.logId = 0;
				logBuffer.size = 0;
			}
		}

		dataView = logBuffer.dataView;
		if (latestLogId !== logId) {
			size = logBuffer.size;
			if (size == undefined) {
				size = logBuffer.size = this.getLogFileSize(logId);
			}
		}
		let foundExactStart = false;
		return {
			[Symbol.iterator](): IterableIterator<TransactionEntry> { return this; },
			next() {
				let timestamp: number;
				if (position >= size) {
					// our position is beyond the size limit, get the updated
					// size in case we can keep reading further from the same block
					let latestLogId = loadLastPosition();
					let latestSize = size;
					if (latestLogId > logBuffer.logId) {
						// if it is not the latest log, get the file size
						size = logBuffer.size ?? (logBuffer.size = transactionLog.getLogFileSize(logBuffer.logId));
						if (position >= size) {
							// we can't read any further in this block, go to the next block
							const nextLogBuffer = getLogMemoryMap(logBuffer.logId + 1)!;
							logBuffer = nextLogBuffer;
							if (latestLogId > logBuffer.logId) {
								// it is non-current log file, we can safely use or cache the size
								size = logBuffer.size ?? (logBuffer.size = transactionLog.getLogFileSize(logBuffer.logId));
							} else {
								size = latestSize; // use the latest position from loadLastPosition
							}
							position = TRANSACTION_LOG_FILE_HEADER_SIZE;
						}
					}
				}
				while(position < size) {
					// advance to the next entry, reading the timestamp and the data
					do {
						try {
							timestamp = dataView.getFloat64(position);
						} catch(error) {
							(error as Error).message += ' at position ' + position + ' of log ' + logBuffer.logId + ' of size ' +  size + 'log buffer length' + logBuffer.length;
							throw error;
						}
						// skip past any leading zeros (which leads to a tiny float that is < 1e-303)
					} while (timestamp < 1 && ++position < size);
					if (!timestamp) {
						// we have gone beyond the last transaction and reached the end
						return { done: true, value: undefined };
					}

					const length = dataView.getUint32(position + 8);
					position += TRANSACTION_LOG_ENTRY_HEADER_SIZE;
					let matchesRange: boolean;
					if (foundExactStart) { // already found the exact start, only need to match on remaining conditions
						matchesRange = (!exclusiveStart || timestamp !== start) && timestamp < end!;
					} else if (exactStart) {
						// in exact start mode, we are look for the exact identifying timestamp of the first transaction
						if (timestamp === start) {
							matchesRange = !exclusiveStart;
							// after finding this transaction, match all remaining (but still respecting end and exclusiveStart
							foundExactStart = true;
						} else {
							matchesRange = false;
						}
					} else { // no exact start, so just match on conditions
						matchesRange = (exclusiveStart ? timestamp > start! : timestamp >= start!) && timestamp < end!;
					}
					let entryStart = position;
					position += length;
					if (matchesRange) {
						// fits in the same block, just subarray the data out
						return {
							done: false,
							value: {
								timestamp,
								endTxn: Boolean(logBuffer[entryStart - 1] & 1),
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
							if (size == undefined) {
								size = transactionLog.getLogFileSize(logBuffer.logId);
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
		function getLogMemoryMap(logId: number) {
			if (logId > 0) {
				let logBuffer = transactionLog._logBuffers.get(logId)?.deref();
				if (logBuffer) { // if we have a cached buffer, return it
					dataView = logBuffer.dataView;
					return logBuffer;
				}
				try {
					logBuffer = transactionLog._getMemoryMapOfFile(logId);
				} catch (error) {
					(error as Error).message += ` (log file ID: ${logId})`;
					throw error;
				}
				if (!logBuffer) return;
				logBuffer.logId = logId;
				dataView = new DataView(logBuffer.buffer);
				logBuffer.dataView = dataView;
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
					nextSize = transactionLog.getLogFileSize(nextLogId);
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