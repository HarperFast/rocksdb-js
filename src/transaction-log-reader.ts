import {
	constants,
	type LogBuffer,
	type TransactionEntry,
	TransactionLog,
	type TransactionLogQueryOptions,
} from './load-binding.ts';

const FLOAT_TO_UINT32 = new Float64Array(1);
const UINT32_FROM_FLOAT = new Uint32Array(FLOAT_TO_UINT32.buffer);

const { TRANSACTION_LOG_FILE_HEADER_SIZE, TRANSACTION_LOG_ENTRY_HEADER_SIZE } = constants;

/**
 * Number of consecutive well-formed frames that, on their own, signal that real log data has
 * resumed after a framing break. Kept in step with `RESYNC_MIN_FRAMES` in
 * `src/binding/transaction_log/transaction_log_recovery.cpp`, which uses the same heuristic to
 * classify a break as mid-file corruption (valid entries follow) rather than a torn tail.
 */
const RESYNC_MIN_FRAMES = 8;

/**
 * Thrown when an entry's framing is broken. `resyncPosition` is the offset where valid framing
 * resumes, when one was found: the frames after a mid-log break are intact and reachable, and
 * only the broken span between `position` and `resyncPosition` is unreadable. It is `undefined`
 * for a torn tail, where nothing follows the break.
 *
 * The iterator is left positioned at `resyncPosition` (or at end-of-log when there is none), so a
 * caller that chooses to recover the entries past the break calls `next()` again.
 */
export class CorruptFrameError extends RangeError {
	/** Sequence number of the log file the break is in. */
	logId: number;
	/** Offset of the broken frame. */
	position: number;
	/** Offset where valid framing resumes, or `undefined` when nothing valid follows. */
	resyncPosition?: number;
	/** Bytes between the break and the resume point that cannot be read. */
	unreadableBytes: number;

	constructor(
		message: string,
		logId: number,
		position: number,
		resyncPosition: number | undefined
	) {
		super(
			resyncPosition === undefined
				? message
				: `${message}; valid framing resumes at ${resyncPosition.toString(16)}, ${
						resyncPosition - position
					} byte(s) unreadable`
		);
		this.name = 'CorruptFrameError';
		this.logId = logId;
		this.position = position;
		this.resyncPosition = resyncPosition;
		this.unreadableBytes = resyncPosition === undefined ? 0 : resyncPosition - position;
	}
}

/**
 * Whether a complete, in-bounds frame begins at `pos`. The only sane bound on an entry's length is
 * the written extent: a single entry can legitimately exceed the rotation threshold (the first
 * entry written to a fresh file is always written in full). A zero timestamp is the
 * end-of-entries marker, not a frame.
 */
function frameFits(dataView: DataView, pos: number, dataEnd: number): boolean {
	if (pos + TRANSACTION_LOG_ENTRY_HEADER_SIZE > dataEnd) {
		return false;
	}
	if (dataView.getFloat64(pos) === 0) {
		return false;
	}
	const length = dataView.getUint32(pos + 8);
	if (length === 0) {
		return false;
	}
	return pos + TRANSACTION_LOG_ENTRY_HEADER_SIZE + length <= dataEnd;
}

/**
 * Finds the offset in `[from, dataEnd)` where valid log framing resumes: a run of at least
 * `RESYNC_MIN_FRAMES` well-formed frames, or a run that lands exactly on `dataEnd`. Returns
 * `undefined` when nothing resumes (a torn tail).
 *
 * `dataEnd` must be the **written extent**, not the mapped capacity. The log's memory map is
 * pre-extended well past the data, and every offset inside that zero padding reads as an
 * end-of-entries marker — so scanning against the map would both lose the exact-end signal (the
 * chain can never land on a capacity that data does not reach) and, if a zero were accepted as a
 * terminator, let a chain "end" anywhere in megabytes of padding. Against the written extent the
 * end is a single offset, so a chain landing on it is ~1/2^32 to be coincidence.
 *
 * The JS counterpart of `validFramingResumes()` in `transaction_log_recovery.cpp`, which answers
 * the same question at open time but keeps only the yes/no, discarding the offset.
 */
function findResyncPosition(dataView: DataView, from: number, dataEnd: number): number | undefined {
	const lastStart = dataEnd - TRANSACTION_LOG_ENTRY_HEADER_SIZE;
	for (let start = from; start <= lastStart; start++) {
		let pos = start;
		let frames = 0;
		while (frames < RESYNC_MIN_FRAMES && frameFits(dataView, pos, dataEnd)) {
			pos += TRANSACTION_LOG_ENTRY_HEADER_SIZE + dataView.getUint32(pos + 8);
			if (++frames >= RESYNC_MIN_FRAMES || pos === dataEnd) {
				return start;
			}
		}
	}
}

/**
 * Builds the report for a broken frame, with the offset iteration should continue from. Resolves
 * the written extent here rather than per frame: `getLogFileSize` crosses into native and takes
 * the store mutex, which a healthy read must never pay.
 */
function corruptFrame(
	message: string,
	transactionLog: TransactionLog,
	dataView: DataView,
	logBuffer: LogBuffer,
	position: number,
	limit: number,
	size: number,
	readUncommitted: boolean
): { error: CorruptFrameError; nextPosition: number } {
	// An uncommitted read's `limit` is the pre-extended map, so ask the file for its written extent;
	// a committed read is already bounded at the watermark.
	let dataEnd = limit;
	if (readUncommitted) {
		try {
			const writtenExtent = transactionLog.getLogFileSize(logBuffer.logId);
			dataEnd = writtenExtent > 0 ? Math.min(logBuffer.length, writtenExtent) : 0;
		} catch {
			dataEnd = 0;
		}
	}
	const resyncPosition = findResyncPosition(dataView, position + 1, dataEnd);
	return {
		error: new CorruptFrameError(message, logBuffer.logId, position, resyncPosition),
		// With no resume point iteration is over, so land past `size` as well as the readable end:
		// a committed `size` that over-reports the mapped buffer would otherwise leave
		// `position < size`, and the next call would re-enter the loop and read off the end.
		nextPosition: resyncPosition ?? Math.max(dataEnd || limit, size, position + 1),
	};
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
Object.defineProperty(TransactionLog.prototype, 'query', {
	value(
		this: TransactionLog,
		{
			start,
			end,
			exactStart,
			startFromLastFlushed,
			readUncommitted,
			exclusiveStart,
		}: TransactionLogQueryOptions = {}
	): IterableIterator<TransactionEntry> {
		if (!this._lastCommittedPosition) {
			// if this is the first time we are querying the log, initialize the last committed position and memory map cache
			const lastCommittedPosition = this._getLastCommittedPosition();
			this._lastCommittedPosition = new Float64Array(lastCommittedPosition.buffer);
			this._logBuffers = new Map<number, WeakRef<LogBuffer>>();
		}
		end ??= Number.MAX_VALUE;

		const transactionLog = this;
		let { logId: latestLogId, size } = loadLastPosition(this, !!readUncommitted);
		let logId = latestLogId;
		let position = 0;
		let dataView: DataView;
		let logBuffer: LogBuffer | undefined = this._currentLogBuffer; // try the current one first
		let foundExactStart = false;

		if (start === undefined && !startFromLastFlushed) {
			// if no start timestamp is specified, start from the last committed position
			position = size;
			if (position === 0) {
				// if the file is empty, start after the header
				position = TRANSACTION_LOG_FILE_HEADER_SIZE;
			}
			start = 0;
		} else {
			if (startFromLastFlushed) {
				// read from the last flushed position
				FLOAT_TO_UINT32[0] = this._getLastFlushed();
				if (FLOAT_TO_UINT32[0] === 0) {
					// no flushes have ever occurred, go to the beginning (which is actually after the file header)
					FLOAT_TO_UINT32[0] = this._findPosition(0);
				}
				start ??= 0; // if no start timestamp is specified, include all
			} else {
				// otherwise, find the log file that contains the start timestamp, and find the position within that file
				FLOAT_TO_UINT32[0] = this._findPosition(start!);
			}
			// extract the log file ID from the 64-bit float returned by _findPosition, which is stored in the high 32 bits of the float
			logId = UINT32_FROM_FLOAT[1];
			// and position from the low 32 bits of the float
			position = UINT32_FROM_FLOAT[0];
			if (position === 0) {
				// if the file is empty, start after the header
				position = TRANSACTION_LOG_FILE_HEADER_SIZE;
			}
		}

		if (logBuffer === undefined || logBuffer.logId !== logId) {
			// if the current log buffer is not the one we want, load the memory map
			logBuffer = getLogMemoryMap(this, logId);

			// if this is the latest, cache for easy access, unless...
			// if we are reading uncommitted, we might be a log file ahead of the committed transaction
			// also, it is pointless to cache the latest log file in a memory map on Windows, because it is not growable
			if (logBuffer && latestLogId === logId && !readUncommitted) {
				this._currentLogBuffer = logBuffer;
			}

			if (logBuffer === undefined) {
				// create a fake log buffer if we don't have any log buffer yet
				logBuffer = Buffer.alloc(0) as unknown as LogBuffer;
				logBuffer.logId = 0;
				logBuffer.size = 0;
				logBuffer.dataView = new DataView(logBuffer.buffer);
				// the outer size variable was set from loadLastPosition() above, but if we
				// couldn't acquire a memory map for that logId (e.g. the committed-position
				// references a logId that doesn't exist on disk — purged, never created, or
				// torn write of the position word), reading at any non-zero size would read
				// past the empty buffer. force size to match the empty buffer; the iterator's
				// position-vs-size logic then routes through the existing advance-to-next-log
				// path, which correctly returns done when no log file can be mapped.
				size = 0;
			}
		}

		dataView = logBuffer.dataView;

		if (latestLogId !== logId) {
			size = logBuffer.size;
			if (size === undefined) {
				size = logBuffer.size = this.getLogFileSize(logId);
			}
		}

		return {
			[Symbol.iterator](): IterableIterator<TransactionEntry> {
				return this;
			},
			next() {
				let timestamp: number;
				if (position >= size) {
					// our position is beyond the size limit, get the updated
					// size in case we can keep reading further from the same block
					const { logId: latestLogId, size: latestSize } = loadLastPosition(
						transactionLog,
						!!readUncommitted
					);
					size = latestSize;
					if (latestLogId > logBuffer!.logId) {
						// if it is not the latest log, get the file size
						size =
							logBuffer!.size ??
							(logBuffer!.size = transactionLog.getLogFileSize(logBuffer!.logId));
						if (position >= size) {
							const nextLogBuffer = getNextLogMemoryMap(
								transactionLog,
								logBuffer!.logId,
								latestLogId
							);
							if (nextLogBuffer) {
								dataView = nextLogBuffer.dataView;
								logBuffer = nextLogBuffer;
								if (latestLogId > logBuffer!.logId) {
									// it is non-current log file, we can safely use or cache the size
									size =
										logBuffer!.size ??
										(logBuffer!.size = transactionLog.getLogFileSize(logBuffer!.logId));
								} else {
									size = latestSize; // use the latest position from loadLastPosition
								}
								position = TRANSACTION_LOG_FILE_HEADER_SIZE;
							}
						}
					}
				}

				while (position < size) {
					try {
						timestamp = dataView.getFloat64(position);
					} catch (error) {
						(error as Error).message += ` at position ${position.toString(16)} of log ${
							logBuffer!.logId
						} (size=${size}, log buffer length=${logBuffer!.length})`;
						throw error;
					}
					if (!timestamp) {
						// we have gone beyond the last transaction and reached the end
						return { done: true, value: undefined };
					}

					// Corruption bound: a committed read can't legitimately extend past
					// the committed `size` (a true entry boundary); an uncommitted read is
					// bounded only by the physically mapped buffer. In both cases the bound
					// is also clamped to `logBuffer.length` — if committed `size` over-reports
					// the mapped buffer (e.g. a truncated/torn file), an unclamped `size`
					// would let `position` advance past the buffer and `subarray` silently
					// return a truncated frame instead of throwing. A torn/corrupt entry
					// can declare a length far past this bound — without the checks below,
					// `position` runs past the buffer, `subarray` hands back a misframed
					// (garbage) transaction, and the advance-to-next-log path can
					// dereference an undefined buffer. Fail loudly with a bounded error.
					// The throw leaves `position` at the resume point, so a broken frame ends the
					// entry rather than the rest of the log (HarperFast/harper#2016, #2063).
					const limit = readUncommitted ? logBuffer!.length : Math.min(size, logBuffer!.length);
					if (position + TRANSACTION_LOG_ENTRY_HEADER_SIZE > limit) {
						const broken = corruptFrame(
							`Corrupt transaction log: truncated entry header at position ${position.toString(16)} of log ${
								logBuffer!.logId
							} (available=${limit - position})`,
							transactionLog,
							dataView,
							logBuffer!,
							position,
							limit,
							size,
							!!readUncommitted
						);
						position = broken.nextPosition;
						throw broken.error;
					}
					const length = dataView.getUint32(position + 8);
					if (length === 0 || position + TRANSACTION_LOG_ENTRY_HEADER_SIZE + length > limit) {
						const broken = corruptFrame(
							length === 0
								? `Corrupt transaction log entry at position ${position.toString(16)} of log ${
										logBuffer!.logId
									}: zero-length entry`
								: `Corrupt transaction log entry at position ${position.toString(16)} of log ${
										logBuffer!.logId
									}: declared length ${length} overruns the log (limit=${limit})`,
							transactionLog,
							dataView,
							logBuffer!,
							position,
							limit,
							size,
							!!readUncommitted
						);
						position = broken.nextPosition;
						throw broken.error;
					}
					position += TRANSACTION_LOG_ENTRY_HEADER_SIZE;
					let matchesRange: boolean;
					if (foundExactStart) {
						// already found the exact start, only need to match on remaining conditions
						matchesRange = (!exclusiveStart || timestamp !== start) && timestamp < end;
					} else if (exactStart) {
						// in exact start mode, we are look for the exact identifying timestamp of the first transaction
						if (timestamp === start) {
							matchesRange = !exclusiveStart;
							// after finding this transaction, match all remaining (but still respecting end and exclusiveStart
							foundExactStart = true;
						} else {
							matchesRange = false;
						}
					} else {
						// no exact start, so just match on conditions
						matchesRange =
							(exclusiveStart ? timestamp > start! : timestamp >= start!) && timestamp < end;
					}
					const entryStart = position;
					position += length;
					if (matchesRange) {
						// fits in the same block, just subarray the data out
						return {
							done: false,
							value: {
								timestamp,
								endTxn: Boolean(logBuffer![entryStart - 1] & 1),
								data: logBuffer!.subarray(entryStart, position),
							},
						};
					}
					if (position >= size) {
						// move to the next log file
						const { logId: latestLogId, size: latestSize } = loadLastPosition(
							transactionLog,
							!!readUncommitted
						);
						size = latestSize;
						if (latestLogId > logBuffer!.logId) {
							const nextLogBuffer = getNextLogMemoryMap(
								transactionLog,
								logBuffer!.logId,
								latestLogId
							);
							if (!nextLogBuffer) {
								// the next log file can't be mapped (purged, mid-rotation,
								// 0-byte at mmap time, FS race); stop cleanly rather than
								// dereferencing an undefined buffer
								return { done: true, value: undefined };
							}
							logBuffer = nextLogBuffer;
							dataView = logBuffer.dataView;
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
			},
		};
	},
});

function getLogMemoryMap(transactionLog: TransactionLog, logId: number): LogBuffer | undefined {
	if (logId <= 0) {
		return;
	}
	let logBuffer = transactionLog._logBuffers!.get(logId)?.deref();
	if (logBuffer) {
		// if we have a cached buffer, return it
		return logBuffer;
	}
	try {
		logBuffer = transactionLog._getMemoryMapOfFile(logId);
	} catch (error) {
		(error as Error).message += ` (log file ID: ${logId})`;
		throw error;
	}
	if (!logBuffer) {
		return;
	}
	logBuffer.logId = logId;
	logBuffer.dataView = new DataView(logBuffer.buffer);
	transactionLog._logBuffers!.set(logId, new WeakRef(logBuffer)); // add to cache
	let maxMisses = 3;
	for (const [logId, reference] of transactionLog._logBuffers!) {
		// clear out any references that have been collected
		if (reference.deref() === undefined) {
			transactionLog._logBuffers!.delete(logId);
		} else if (--maxMisses === 0) {
			break;
		}
	}
	return logBuffer;
}

function getNextLogMemoryMap(
	transactionLog: TransactionLog,
	logId: number,
	latestLogId: number
): LogBuffer | undefined {
	while (++logId <= latestLogId) {
		const logBuffer = getLogMemoryMap(transactionLog, logId);
		if (logBuffer) return logBuffer;
	}
}

function loadLastPosition(
	transactionLog: TransactionLog,
	readUncommitted: boolean
): { logId: number; size: number } {
	// atomically copy the full 64-bit last committed position word to a local variable so we can read it without memory tearing
	FLOAT_TO_UINT32[0] = transactionLog._lastCommittedPosition![0];
	let logId = UINT32_FROM_FLOAT[1];
	let size = 0;

	if (readUncommitted) {
		// if we are reading uncommitted transactions, we need to read the entire log file to find the latest position
		let nextSize = 0;
		let nextLogId = logId || 1;
		while (true) {
			nextSize = transactionLog.getLogFileSize(nextLogId);
			if (nextSize === 0) {
				// if the size is zero, there is no next log file, we are done
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
	return { logId, size };
}
