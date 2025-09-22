import { type NativeDatabase, NativeTransactionLog } from './load-binding';
import type { BufferWithDataView } from './encoding.js';

const BLOCK_SIZE_BITS = 12;
const BLOCK_SIZE = 2**BLOCK_SIZE_BITS; // 4kb
const MAX_LOG_FILE_SIZE = 2**24; // 16mb maximum size of a log file
type LogBuffer = Buffer & {
	dataView: DataView;
	logId: string|number;
}
export class TransactionLog {
	#log: NativeTransactionLog;
	#currentLogBuffer?: LogBuffer;
	#logBuffers = new Map<string|number, LogBuffer>();

	constructor(context: NativeDatabase) {
		this.#log = new NativeTransactionLog(context);
	}

	#getMemoryMappedBuffer(logFile: string) {
		// get a memory mapped buffer for the given log file
		//return this.#log.getMemoryMappedBuffer();
		// if no memory map has been created for this log file, the native code should create one:
		// mmap(0,
	}

	getRange(start: number, end: number) {
		//
		let logBuffer: LogBuffer = this.#currentLogBuffer!// ?? getNextLogFile() as LogBuffer;

		let dataView: DataView = logBuffer?.dataView;
		const firstTimestamp = this.#currentLogBuffer?.dataView.getFloat64(0)!;
		if (firstTimestamp > start) {
			// find earlier log file
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
						return { done: true };
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
		}
	}
}
