import { dirname, join, resolve } from 'node:path';
import { readdirSync } from 'node:fs';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import type { BufferWithDataView, Key } from './encoding.js';
import type { IteratorOptions, RangeOptions } from './dbi.js';
import type { Context } from './store.js';

export type TransactionOptions = {
	/**
	 * Whether to disable snapshots.
	 *
	 * @default false
	 */
	disableSnapshot?: boolean;
};

export type NativeTransaction = {
	id: number;
	new(context: NativeDatabase, options?: TransactionOptions): NativeTransaction;
	abort(): void;
	commit(resolve: () => void, reject: (err: Error) => void): void;
	commitSync(): void;
	get(key: Key, resolve: (value: Buffer) => void, reject: (err: Error) => void): number;
	getCount(options?: RangeOptions): number;
	getSync(key: Key): Buffer;
	getTimestamp(): number;
	putSync(key: Key, value: Buffer | Uint8Array, txnId?: number): void;
	removeSync(key: Key): void;
	setTimestamp(timestamp?: number): void;
	useLog(name: string | number): TransactionLog;
};
export type LogBuffer = Buffer & {
	dataView: DataView;
	logId: number;
	size: number;
}

export type TransactionLog = {
	new(name: string): TransactionLog;
	addEntry(data: Buffer | Uint8Array, txnId?: number): void;
	addEntryCopy(data: Buffer | Uint8Array, txnId?: number): void;
	getMemoryMapOfFile(sequenceId: number): LogBuffer;
	getLogFileSize(sequenceId: number): number;
	getLastCommittedPosition(): Buffer;
	lastCommittedPosition?: Buffer;
};

export declare class NativeIteratorCls<T> implements Iterator<T> {
	constructor(context: Context, options: IteratorOptions);
	next(): IteratorResult<T>;
	return(): IteratorResult<T>;
	throw(): IteratorResult<T>;
}

export type NativeDatabaseMode = 'optimistic' | 'pessimistic';

export type NativeDatabaseOptions = {
	disableWAL?: boolean;
	mode?: NativeDatabaseMode;
	name?: string;
	noBlockCache?: boolean;
	parallelismThreads?: number;
	transactionLogMaxSize?: number;
	transactionLogRetentionMs?: number;
	transactionLogsPath?: string;
};

type ResolveCallback<T> = (value: T) => void;
type RejectCallback = (err: Error) => void;

export type UserSharedBufferCallback = () => void;

export type PurgeLogsOptions = {
	destroy?: boolean;
	name?: string;
};

export type NativeDatabase = {
	new(): NativeDatabase;
	addListener(event: string, callback: (...args: any[]) => void): void;
	clear(resolve: ResolveCallback<number>, reject: RejectCallback, batchSize?: number): void;
	clearSync(batchSize?: number): number;
	close(): void;
	notify(event: string | BufferWithDataView, args?: any[]): boolean;
	get(key: BufferWithDataView, resolve: ResolveCallback<Buffer>, reject: RejectCallback, txnId?: number): number;
	getCount(options?: RangeOptions, txnId?: number): number;
	getOldestSnapshotTimestamp(): number;
	getSync(key: BufferWithDataView, txnId?: number): Buffer;
	getUserSharedBuffer(key: BufferWithDataView, defaultBuffer: ArrayBuffer, callback?: UserSharedBufferCallback): ArrayBuffer;
	hasLock(key: BufferWithDataView): boolean;
	listeners(event: string | BufferWithDataView): number;
	listLogs(): string[];
	opened: boolean;
	open(
		path: string,
		options?: NativeDatabaseOptions
	): void;
	purgeLogs(options?: PurgeLogsOptions): string[];
	putSync(key: BufferWithDataView, value: any, txnId?: number): void;
	removeListener(event: string | BufferWithDataView, callback: () => void): boolean;
	removeSync(key: BufferWithDataView, txnId?: number): void;
	tryLock(key: BufferWithDataView, callback?: () => void): boolean;
	unlock(key: BufferWithDataView): void;
	useLog(name: string): TransactionLog;
	withLock(key: BufferWithDataView, callback: () => void | Promise<void>): Promise<void>;
};

export type RocksDatabaseConfig = {
	blockCacheSize?: number;
};

const nativeExtRE = /\.node$/;

/**
 * Locates the native binding in the `build` directory, then the `prebuilds`
 * directory.
 *
 * @returns The path to the native binding.
 */
function locateBinding(): string {
	const baseDir = dirname(dirname(fileURLToPath(import.meta.url)));

	for (const type of ['Release', 'Debug'] as const) {
		try {
			const dir = join(baseDir, 'build', type);
			const files = readdirSync(dir);
			for (const file of files) {
				if (nativeExtRE.test(file)) {
					return resolve(dir, file);
				}
			}

		/* v8 ignore next -- @preserve */
		} catch {}
	}

	// the following lines are non-trivial to test, so we'll ignore them
	/* v8 ignore next 17 -- @preserve */

	// check prebuilds
	try {
		for (const target of readdirSync(join(baseDir, 'prebuilds'))) {
			const [platform, arch] = target.split('-');
			if (platform === process.platform && arch === process.arch) {
				for (const binding of readdirSync(join(baseDir, 'prebuilds', target))) {
					if (nativeExtRE.test(binding)) {
						return resolve(join(baseDir, 'prebuilds', target, binding));
					}
				}
			}
		}
	} catch {}

	throw new Error('Unable to locate rocksdb-js native binding');
}

const req = createRequire(import.meta.url);
const binding = req(locateBinding());

export const config: (options: RocksDatabaseConfig) => void = binding.config;
export const constants: {
	WOOF_TOKEN: number;
	BLOCK_SIZE: number;
	FILE_HEADER_SIZE: number;
	BLOCK_HEADER_SIZE: number;
	TXN_HEADER_SIZE: number;
	CONTINUATION_FLAG: number;
} = binding.constants;
export const NativeDatabase: NativeDatabase = binding.Database;
export const NativeIterator: typeof NativeIteratorCls = binding.Iterator;
export const NativeTransaction: NativeTransaction = binding.Transaction;
export const TransactionLog: TransactionLog = binding.TransactionLog;
export const version: string = binding.version;
