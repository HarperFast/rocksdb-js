import { dirname, join, resolve } from 'node:path';
import { readdirSync } from 'node:fs';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import type { BufferWithDataView, Key } from './encoding.js';
import type { IteratorOptions, RangeOptions } from './dbi.js';

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
	putSync(key: Key, value: Buffer | Uint8Array, txnId?: number): void;
	removeSync(key: Key): void;
};

export declare class NativeIteratorCls<T> implements Iterator<T> {
	constructor(context: NativeDatabase | NativeTransaction, options: IteratorOptions);
	next(): IteratorResult<T>;
	return(): IteratorResult<T>;
	throw(): IteratorResult<T>;
}

export type NativeDatabaseMode = 'optimistic' | 'pessimistic';

export type NativeDatabaseOptions = {
	name?: string;
	noBlockCache?: boolean;
	parallelismThreads?: number;
	mode?: NativeDatabaseMode;
};

type ResolveCallback<T> = (value: T) => void;
type RejectCallback = (err: Error) => void;

export type NativeDatabase = {
	new(): NativeDatabase;
	addListener(key: BufferWithDataView, callback: (...args: any[]) => void): void;
	clear(resolve: ResolveCallback<number>, reject: RejectCallback, batchSize?: number): void;
	clearSync(batchSize?: number): number;
	close(): void;
	emit(key: BufferWithDataView, args?: any[]): boolean;
	get(key: BufferWithDataView, resolve: ResolveCallback<Buffer>, reject: RejectCallback, txnId?: number): number;
	getCount(options?: RangeOptions, txnId?: number): number;
	getOldestSnapshotTimestamp(): number;
	getSync(key: BufferWithDataView, txnId?: number): Buffer;
	getUserSharedBuffer(key: BufferWithDataView, defaultBuffer: ArrayBuffer): ArrayBuffer;
	hasLock(key: BufferWithDataView): boolean;
	opened: boolean;
	open(
		path: string,
		options?: NativeDatabaseOptions
	): void;
	putSync(key: BufferWithDataView, value: any, txnId?: number): void;
	removeListener(key: BufferWithDataView, callback: () => void): boolean;
	removeSync(key: BufferWithDataView, txnId?: number): void;
	tryLock(key: BufferWithDataView, callback?: () => void): boolean;
	unlock(key: BufferWithDataView): void;
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
export const NativeDatabase: NativeDatabase = binding.Database;
export const NativeIterator: typeof NativeIteratorCls = binding.Iterator;
export const NativeTransaction: NativeTransaction = binding.Transaction;
export const version = binding.version;
