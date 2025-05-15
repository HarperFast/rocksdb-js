import { dirname, join, resolve } from 'node:path';
import { readdirSync } from 'node:fs';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import type { Key } from './encoding.js';
import type { IteratorOptions } from './iterator.js';

export type NativeTransaction = {
	id: number;
	new(context: NativeDatabase): NativeTransaction;
	abort(): void;
	commit(resolve: () => void, reject: (err: Error) => void): void;
	commitSync(): void;
	get(key: Key, resolve: (value: Buffer) => void, reject: (err: Error) => void): number;
	getSync(key: Key, txnId?: number): Buffer;
	putSync(key: Key, value: Buffer | Uint8Array, txnId?: number): void;
	removeSync(key: Key, txnId?: number): void;
};

export interface NativeIterator<T> {
	new(context: NativeDatabase | NativeTransaction, options?: IteratorOptions): NativeIterator<T>;
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

export type NativeDatabase = {
	new(): NativeDatabase;
	close(): void;
	get(key: Key, resolve: (value: Buffer) => void, reject: (err: Error) => void, txnId?: number): number;
	getSync(key: Key, txnId?: number): Buffer;
	opened: boolean;
	open(
		path: string,
		options?: NativeDatabaseOptions
	): void;
	putSync(key: Key, value: any, txnId?: number): void;
	removeSync(key: Key, txnId?: number): void;
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
export const NativeTransaction: NativeTransaction = binding.Transaction;
export const NativeIterator: NativeIterator<any> = binding.Iterator;
export const version = binding.version;
