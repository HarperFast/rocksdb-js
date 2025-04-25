import { join, resolve } from 'node:path';
import { readdirSync } from 'node:fs';
import { createRequire } from 'node:module';
import type { Key } from './types.js';

export type NativeTransaction = {
	id: number;
	new(): NativeTransaction;
	abort(): void;
	commit(resolve: () => void, reject: (err: Error) => void): void;
	get(key: Key, txnId?: number): Buffer;
	put(key: Key, value: Buffer | Uint8Array, txnId?: number): void;
	remove(key: Key, txnId?: number): void;
};

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
	createTransaction(): NativeTransaction;
	get(key: Key, txnId?: number): Buffer;
	opened: boolean;
	open(
		path: string,
		options?: NativeDatabaseOptions
	): void;
	put(key: Key, value: any, txnId?: number): void;
	remove(key: Key, txnId?: number): void;
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
	for (const type of ['Release', 'Debug'] as const) {
		try {
			const dir = join('build', type);
			const files = readdirSync(dir);
			for (const file of files) {
				if (nativeExtRE.test(file)) {
					return resolve(dir, file);
				}
			}
		} catch {
			// squelch
		}
	}

	// check prebuilds
	try {
		for (const target of readdirSync('prebuilds')) {
			const [platform, arch] = target.split('-');
			if (platform === process.platform && arch === process.arch) {
				for (const binding of readdirSync(join('prebuilds', target))) {
					if (nativeExtRE.test(binding)) {
						return resolve('prebuilds', target, binding);
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
export const version = binding.version;
