import nodeGypBuild from 'node-gyp-build/node-gyp-build.js';
import type { Key } from '../types.js';

const binding = nodeGypBuild();

export type NativeTransaction = {
	id: number;
	new(): NativeTransaction;
	abort(): void;
	commit(resolve: () => void, reject: (err: Error) => void): void;
	get(key: Key): Buffer;
	put(key: Key, value: any): void;
	remove(key: Key): void;
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

export const config: (options: RocksDatabaseConfig) => void = binding.config;
export const NativeDatabase: NativeDatabase = binding.Database;
export const NativeTransaction: NativeTransaction = binding.Transaction;
export const version = binding.version;
