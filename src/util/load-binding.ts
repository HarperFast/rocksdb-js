import nodeGypBuild from 'node-gyp-build/node-gyp-build.js';
import type { Key } from '../types.js';

const binding = nodeGypBuild();

export type NativeTransaction = {
	new(): NativeTransaction;
	abort(): void;
	commit(): void;
	get(key: Key): Buffer;
	put(key: Key, value: any): void;
	remove(key: Key): void;
};

export type NativeDatabaseMode = 'optimistic' | 'pessimistic';

export type NativeDatabaseOptions = {
	name?: string;
	parallelism?: number;
	mode?: NativeDatabaseMode;
};

export type NativeDatabase = {
	new(): NativeDatabase;
	close(): void;
	createTransaction(): NativeTransaction;
	get(key: Key): Buffer;
	opened: boolean;
	open(
		path: string,
		options?: NativeDatabaseOptions
	): void;
	put(key: Key, value: any): void;
	remove(key: Key): void;
};

export const NativeDatabase: NativeDatabase = binding.Database;
export const NativeTransaction: NativeTransaction = binding.Transaction;
export const version = binding.version;
