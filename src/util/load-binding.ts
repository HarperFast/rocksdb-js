import nodeGypBuild from 'node-gyp-build/node-gyp-build.js';
import type { Key } from '../types.js';

const binding = nodeGypBuild();

export type Txn = {
	new(): Txn;
	// abort(): void;
	// commit(): void;
	get(key: Key): Buffer;
	// put(key: Key, value: any): void;
	// remove(key: Key): void;
};

export type DB = {
	new(): DB;
	close(): void;
	get(key: Key): Buffer;
	opened: boolean;
	open(
		path: string,
		options?: {
			name?: string;
			parallelism?: number;
		}
	): void;
	put(key: Key, value: any): void;
	remove(key: Key): void;
	transaction(callback: (txn: Txn) => Promise<void>): Promise<void>;
};

export type DBContext = {
	txn?: Txn;
};

export const DB: DB = binding.DB;
export const Txn: Txn = binding.Txn;
export const version = binding.version;
