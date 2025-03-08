import nodeGypBuild from 'node-gyp-build/node-gyp-build.js';
import type { Key } from '../types.js';

const binding = nodeGypBuild();

export interface DB {
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
}

export const DB: DB = binding.DB;

export const version = binding.version;
