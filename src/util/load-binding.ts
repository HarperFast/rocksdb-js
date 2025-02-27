import nodeGypBuild from 'node-gyp-build/node-gyp-build.js';
import type { Key } from '../types.js';

export interface Database {
	close(): void;
	get(key: Key): Buffer;
	opened: boolean;
	open(options?: { name?: string; parallelism?: number }): void;
	put(key: Key, value: any): void;
}

export interface Store {
}

const binding = nodeGypBuild();

export const NativeDatabase: { new(path: string): Database } = binding.Database;
export const NativeStore: { new(name: string): Store } = binding.Store;
export const version = binding.version;
