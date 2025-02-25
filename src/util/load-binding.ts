import nodeGypBuild from 'node-gyp-build/node-gyp-build.js';
import type { Key } from '../types.js';

export interface Database {
	close(): void;
	get(key: Key): Buffer;
	opened: boolean;
	open(options?: { parallelism?: number }): void;
	put(key: Key, value: any): void;
}

const binding = nodeGypBuild();

export const NativeDatabase: { new(path: string): Database } = binding.Database;
export const version = binding.version;
