import nodeGypBuild from 'node-gyp-build/node-gyp-build.js';
import type { Key } from '../types.js';

const binding = nodeGypBuild();

export interface DBI {
	new(path: string, name?: string): DBI;
	close(): void;
	get(key: Key): Buffer;
	opened: boolean;
	open(options?: { name?: string; parallelism?: number }): void;
	put(key: Key, value: any): void;
}

export const DBI: DBI = binding.DBI;

export const version = binding.version;
