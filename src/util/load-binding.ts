import nodeGypBuild from 'node-gyp-build/node-gyp-build.js';
import type { Key } from '../types.js';

export interface Database {
	get(key: Key): Buffer;
	open(path: string): void;
	put(key: Key, value: Buffer): void;
}

const binding = nodeGypBuild();

export const NativeDatabase: { new(): Database } = binding.Database;
export const version = binding.version;
