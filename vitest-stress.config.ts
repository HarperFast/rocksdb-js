import { defineConfig } from 'vitest/config';
import os from 'node:os';
import { readFileSync } from 'node:fs';

const runtime = process.versions.bun
	? `Bun/${process.versions.bun}`
	: process.versions.deno
		? `Deno/${process.versions.deno}`
		: `Node.js/${process.versions.node}`;
const memory = `${(os.totalmem() / 1024 / 1024 / 1024).toFixed(0)}GB`;
const machine = `${process.platform}/${process.arch}, ${os.cpus().length} cpus, ${memory}`;
const version = JSON.parse(readFileSync('./package.json', 'utf8')).version;
const rocksdbVersion = await import('./dist/index.js')
	.then(m => m.versions.rocksdb)
	.catch(() => '?');
console.log(`${runtime} (${machine}) rocksdb-js/${version} RocksDB/${rocksdbVersion}`);

export default defineConfig({
	test: {
		allowOnly: true,
		environment: 'node',
		globals: false,
		include: ['stress-test/**/*.test.ts'],
		pool: 'threads',
		poolOptions: {
			threads: {
				// NOTE: by default, Vitest will run tests in parallel, but
				// single threaded mode is useful for debugging:
				// singleThread: true
			}
		},
		reporters: ['verbose'],
		silent: false,
		testTimeout: 5 * 60 * 1000, // 5 minutes
		watch: false
	}
});
