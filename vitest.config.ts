import { readFileSync } from 'node:fs';
import os from 'node:os';
import { defineConfig } from 'vitest/config';

const runtime = process.versions.bun
	? `Bun/${process.versions.bun}`
	: process.versions.deno
		? `Deno/${process.versions.deno}`
		: `Node.js/${process.versions.node}`;
const memory = `${(os.totalmem() / 1024 / 1024 / 1024).toFixed(0)}GB`;
const machine = `${process.platform}/${process.arch}, ${os.cpus().length} cpus, ${memory}`;
const version = JSON.parse(readFileSync('./package.json', 'utf8')).version;
const rocksdbVersion = await import('./dist/index.mjs')
	.then((m) => m.versions.rocksdb)
	.catch(() => '?');
console.log(`${runtime} (${machine}) rocksdb-js/${version} RocksDB/${rocksdbVersion}`);

export default defineConfig({
	test: {
		allowOnly: true,
		benchmark: { include: ['benchmark/**/*.bench.ts'], reporters: ['verbose'] },
		coverage: { include: ['src/**/*.ts'], reporter: ['html', 'lcov', 'text'] },
		environment: 'node',
		fileParallelism: false,
		exclude: ['stress-test/**/*.test.ts'],
		globals: false,
		hookTimeout: 30000,
		include: ['test/**/*.test.ts'],
		pool: process.versions.bun || process.versions.deno ? 'forks' : 'threads',
		reporters: ['verbose'],
		silent: false,
		testTimeout: 30000,
		watch: false,
	},
});
