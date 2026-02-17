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
		environment: 'node',
		fileParallelism: false,
		globals: false,
		include: ['stress-test/**/*.stress.test.ts'],
		pool: 'threads',
		reporters: ['verbose'],
		silent: false,
		testTimeout: 5 * 60 * 1000, // 5 minutes
		watch: false,
	},
});
