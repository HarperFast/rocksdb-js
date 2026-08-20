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
// Computed specifier (not a literal) so the config bundler doesn't hard-resolve it:
// the version banner degrades to '?' when the module can't load, and tests run from
// src with no dist build required.
const indexUrl = new URL('./src/index.ts', import.meta.url).href;
const rocksdbVersion = await import(indexUrl).then((m) => m.versions.rocksdb).catch(() => '?');
console.log(`${runtime} (${machine}) rocksdb-js/${version} RocksDB/${rocksdbVersion}`);

const isAlternateRuntime = !!(process.versions.bun || process.versions.deno);

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
		pool: isAlternateRuntime ? 'forks' : 'threads',
		// Deno's node:worker_threads compat is flaky when native-backed tests run
		// concurrently in the same fork; sequential execution avoids V8 HandleScope crashes.
		sequence: isAlternateRuntime ? { concurrent: false } : undefined,
		reporters: ['verbose'],
		silent: false,
		testTimeout: 30000,
		watch: false,
	},
});
