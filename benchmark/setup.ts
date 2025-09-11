import { RocksDatabase, RocksDatabaseOptions } from '../dist/index.js';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { rimraf } from 'rimraf';
import * as lmdb from 'lmdb';
import { bench as vitestBench, describe } from 'vitest';
import { randomBytes } from 'node:crypto';
import { parentPort, Worker, workerData } from 'node:worker_threads';

type LMDBDatabase = lmdb.RootDatabase<any, string> & { path: string };

interface BenchmarkContext<T> extends Record<string, any> {
	db: T;
};

type BenchmarkOptions<T, U> = {
	name?: string,
	dbOptions?: U,
	bench: (ctx: BenchmarkContext<T>) => void | Promise<void>,
	setup?: (ctx: BenchmarkContext<T>) => void | Promise<void>,
	teardown?: (ctx: BenchmarkContext<T>) => void | Promise<void>
};

export function benchmark(type: 'rocksdb', options: BenchmarkOptions<RocksDatabase, RocksDatabaseOptions>): void;
export function benchmark(type: 'lmdb', options: BenchmarkOptions<LMDBDatabase, lmdb.RootDatabaseOptions>): void;
export function benchmark(type: string, options: any): void {
	if (type !== 'rocksdb' && type !== 'lmdb') {
		throw new Error(`Unsupported benchmark type: ${type}`);
	}

	if ((process.env.ROCKSDB_ONLY && type !== 'rocksdb') || (process.env.LMDB_ONLY && type !== 'lmdb')) {
		return;
	}

	const { bench, setup, teardown, dbOptions, name } = options;
	const path = join(tmpdir(), `rocksdb-benchmark-${randomBytes(8).toString('hex')}`);
	let ctx: BenchmarkContext<any>;

	vitestBench(name || type, () => bench(ctx), {
		throws: true,
		setup() {
			if (type === 'rocksdb') {
				ctx = { data: null, db: RocksDatabase.open(path, dbOptions) };
			} else {
				ctx = { data: null, db: lmdb.open({ path, compression: true, ...dbOptions }) };
			}
			if (typeof setup === 'function') {
				return setup(ctx);
			}
		},
		async teardown() {
			if (typeof teardown === 'function') {
				await teardown(ctx);
			}
			if (ctx.db) {
				const path = ctx.db.path;
				await ctx.db.close();
				try {
					await rimraf(path);
				} catch {
					// ignore cleanup errors in benchmarks
				}
			}
		}
	});
}

export function generateTestData(count: number, keySize: number = 20, valueSize: number = 100) {
	const data: Array<{ key: string; value: string }> = [];

	for (let i = 0; i < count; i++) {
		const key = `key-${i.toString().padStart(10, '0')}-${randomString(keySize - 15)}`;
		const value = randomString(valueSize);
		data.push({ key, value });
	}

	return data;
}

export function randomString(length: number): string {
	const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
	let result = '';
	for (let i = 0; i < length; i++) {
		result += chars.charAt(Math.floor(Math.random() * chars.length));
	}
	return result;
}

export function generateSequentialKeys(count: number, prefix: string = 'key'): string[] {
	return Array.from({ length: count }, (_, i) => {
		return `${prefix}-${i.toString().padStart(10, '0')}`;
	});
}

export function generateRandomKeys(count: number, keySize: number = 20): string[] {
	return Array.from({ length: count }, () => randomString(keySize));
}


interface WorkerDescribeOptions {
	numWorkers?: number;
}

export function workerDescribe(name: string, fn: () => void, options?: WorkerDescribeOptions) {
	return describe(name, fn);
}

workerDescribe.only = describe.only;
workerDescribe.skip = describe.skip;
workerDescribe.todo = describe.todo;

export function workerBenchmark(type: 'rocksdb', options: BenchmarkOptions<RocksDatabase, RocksDatabaseOptions>): void;
export function workerBenchmark(type: 'lmdb', options: BenchmarkOptions<LMDBDatabase, lmdb.RootDatabaseOptions>): void;
export function workerBenchmark(type: string, options: any): void {
	if (type !== 'rocksdb' && type !== 'lmdb') {
		throw new Error(`Unsupported benchmark type: ${type}`);
	}

	if ((process.env.ROCKSDB_ONLY && type !== 'rocksdb') || (process.env.LMDB_ONLY && type !== 'lmdb')) {
		return;
	}

	const { bench, setup, teardown, dbOptions, name } = options;
	const path = join(tmpdir(), `rocksdb-benchmark-${randomBytes(8).toString('hex')}`);
	let ctx: BenchmarkContext<any>;

	if (parentPort) {
		//
	} else {
		console.log('main thread');
	}
}

export function workerLaunch(workerData: Record<string, any> = {}) {
	// Node.js 18 and older doesn't properly eval ESM code
	const majorVersion = parseInt(process.versions.node.split('.')[0]);
	const script = majorVersion < 20
		?	`
			const tsx = require('tsx/cjs/api');
			tsx.require('./benchmark/worker.mts', __dirname);
			`
		:	`
			import { register } from 'tsx/esm/api';
			register();
			import('./benchmark/worker.mts');
			`;

	const worker = new Worker(
		script,
		{
			eval: true,
			workerData,
		}
	);

	return worker;
}
