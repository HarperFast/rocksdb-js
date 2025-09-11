import { RocksDatabase, RocksDatabaseOptions } from '../dist/index.js';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { rimraf } from 'rimraf';
import * as lmdb from 'lmdb';
import { randomBytes } from 'node:crypto';
import { parentPort, Worker, workerData } from 'node:worker_threads';

const vitestBench = workerData.benchmarkWorker ? () => {
	console.log('vitest bench worker');
} : (await import('vitest')).bench;

type LMDBDatabase = lmdb.RootDatabase<any, string> & { path: string };

interface BenchmarkContext<T> extends Record<string, any> {
	db: T;
};

type BenchmarkOptions<T, U> = {
	bench: (ctx: BenchmarkContext<T>) => void | Promise<void>,
	dbOptions?: U,
	name?: string,
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
				ctx = { db: RocksDatabase.open(path, dbOptions) };
			} else {
				ctx = { db: lmdb.open({ path, compression: true, ...dbOptions }) };
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

/**
 * Worker Benchmark API
 */

interface WorkerBenchmark {
	bench: WorkerBenchmarkOptions['bench'];
	dbOptions?: any;
	name: string;
	numWorkers?: number;
	setup?: WorkerBenchmarkOptions['setup'];
	teardown?: WorkerBenchmarkOptions['teardown'];
	type: 'rocksdb' | 'lmdb';
}
interface WorkerSuite {
	name: string;
	benchmarks: WorkerBenchmark[];
	suites: Record<string, WorkerSuite>;
}

interface WorkerState {
	worker: Worker;
	benchPromise: ReturnType<typeof withResolvers<void>>;
	setupPromise: ReturnType<typeof withResolvers<void>>;
	teardownPromise: ReturnType<typeof withResolvers<void>>;
}

interface WorkerBenchmarkOptions extends BenchmarkOptions<any, any> {
	numWorkers?: number;
}

const workerSuites: Record<string, WorkerSuite> = {};
let workerCurrentSuites: WorkerSuite[] = [];

function describeShim(name: string, fn: () => void) {
	const suites = workerCurrentSuites.length > 0
		? workerCurrentSuites[workerCurrentSuites.length - 1].suites
		: workerSuites;
	let suite: WorkerSuite | undefined = suites[name];

	if (!suite) {
		suite = {
			name,
			benchmarks: [],
			suites: {}
		};
		suites[name] = suite;
	}

	workerCurrentSuites.push(suite);
	fn();
	workerCurrentSuites.pop();
}

async function getVitestDescribe() {
	if (workerData.benchmarkWorker) {
		return Object.assign(describeShim, { only: () => {}, skip: () => {}, todo: () => {} });
	}

	const { describe } = await import('vitest');
	return Object.assign((name: string, fn: () => void) => {
		describeShim(name, () => {
			const state = [...workerCurrentSuites];
			describe(name, () => {
				const previous = workerCurrentSuites;
				workerCurrentSuites = state;
				fn();
				workerCurrentSuites = previous;
			});
		});
	}, {
		only: describe.only,
		skip: describe.skip,
		todo: describe.todo,
	});
}

export const workerDescribe = await getVitestDescribe();

export function workerBenchmark(type: 'rocksdb', options: WorkerBenchmarkOptions): void;
export function workerBenchmark(type: 'lmdb', options: WorkerBenchmarkOptions): void;
export function workerBenchmark(type: string, options: any): void {
	if (type !== 'rocksdb' && type !== 'lmdb') {
		throw new Error(`Unsupported benchmark type: ${type}`);
	}

	if ((process.env.ROCKSDB_ONLY && type !== 'rocksdb') || (process.env.LMDB_ONLY && type !== 'lmdb')) {
		return;
	}

	let { bench, dbOptions, name, numWorkers, setup, teardown } = options;
	numWorkers = Math.max(numWorkers || 1, 1);
	const benchmarkName = name || type;
	const suite = workerCurrentSuites[workerCurrentSuites.length - 1];
	if (suite) {
		suite.benchmarks.push({ bench, dbOptions, name: benchmarkName, numWorkers, setup, teardown, type });
	}

	if (workerData.benchmarkWorker) {
		return;
	}

	const workerState: WorkerState[] = [];
	const workerPayload = {
		suites: workerCurrentSuites.map(suite => suite.name),
		benchmark: benchmarkName
	};

	vitestBench(benchmarkName, async () => {
		for (let i = 0; i < numWorkers; i++) {
			const state = workerState[i];
			if (!state) {
				throw new Error(`Worker ${i} not found`);
			}
			state.worker.postMessage({ bench: true });
			await state.benchPromise.promise;
		}
	}, {
		throws: true,
		async setup() {
			await Promise.all(Array.from({ length: numWorkers }, () => new Promise<void>((resolve, reject) => {
				const benchPromise = withResolvers<void>();
				const setupPromise = withResolvers<void>();
				const teardownPromise = withResolvers<void>();
				const worker = workerLaunch(workerPayload);
				workerState.push({ worker, benchPromise, setupPromise, teardownPromise });
				worker.on('error', reject);
				worker.on('message', event => {
					if (event.setupDone) {
						setupPromise.resolve();
						resolve();
					} else if (event.benchDone) {
						benchPromise.resolve();
					} else if (event.teardownDone) {
						teardownPromise.resolve();
					}
				});
			})));
		},
		async teardown() {
			for (let i = 0; i < numWorkers; i++) {
				const state = workerState[i];
				if (!state) {
					throw new Error(`Worker ${i} not found`);
				}
				state.worker.postMessage({ teardown: true });
				await state.teardownPromise.promise;
			}
		}
	});
}

export async function workerInit() {
	if (!parentPort) {
		throw new Error('Failed to initialize worker: parentPort is not available');
	}

	const path = join(tmpdir(), `rocksdb-benchmark-${randomBytes(8).toString('hex')}`);
	let ctx: BenchmarkContext<any>;

	parentPort.on('message', async (event: any) => {
		if (event.bench) {
			const { bench } = workerFindBenchmark();
			await bench(ctx);
			parentPort?.postMessage({ benchDone: true });
		} else if (event.teardown) {
			const { teardown } = workerFindBenchmark();
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
			parentPort?.postMessage({ teardownDone: true });
			process.exit(0);
		}
	});

	const { setup, dbOptions, type } = workerFindBenchmark();
	if (type === 'rocksdb') {
		ctx = { db: RocksDatabase.open(path, dbOptions) };
	} else {
		ctx = { db: lmdb.open({ path, compression: true, ...dbOptions }) };
	}
	if (typeof setup === 'function') {
		await setup(ctx);
	}
	parentPort.postMessage({ setupDone: true });
}

function workerFindBenchmark() {
	let suite: WorkerSuite | undefined;
	let suites: Record<string, WorkerSuite> | undefined = workerSuites;
	for (const suiteName of workerData.suites as string[]) {
		suite = suites[suiteName];
		if (!suite) {
			throw new Error(`Unknown suite: ${suiteName}`);
		}
		suites = suite.suites;
	}
	const options = suite?.benchmarks.find(bm => bm.name === workerData.benchmark);
	if (!options) {
		throw new Error(`Unknown benchmark: ${workerData.benchmark}`);
	}
	return options;
}

export function workerLaunch(workerData: Record<string, any> = {}) {
	// Node.js 18 and older doesn't properly eval ESM code
	const majorVersion = parseInt(process.versions.node.split('.')[0]);
	const script = majorVersion < 20
		?	`
			const tsx = require('tsx/cjs/api');
			tsx.require('./benchmark/worker.bench.ts', __dirname);
			const { workerInit } = tsx.require('./benchmark/setup.ts', __dirname);
			workerInit();
			`
		:	`
			import { register } from 'tsx/esm/api';
			register();
			import('./benchmark/worker.bench.ts')
				.then(() => import('./benchmark/setup.ts'))
				.then(module => module.workerInit());
			`;

	return new Worker(
		script,
		{
			eval: true,
			workerData: {
				...workerData,
				benchmarkWorker: true
			},
		}
	);
}

function withResolvers<T>() {
	let resolve, reject;
	const promise = new Promise<T>((res, rej) => {
		resolve = res;
		reject = rej;
	});
	return {
		resolve,
		reject,
		promise
	};
}
