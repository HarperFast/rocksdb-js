import { RocksDatabase, RocksDatabaseOptions } from '../dist/index.mjs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import * as lmdb from 'lmdb';
import { randomBytes } from 'node:crypto';
import { parentPort, Worker, workerData } from 'node:worker_threads';
import { setImmediate as rest } from 'node:timers/promises';
import { rmSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
const isWindows = process.platform === 'win32';

const vitestBench = workerData?.benchmarkWorkerId ? () => {
	throw new Error('Workers should not be directly calling vitest\'s bench()');
} : (await import('vitest')).bench;

export type LMDBDatabase = lmdb.RootDatabase<any, string> & { path: string };

export interface BenchmarkContext<T> extends Record<string, any> {
	db: T;
};

type BenchmarkOptions<T, U> = {
	bench: (ctx: BenchmarkContext<T>) => void | Promise<void>,
	dbOptions?: U,
	name?: string,
	setup?: (ctx: BenchmarkContext<T>) => void | Promise<void>,
	timeout?: number,
	teardown?: (ctx: BenchmarkContext<T>) => void | Promise<void>,
	mode?: 'essential' | 'full'
};

export function benchmark(type: 'rocksdb', options: BenchmarkOptions<RocksDatabase, RocksDatabaseOptions>): void;
export function benchmark(type: 'lmdb', options: BenchmarkOptions<LMDBDatabase, lmdb.RootDatabaseOptions>): void;
export function benchmark(type: string, options: any): void {
	if (type !== 'rocksdb' && type !== 'lmdb') {
		throw new Error(`Unsupported benchmark type: ${type}`);
	}

	if ((process.env.ROCKSDB_ONLY && type !== 'rocksdb') || (process.env.LMDB_ONLY && type !== 'lmdb') ||
		(process.env.BENCHMARK_MODE && process.env.BENCHMARK_MODE !== 'full' && options.mode !== process.env.BENCHMARK_MODE)) {
		return;
	}

	const { bench, setup, teardown, dbOptions, name } = options;
	// it is important to run benchmarks on a real filesystem (not a tempfs)
	const dbPath = join('benchmark', 'data', `rocksdb-benchmark-${randomBytes(8).toString('hex')}`);
	let ctx: BenchmarkContext<any>;

	vitestBench(name || type, () => {
		let timer: NodeJS.Timeout | undefined;
		return Promise.race([
			(async () => {
				await bench(ctx);
				if (timer) {
					clearTimeout(timer);
				}
			})(),
			new Promise<void>((_resolve, reject) => {
				timer = setTimeout(() => {
					reject(new Error('Benchmark timed out'));
				}, options.timeout || 60_000);
			})
		]);
	}, {
		throws: true,
		async setup(task, mode: 'warmup' | 'run') {
			if (mode === 'warmup') { // the first setup, create the database
				if (type === 'rocksdb') {
					ctx = { db: RocksDatabase.open(dbPath, dbOptions), mode };
				} else {
					ctx = { db: lmdb.open({ path: dbPath, compression: true, ...dbOptions }), mode };
				}
			}
			if (typeof setup === 'function') {
				await setup(ctx, task, mode);
			}
		},
		async teardown(task, mode: 'warmup' | 'run') {
			if (typeof teardown === 'function') {
				await teardown(ctx, task, mode);
			}
			if (ctx.db && mode === 'run') { // only teardown after the real run
				const path = ctx.db.path;
				ctx.db.close();
				try {
					rmSync(path, { force: true, recursive: true, maxRetries: 3 });
				} catch (err) {
					console.warn(`Benchmark teardown failed to delete db path: ${err}`);
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
 *
 * To write a benchmark that uses workers, you need to use the exported
 * `workerDescribe()` and `workerBenchmark()` functions.
 *
 * ```ts
 * import {
 *   workerDescribe as describe,
 *   workerBenchmark as benchmark
 * } from './setup.ts';
 *
 * describe('My suite', () => {
 *   benchmark('some title', {
 *     numWorkers: 2,
 *     setup(ctx) {
 *       ctx.data = generateRandomKeys(100);
 *       for (const key of ctx.data) {
 *         ctx.db.putSync(key, 'test-value');
 *       }
 *     },
 * 	   bench({ db, data }) {
 *       for (const key of data) {
 *         db.getSync(key);
 *       }
 *     }
 *   });
 * });
 *
 * On the main thread, `workerDescribe()` wraps `vitest.describe()` and records
 * all nested `describe()` calls and groups them into suites.
 * `workerBenchmark()` wraps our `vitestBench()` function and also records all
 * benchmarks for the current suite.
 *
 * Note that Vitest can only be run on the main thread. That's why this file
 * carefully lazy loads Vitest only on the main thread.
 *
 * When Vitest runs a benchmark, `setup()` on the main thread will create the
 * worker threads and wait for them to initialize. Each worker thread needs
 * to discover all `describe()` and `benchmark()` calls. The worker will also
 * open the database. Once initialized, the worker notifies the main thread and
 * signals Vitest to call `bench()`.
 *
 * Vitest will call `bench()` on the main thread which tells the worker to run
 * the benchmark. When the worker finishes, it notifies the main thread and the
 * benchmark completion promise is reset.
 *
 * Lastly Vitest calls `teardown()` on the main thread which tells the worker to
 * close the database and gracefully exit.
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
	exitPromise: ReturnType<typeof withResolvers<void>>;
	teardownPromise: ReturnType<typeof withResolvers<void>>;
}

interface WorkerBenchmarkOptions extends BenchmarkOptions<any, any> {
	numWorkers?: number;
}

const workerSuites: Record<string, WorkerSuite> = {};
let workerCurrentSuites: WorkerSuite[] = [];

/**
 * Runs on the main thread and the worker thread. It discovers nested
 * `describe()` calls and groups them into suites.
 */
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

/**
 * This is the main `workerDescribe()` function that is exported. It has two
 * code paths:
 *
 * 1. The main thread, which does discovery and calls `vitest.describe()`
 * 2. The worker thread, which does discovery only
 */
export const workerDescribe = await (async () => {
	if (workerData?.benchmarkWorkerId) {
		return Object.assign(describeShim, {
			only(name: string, fn: () => void) {
				describeShim(name, fn);
			},
			skip() {
				throw new Error('skip not supported in worker');
			},
			todo() {
				throw new Error('todo not supported in worker');
			}
		});
	}

	// main thread
	const { describe } = await import('vitest');
	return Object.assign((name: string, fn: () => void) => {
		describeShim(name, () => {
			// snapshot the worker current suites in the closure because vitest
			// fires callbacks once all describes()'s have been discovered
			const state = [...workerCurrentSuites];
			describe(name, () => {
				// now that all describes()'s have been discovered,
				// `workerCurrentSuites` can be clobbered
				workerCurrentSuites = state;
				fn();
			});
		});
	}, {
		only(name: string, fn: () => void) {
			describeShim(name, () => {
				const state = [...workerCurrentSuites];
				describe.only(name, () => {
					workerCurrentSuites = state;
					fn();
				});
			});
		},
		skip: describe.skip,
		todo: describe.todo,
	});
})();

/**
 * Defines a benchmark. This is called on both the main thread and the worker.
 * Both paths record the benchmark in the current suite.
 *
 * The main thread continues by registering the benchmark with Vitest. Note that
 * the benchmark `setup()`, `bench()`, and `teardown()` functions are not called
 * on the main thread. Instead, Vitest is wired up to send messages to the worker
 * to call the benchmark functions.
 */
export function workerBenchmark(type: 'rocksdb', options: WorkerBenchmarkOptions): void;
export function workerBenchmark(type: 'lmdb', options: WorkerBenchmarkOptions): void;
export function workerBenchmark(type: string, options: any): void {
	if (type !== 'rocksdb' && type !== 'lmdb') {
		throw new Error(`Unsupported benchmark type: ${type}`);
	}

	if ((process.env.ROCKSDB_ONLY && type !== 'rocksdb') || (process.env.LMDB_ONLY && type !== 'lmdb') ||
		(process.env.BENCHMARK_MODE && process.env.BENCHMARK_MODE !== 'full' && options.mode !== process.env.BENCHMARK_MODE)) {
		return;
	}

	// get the current benchmark file
	const originalPrepareStackTrace = Error.prepareStackTrace;
	Error.prepareStackTrace = (_, stack) => stack;
	const stack = new Error().stack as any;
	Error.prepareStackTrace = originalPrepareStackTrace;
	const benchmarkFile = stack.map(frame => frame.getFileName()).find(frame => frame.endsWith('.bench.ts'));

	let { bench, dbOptions, name, numWorkers, setup, teardown } = options;
	numWorkers = Math.max(numWorkers || 1, 1);
	const benchmarkName = name || type;
	const suite = workerCurrentSuites[workerCurrentSuites.length - 1];
	if (suite) {
		suite.benchmarks.push({ bench, dbOptions, name: benchmarkName, numWorkers, setup, teardown, type });
	}

	if (workerData?.benchmarkWorkerId) {
		// worker thread only needs to discover the benchmarks
		return;
	}

	const workerState: WorkerState[] = [];
	const workerPayload = {
		suites: workerCurrentSuites.map(suite => suite.name),
		benchmark: benchmarkName
	};

	vitestBench(benchmarkName, async () => {
		await Promise.all(Array.from({ length: numWorkers }, (_, i) => {
			const state = workerState[i];
			state.worker.postMessage({ bench: true });
			return state.benchPromise.promise;
		}));

		for (let i = 0; i < numWorkers; i++) {
			workerState[i].benchPromise = withResolvers<void>();
		}
	}, {
		throws: true,
		async setup(_task, mode) {
			if (mode === 'run') return;
			const path = join(tmpdir(), `rocksdb-benchmark-${randomBytes(8).toString('hex')}`);

			// launch all workers and wait for them to initialize
			await Promise.all(Array.from({ length: numWorkers }, (_, i) => {
				return new Promise<void>((resolve, reject) => {
					const worker = workerLaunch({
						...workerPayload,
						benchmarkFile: pathToFileURL(benchmarkFile).toString(),
						benchmarkWorkerId: i + 1,
						mode,
						path
					});
					// important! these promises need to be referenced as
					// properties of `state` because they are reset by reference
					const state = {
						worker,
						benchPromise: withResolvers<void>(),
						exitPromise: withResolvers<void>(),
						teardownPromise: withResolvers<void>()
					};
					workerState[i] = state;
					worker.on('error', reject);
					worker.on('exit', () => {
						state.benchPromise.resolve();
						state.teardownPromise.resolve();
						state.exitPromise.resolve();
					});
					worker.on('message', event => {
						// console.log('parent:', event);
						if (event.setupDone) {
							resolve();
						} else if (event.benchDone) {
							state.benchPromise.resolve();
						} else if (event.teardownDone) {
							state.teardownPromise.resolve();
						} else if (event.timeout) {
							state.teardownPromise.resolve();
							state.benchPromise.reject(new Error('Benchmark timed out'));
						}
					});
				});
			}));
		},
		async teardown(_task, mode) {
			if (mode === 'warmup') return;
			// tell all workers to teardown and wait
			await Promise.all(Array.from({ length: numWorkers }, (_, i) => {
				const state = workerState[i];
				state.worker.postMessage({ teardown: true, mode });
				return state.teardownPromise.promise;
			}));

			// wait for all workers to exit
			await Promise.all(Array.from({ length: numWorkers }, (_, i) => {
				workerState[i].teardownPromise = withResolvers<void>();
				return workerState[i].exitPromise.promise;
			}));
		}
	});
}

/**
 * Runs on the worker thread, opens the database and wires up the message
 * listeners.
 */
export async function workerInit() {
	if (!parentPort) {
		throw new Error('Failed to initialize worker: parentPort is not available');
	}

	const { benchmarkWorkerId, path, timeout } = workerData;
	let ctx: BenchmarkContext<any>;
	const { bench, dbOptions, setup, teardown, type } = workerFindBenchmark();

	// console.log('workerInit', workerData.benchmarkWorkerId, workerData.mode, type, path);

	parentPort.on('message', async (event: any) => {
		// console.log('worker:', event);
		if (event.bench) {
			const timer = setTimeout(() => {
				parentPort!.postMessage({ timeout: true, benchmarkWorkerId });
				process.exit(1);
			}, timeout || 60_000); // 1 minute
			await bench(ctx);
			clearTimeout(timer);
			parentPort!.postMessage({ benchDone: true, benchmarkWorkerId });
		} else if (event.teardown) {
			if (typeof teardown === 'function') {
				await teardown(ctx);
			}
			if (ctx.db) {
				// console.log('workerTeardown', workerData.benchmarkWorkerId, workerData.mode, type, path);
				ctx.db.close();
				try {
					rmSync(path, { force: true, recursive: true, maxRetries: 3 });
				} catch (err) {
					console.warn(`Benchmark teardown failed to delete db path: ${err}`);
				}
			}
			parentPort!.postMessage({ teardownDone: true, benchmarkWorkerId });
			process.exit(0);
		}
	});

	if (type === 'rocksdb') {
		ctx = { db: RocksDatabase.open(path, dbOptions) };
	} else {
		ctx = { db: lmdb.open({ path, ...dbOptions }) };
	}
	if (typeof setup === 'function') {
		await setup(ctx);
	}
	parentPort.postMessage({ setupDone: true, benchmarkWorkerId });
}

/**
 * Runs on the worker thread and attempts to find the requested benchmark.
 */
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

/**
 * Runs on the main thread and launches a worker thread.
 */
export function workerLaunch(workerData: Record<string, any> = {}) {
	// Node.js 18 and older doesn't properly eval ESM code
	const majorVersion = parseInt(process.versions.node.split('.')[0]);
	const script = majorVersion < 20
		?	`
			const tsx = require('tsx/cjs/api');
			tsx.require(${JSON.stringify(workerData.benchmarkFile)}, __dirname);
			const { workerInit } = tsx.require('./benchmark/setup.ts', __dirname);
			workerInit();
			`
		:	`
			import { register } from 'tsx/esm/api';
			register();
			import(${JSON.stringify(workerData.benchmarkFile)})
				.then(() => import('./benchmark/setup.ts'))
				.then(module => module.workerInit());
			`;

	return new Worker(
		script,
		{
			eval: true,
			workerData,
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

export function concurrent(suite: BenchmarkOptions<RocksDatabase, RocksDatabaseOptions> & HasConcurrencyOptions): BenchmarkOptions<RocksDatabase, RocksDatabaseOptions>;
export function concurrent(suite: BenchmarkOptions<LMDBDatabase, lmdb.RootDatabaseOptions> & HasConcurrencyOptions): BenchmarkOptions<LMDBDatabase, lmdb.RootDatabaseOptions>;
export function concurrent<T, U, S extends BenchmarkOptions<T, U>>(suite: S & HasConcurrencyOptions): S {
	let index = 0;
	const concurrencyMaximum = suite.concurrencyMaximum ?? 8;
	let currentlyExecuting: Promise<void>[] = Array(concurrencyMaximum);
	const restEachTurn = suite.restEachTurn ?? true;
	return {
		...suite,
		async bench(ctx: BenchmarkContext<T>) {
			await currentlyExecuting[index]; // await the previous execution for this slot
			currentlyExecuting[index] = suite.bench(ctx) as Promise<void>; // let it execute concurrently and resolve after concurrencyMaximum number of executions
			if (isWindows) { // don't do any concurrency on windows
				return currentlyExecuting[index];
			}
			index = (index + 1) % concurrencyMaximum; // cycle in a loop
			if (restEachTurn) { // let asynchronous actions have turns in the event queue, more realistic for most scenarios
				await rest();
			}
		},
		async teardown(ctx: BenchmarkContext<T>) {
			// wait for all outstanding executions to finish
			await Promise.all(currentlyExecuting);
			// any more tearing down
			return suite.teardown?.(ctx);
		}
	}
}
type HasConcurrencyOptions = { concurrencyMaximum?: number, numWorkers?: number, restEachTurn?: boolean };
