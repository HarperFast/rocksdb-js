import { RocksDatabase, shutdown } from '../dist/index.mjs';
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { parseArgs } from 'node:util';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

const { values } = parseArgs({
	options: {
		workers: { type: 'string', default: '4' },
		seconds: { type: 'string', default: '10' },
		concurrency: { type: 'string', default: '8' },
		mode: { type: 'string', default: 'log' },
	},
});
const workers = Number(values.workers);
const seconds = Number(values.seconds);
const concurrency = Number(values.concurrency);
const mode = values.mode;
for (const [name, value] of Object.entries({ workers, seconds, concurrency })) {
	assert(Number.isSafeInteger(value) && value > 0, `${name} must be a positive integer`);
}
assert(mode === 'log' || mode === 'put', 'mode must be log or put');

type Result = { done: true; count: number };

if (isMainThread) {
	mkdirSync(resolve('benchmark/data'), { recursive: true });
	const path = mkdtempSync(resolve('benchmark/data/mutex-contention-'));
	const db = RocksDatabase.open(path, { disableWAL: true });
	const states: Array<{ worker: Worker; ready: Promise<void>; done: Promise<Result> }> = [];
	const deadline = setTimeout(
		() => {
			console.error('Contention benchmark timed out');
			process.exit(1);
		},
		(seconds + 60) * 1000
	);
	try {
		for (let id = 0; id < workers; id++) {
			const worker = new Worker(new URL(import.meta.url), {
				workerData: { path, id, seconds, concurrency, mode },
			});
			const ready = Promise.withResolvers<void>();
			const done = Promise.withResolvers<Result>();
			let completed = false;
			const reject = (error: Error) => {
				ready.reject(error);
				done.reject(error);
			};
			void done.promise.catch(() => {});
			worker.on('error', reject);
			worker.on('exit', (code) => {
				if (!completed) reject(new Error(`Worker ${id} exited before completion (${code})`));
			});
			worker.on('message', (message: { ready?: boolean } & Partial<Result>) => {
				if (message.ready) ready.resolve();
				if (message.done) {
					completed = true;
					done.resolve(message as Result);
				}
			});
			states.push({ worker, ready: ready.promise, done: done.promise });
		}
		await Promise.all(states.map((state) => state.ready));
		const cpuStart = process.cpuUsage();
		const start = performance.now();
		for (const state of states) state.worker.postMessage('go');
		const results = await Promise.all(states.map((state) => state.done));
		const elapsed = (performance.now() - start) / 1000;
		const cpu = process.cpuUsage(cpuStart);
		const count = results.reduce((sum, result) => sum + result.count, 0);
		if (mode === 'log') {
			assert.equal(db.useLog('shared').getStats().totals.transactionsWritten, count);
		} else {
			for (let id = 0; id < workers; id++) {
				for (let lane = 0; lane < concurrency; lane++) {
					assert.deepEqual(db.getSync(`${id}-${lane}`), Buffer.alloc(100, id));
				}
			}
		}
		console.log(
			JSON.stringify({
				workers,
				concurrency,
				seconds,
				mode,
				count,
				elapsed,
				opsPerSecond: count / elapsed,
				cpu,
			})
		);
	} finally {
		await Promise.all(states.map((state) => state.worker.terminate()));
		db.close();
		shutdown();
		rmSync(path, { recursive: true, force: true });
		clearTimeout(deadline);
	}
} else {
	const { path, id, seconds, concurrency, mode } = workerData;
	const db = RocksDatabase.open(path, { disableWAL: true });
	const log = mode === 'log' ? db.useLog('shared') : undefined;
	const data = Buffer.alloc(100, id);
	parentPort!.once('message', async () => {
		const end = performance.now() + seconds * 1000;
		const counts = await Promise.all(
			Array.from({ length: concurrency }, async (_, lane) => {
				let count = 0;
				while (performance.now() < end) {
					await db.transaction((txn) => {
						if (log) log.addEntry(data, txn.id);
						else txn.putSync(`${id}-${lane}`, data);
					});
					count++;
				}
				return count;
			})
		);
		parentPort!.postMessage({ done: true, count: counts.reduce((sum, count) => sum + count, 0) });
	});
	parentPort!.postMessage({ ready: true });
}
