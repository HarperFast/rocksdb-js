import { dbRunner } from '../test/lib/util.js';
import { createWorkerBootstrapScript } from '../test/lib/util.js';
import { stressTest } from './setup.js';
import { Worker } from 'node:worker_threads';
import { describe } from 'vitest';

// Node.js 18 and older doesn't properly eval ESM code
const bootstrapScript = createWorkerBootstrapScript(
	'./stress-test/workers/stress-transaction-put-worker.mts'
);

describe('Stress Transactions', () => {
	stressTest(
		'should create 30 worker threads and commit 10k transactions',
		{ mode: 'essential' },
		() =>
			dbRunner({ skipOpen: true }, async ({ dbPath }) => {
				const promises: Promise<void>[] = [];
				const workers: Worker[] = [];

				for (let i = 0; i < 30; i++) {
					const worker = new Worker(bootstrapScript, {
						eval: true,
						workerData: { path: dbPath, iterations: 10_000 },
					});
					workers.push(worker);

					promises.push(
						new Promise<void>((resolve, reject) => {
							worker.on('error', reject);
							worker.on('message', (event) => {
								if (event.done) {
									resolve();
								}
							});
						})
					);

					worker.postMessage({ runTransactions10k: true });
				}

				await Promise.all(promises);
			})
	);

	stressTest(
		'should create 30 worker threads and commit 10k transactions with logs and random entry sizes',
		{ mode: 'essential' },
		() =>
			dbRunner({ skipOpen: true }, async ({ dbPath }) => {
				const promises: Promise<void>[] = [];
				const workers: Worker[] = [];

				for (let i = 0; i < 30; i++) {
					const worker = new Worker(bootstrapScript, {
						eval: true,
						workerData: { path: dbPath, iterations: 1_000 },
					});
					workers.push(worker);

					promises.push(
						new Promise<void>((resolve, reject) => {
							worker.on('error', reject);
							worker.on('message', (event) => {
								if (event.done) {
									resolve();
								}
							});
						})
					);

					worker.postMessage({ runTransactions10kWithLogs: true });
				}

				await Promise.all(promises);
			})
	);
});
