import { describe, it } from 'vitest';
import { Worker } from 'node:worker_threads';
import { dbRunner } from '../test/lib/util.js';

// Node.js 18 and older doesn't properly eval ESM code
const majorVersion = parseInt(process.versions.node.split('.')[0]);
const bootstrapScript = process.versions.deno || process.versions.bun
	?	`
		import { pathToFileURL } from 'node:url';
		import(pathToFileURL('./stress-test/workers/stress-transaction-put-worker.mts'));
		`
	:	majorVersion < 20
		?	`
			const tsx = require('tsx/cjs/api');
			tsx.require('./stress-test/workers/stress-transaction-put-worker.mts', __dirname);
			`
		:	`
			import { register } from 'tsx/esm/api';
			register();
			import('./stress-test/workers/stress-transaction-put-worker.mts');
			`;

describe('Stress Transactions', () => {
	it('should create 30 worker threads and commit 10k transactions', () => dbRunner({
		skipOpen: true
	}, async ({ dbPath }) => {
		const promises: Promise<void>[] = [];
		const workers: Worker[] = [];

		for (let i = 0; i < 30; i++) {
			const worker = new Worker(
				bootstrapScript,
				{
					eval: true,
					workerData: {
						path: dbPath,
						iterations: 10_000,
					}
				}
			);
			workers.push(worker);

			promises.push(new Promise<void>((resolve, reject) => {
				worker.on('error', reject);
				worker.on('message', event => {
					if (event.done) {
						resolve();
					}
				});
			}));

			worker.postMessage({ runTransactions10k: true });
		}

		await Promise.all(promises);
	}));

	it('should create 30 worker threads and commit 10k transactions with logs and random entry sizes', () => dbRunner({
		skipOpen: true
	}, async ({ dbPath }) => {
		const promises: Promise<void>[] = [];
		const workers: Worker[] = [];

		for (let i = 0; i < 30; i++) {
			const worker = new Worker(
				bootstrapScript,
				{
					eval: true,
					workerData: {
						path: dbPath,
						iterations: 1_000,
					}
				}
			);
			workers.push(worker);

			promises.push(new Promise<void>((resolve, reject) => {
				worker.on('error', reject);
				worker.on('message', event => {
					if (event.done) {
						resolve();
					}
				});
			}));

			worker.postMessage({ runTransactions10kWithLogs: true });
		}

		await Promise.all(promises);
	}));
});
