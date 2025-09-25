import { describe, it } from 'vitest';
import { Worker } from 'node:worker_threads';
import { dbRunner } from '../test/lib/util.js';

describe('Stress Transactions', () => {
	it('should create 100 worker threads and commit 10k transactions', () => dbRunner({
		skipOpen: true
	}, async ({ dbPath }) => {
		// Node.js 18 and older doesn't properly eval ESM code
		const majorVersion = parseInt(process.versions.node.split('.')[0]);
		const script = process.versions.deno || process.versions.bun
			?	`
				import { pathToFileURL } from 'node:url';
				import(pathToFileURL('./stress-test/fixtures/stress-transaction-put-worker.mts'));
				`
			:	majorVersion < 20
				?	`
					const tsx = require('tsx/cjs/api');
					tsx.require('./stress-test/fixtures/stress-transaction-put-worker.mts', __dirname);
					`
				:	`
					import { register } from 'tsx/esm/api';
					register();
					import('./stress-test/fixtures/stress-transaction-put-worker.mts');
					`;

		const promises: Promise<void>[] = [];
		const workers: Worker[] = [];

		for (let i = 0; i < 100; i++) {
			const worker = new Worker(
				script,
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
		}

		await Promise.all(promises);
	}));
});
