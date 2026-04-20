import { registryStatus } from '../src/index.js';
import { dbRunner } from '../test/lib/util.js';
import { createWorkerBootstrapScript } from '../test/lib/util.js';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';
import { describe, expect, it } from 'vitest';

// Node.js 18 and older doesn't properly eval ESM code
const bootstrapScript = createWorkerBootstrapScript(
	'./stress-test/workers/stress-db-instances-worker.mts'
);

describe('Stress DB Instances', () => {
	it.skipIf(!globalThis.gc)(
		'should create 10 worker threads and each open 500 databases with 25 column families',
		() =>
			dbRunner(async ({ dbPath }) => {
				const promises: Promise<void>[] = [];
				const workers: Worker[] = [];

				const [initial] = registryStatus();

				const workerThreads = 10;
				const dbInstances = 500;
				const numColumnFamilies = 25;

				for (let i = 0; i < workerThreads; i++) {
					const worker = new Worker(bootstrapScript, {
						eval: true,
						workerData: { path: dbPath, dbInstances, numColumnFamilies },
					});
					workers.push(worker);

					promises.push(
						new Promise<void>((resolve, reject) => {
							worker.on('error', reject);
							worker.on('message', (event) => {
								if (event.done) {
									resolve();
								} else if (event.closed) {
									resolve();
								}
							});
						})
					);
				}

				await Promise.all(promises);
				promises.length = 0;

				const [before] = registryStatus();

				for (const worker of workers) {
					worker.postMessage({ close: true });
				}
				await Promise.all(promises);

				if (globalThis.gc) {
					globalThis.gc();
					await delay(100);
					// Second gc() to finalize external buffers (napi_create_external_buffer
					// C++ destructors may be deferred past the first gc cycle on some Node versions)
					globalThis.gc();
					await delay(50);
				}

				const [after] = registryStatus();

				// +1 for the default column family
				expect(Object.keys(before?.columnFamilies).length).toBe(numColumnFamilies + 1);
				expect(initial.refCount).toBe(after.refCount);
			})
	);
});
