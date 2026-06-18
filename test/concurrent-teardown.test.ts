import { createWorkerBootstrapScript, generateDBPath } from './lib/util.js';
import { rmSync } from 'node:fs';
import { Worker } from 'node:worker_threads';
import { describe, expect, it } from 'vitest';

/**
 * Regression for the shared-DBDescriptor use-after-free in
 * `DBRegistry::CloseDB`. Worker threads that open the SAME path share one
 * process-global `DBDescriptor`; tearing several of them down concurrently
 * raced two `CloseDB` calls into `close()`ing a freed descriptor (locking its
 * destroyed mutex), which surfaced on Linux/glibc as
 * "malloc(): unaligned tcache chunk detected" and crashed the benchmark suite.
 *
 * Each round spawns many same-path workers and asserts they all exit cleanly. A
 * regression makes a worker abort (non-zero exit) or takes down the whole
 * process; under the ASan build (ROCKSDB_ASAN=1, see benchmark-asan.yml) it
 * additionally faults at the offending access.
 */
describe('Concurrent cross-thread teardown', () => {
	it('opens one database from many workers and tears down without corruption', async () => {
		const ROUNDS = 12;
		const WORKERS = 8;

		for (let round = 0; round < ROUNDS; round++) {
			const path = generateDBPath();

			const exitCodes = await Promise.all(
				Array.from(
					{ length: WORKERS },
					(_unused, i) =>
						new Promise<number>((resolve, reject) => {
							const worker = new Worker(
								createWorkerBootstrapScript('./test/workers/concurrent-teardown-worker.mts'),
								{
									eval: true,
									workerData: { path, puts: 150, close: i % 2 === 0 },
								}
							);
							worker.on('error', reject);
							worker.on('exit', (code) => resolve(code));
						})
				)
			);

			expect(exitCodes).toEqual(Array.from({ length: WORKERS }, () => 0));
			rmSync(path, { recursive: true, force: true });
		}
	}, 60_000);
});
