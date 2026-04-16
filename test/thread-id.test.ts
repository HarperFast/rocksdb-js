import { currentThreadId } from '../src/index.js';
import { createWorkerBootstrapScript } from './lib/util.js';
import { Worker } from 'node:worker_threads';
import { describe, expect, it } from 'vitest';

describe('threadId', () => {
	it('should return the current thread id', () => {
		expect(currentThreadId()).toBeTypeOf('number');
	});

	it('should return a different thread id for each thread', async () => {
		const threadIds = new Set<number>();

		for (let i = 0; i < 10; i++) {
			const worker = new Worker(
				createWorkerBootstrapScript('./test/workers/thread-id-worker.mts'),
				{
					eval: true,
				}
			);

			const workerThreadId = await new Promise<number>((resolve) => {
				worker.on('message', (event) => {
					resolve(event.threadId);
				});
			});
			expect(workerThreadId).toBeTypeOf('number');
			expect(workerThreadId).not.toBe(currentThreadId());

			expect(threadIds.has(workerThreadId)).toBe(false);
			threadIds.add(workerThreadId);
		}
	});
});
