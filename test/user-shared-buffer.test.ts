import { withResolvers } from '../src/util.js';
import { createWorkerBootstrapScript, dbRunner, terminateWorker } from './lib/util.js';
import { generateDBPath } from './lib/util.js';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { Worker } from 'node:worker_threads';
import { describe, expect, it } from 'vitest';

describe('User Shared Buffer', () => {
	describe('getUserSharedBuffer()', () => {
		it('should create a new user shared buffer', () =>
			dbRunner(async ({ db }) => {
				const defaultIncrementer = new BigInt64Array(1);
				const sharedBuffer1 = db.getUserSharedBuffer('incrementer-test', defaultIncrementer.buffer);
				expect(sharedBuffer1).toBeInstanceOf(ArrayBuffer);

				const incrementer = new BigInt64Array(sharedBuffer1);
				incrementer[0] = 4n;
				expect(Atomics.add(incrementer, 0, 1n)).toBe(4n);

				const secondDefaultIncrementer = new BigInt64Array(1); // should not get used
				const sharedBuffer2 = db.getUserSharedBuffer(
					'incrementer-test',
					secondDefaultIncrementer.buffer
				);
				expect(sharedBuffer2).toBeInstanceOf(ArrayBuffer);

				const nextIncrementer = new BigInt64Array(sharedBuffer2); // should return same incrementer
				expect(incrementer[0]).toBe(5n);
				expect(Atomics.add(nextIncrementer, 0, 1n)).toBe(5n);
				expect(incrementer[0]).toBe(6n);
				expect(secondDefaultIncrementer[0]).toBe(0n);
			}));

		it('should get next id using shared buffer', () =>
			dbRunner(async ({ db }) => {
				const incrementer = new BigInt64Array(
					db.getUserSharedBuffer('next-id', new BigInt64Array(1).buffer)
				);
				incrementer[0] = 1n;

				const getNextId = () => Atomics.add(incrementer, 0, 1n);

				expect(getNextId()).toBe(1n);
				expect(getNextId()).toBe(2n);
				expect(getNextId()).toBe(3n);
			}));

		it('should have separate buffers for each database instance', () =>
			dbRunner({ dbOptions: [{ name: 'one' }, { name: 'two' }] }, async ({ db }, { db: db2 }) => {
				const incrementer = new BigInt64Array(
					db.getUserSharedBuffer('next-id', new BigInt64Array(1).buffer)
				);
				incrementer[0] = 1n;
				const incrementer2 = new BigInt64Array(
					db2.getUserSharedBuffer('next-id', new BigInt64Array(1).buffer)
				);
				incrementer2[0] = 2n;

				expect(incrementer[0]).toBe(1n);
				expect(incrementer2[0]).toBe(2n);
			}));

		it(
			'should share buffer across worker threads with same database',
			() =>
				dbRunner(async ({ db, dbPath }) => {
					const incrementer = new BigInt64Array(
						db.getUserSharedBuffer('next-id-worker', new BigInt64Array(1).buffer)
					);
					incrementer[0] = 1n;

					const getNextId = () => Atomics.add(incrementer, 0, 1n);

					const worker = new Worker(
						createWorkerBootstrapScript('./test/workers/user-shared-buffer-worker.mts'),
						{ eval: true, workerData: { path: dbPath } }
					);

					let resolver = withResolvers<void>();

					await new Promise<void>((resolve, reject) => {
						worker.on('error', reject);
						worker.on('message', (event) => {
							try {
								if (event.started) {
									resolve();
								} else if (event.nextId) {
									resolver.resolve(event.nextId);
								}
							} catch (error) {
								reject(error);
							}
						});
						worker.on('exit', () => resolver.resolve());
					});

					expect(getNextId()).toBe(1n);
					expect(getNextId()).toBe(2n);

					worker.postMessage({ increment: true });
					await expect(resolver.promise).resolves.toBe(3n);
					expect(getNextId()).toBe(4n);

					resolver = withResolvers<void>();
					worker.postMessage({ close: true });
					await terminateWorker(worker);
					await resolver.promise;

					expect(getNextId()).toBe(5n);
				}),
			10000
		);

		it('should throw an error if the default buffer is not an ArrayBuffer', () =>
			dbRunner(async ({ db }) => {
				expect(() => db.getUserSharedBuffer('incrementer-test', undefined as any)).toThrow(
					'Default buffer must be an ArrayBuffer'
				);
				expect(() => db.getUserSharedBuffer('incrementer-test', 'hello' as any)).toThrow(
					'Default buffer must be an ArrayBuffer'
				);
			}));

		it('should error if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				expect(() => db.getUserSharedBuffer('foo', new ArrayBuffer(1))).toThrow(
					'Database not open'
				);
			}));

		it('should error if options are invalid', () =>
			dbRunner(async ({ db }) => {
				expect(() => db.getUserSharedBuffer('foo', new ArrayBuffer(1), 'foo' as any)).toThrow(
					'Options must be an object'
				);
			}));

		it('should error if callback is not a function', () =>
			dbRunner(async ({ db }) => {
				expect(() =>
					db.getUserSharedBuffer('foo', new ArrayBuffer(1), { callback: 123 as any })
				).toThrow('Callback must be a function');
			}));

		it('should not crash if you close while holding a reference to the buffer', () =>
			dbRunner(async ({ db }) => {
				const sharedBuffer = db.getUserSharedBuffer('foo', new ArrayBuffer(1));
				const view = new Uint8Array(sharedBuffer);
				view[0] = 0xab;
				db.close();
				// reading and writing the retained ArrayBuffer must not crash
				// after close (regression guard for the user-shared-buffer UAF)
				expect(view[0]).toBe(0xab);
				view[0] = 0xcd;
				expect(view[0]).toBe(0xcd);
				expect(() => sharedBuffer.notify()).toThrow('Database not open');
			}));
	});

	describe('Descriptor teardown races', () => {
		it('should survive advancing a range iterator while the last handles close (main + worker)', () =>
			expectReproSurvives('stale-iterator'));
	});

	/**
	 * Regression tests for shutdown races on process-wide registry state.
	 *
	 * These intentionally run in a child process because a failing build
	 * usually SIGSEGV/SIGTRAPs rather than throwing a catchable JS error. Each
	 * mode is also run multiple times because the underlying race only
	 * surfaces on a fraction of attempts, and a single shot can mask
	 * regressions that reproduce 30-50% of the time.
	 */
	describe('User shared buffer use-after-free repro (child process)', () => {
		it('should survive retained ArrayBuffer access after the last DB handle closes (main + worker)', () =>
			expectReproSurvives('buffer'));

		it('should survive retained ArrayBuffer access while handles are closing (main + worker)', () =>
			expectReproSurvives('buffer-race'));

		it('should survive shared buffer use while another worker repeatedly opens/closes the same CF (schema-sync)', () =>
			expectReproSurvives('schema-sync'));
	});
});

const fixturePath = join(__dirname, 'fixtures', 'fork-user-shared-buffer-uaf.mts');

type ReproMode = 'buffer' | 'buffer-race' | 'stale-iterator' | 'schema-sync';

/**
 * Runs the repro fixture in a child process N times and asserts each run
 * exited cleanly. The race only surfaces on a fraction of attempts, so a
 * single shot can mask a 30-50% flake regression — looping gives CI a
 * usefully high detection rate while keeping wall time bounded (~1s/mode).
 */
async function expectReproSurvives(mode: ReproMode, iterations = 3): Promise<void> {
	for (let i = 0; i < iterations; i++) {
		const dbPath = generateDBPath();
		const { code, signal } = await spawnRepro(dbPath, mode);
		expect(signal, `mode=${mode} iteration=${i}`).toBeNull();
		expect(code, `mode=${mode} iteration=${i}`).toBe(0);
	}
}

function spawnRepro(
	dbPath: string,
	mode: ReproMode
): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
	return new Promise((resolve, reject) => {
		const args =
			process.versions.bun || process.versions.deno
				? [fixturePath, dbPath, mode]
				: ['node_modules/tsx/dist/cli.mjs', fixturePath, dbPath, mode];

		const child = spawn(process.execPath, args, {
			env: { ...process.env },
		});

		let stderr = '';
		child.stderr?.on('data', (chunk) => {
			stderr += chunk.toString();
		});

		child.on('close', (code, signal) => {
			if (code !== 0 || signal) {
				console.error(`Repro child (${mode}) stderr:\n${stderr}`);
			}
			resolve({ code, signal });
		});
		child.on('error', reject);
	});
}
