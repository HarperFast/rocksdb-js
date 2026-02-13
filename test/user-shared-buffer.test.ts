import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';
import { assert, describe, expect, it } from 'vitest';
import type { BufferWithDataView } from '../src/encoding.js';
import { withResolvers } from '../src/util.js';
import { createWorkerBootstrapScript, dbRunner } from './lib/util.js';

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
			dbRunner(
				{ dbOptions: [{ name: 'one' }, { name: 'two' }] },
				async ({ db }, { db: db2 }) => {
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

		it('should notify callbacks', () =>
			dbRunner(async ({ db }) => {
				const sharedNumber = new Float64Array(1);
				await new Promise<void>(resolve => {
					const sharedBuffer = db.getUserSharedBuffer('with-callback', sharedNumber.buffer, {
						callback() {
							// wait so notify() returns true
							setTimeout(() => resolve(), 100);
						},
					});
					expect(sharedBuffer.notify()).toBe(true);
				});
			}));

		(globalThis.gc ? it : it.skip)(
			'should cleanup callbacks on GC',
			() =>
				dbRunner(async ({ db }) => {
					const sharedNumber = new Float64Array(1);
					let weakRef: WeakRef<ArrayBuffer> | undefined;

					const encodedKey = db.store.encodeKey('with-callback2');
					const key = Buffer.from(
						encodedKey.subarray(encodedKey.start, encodedKey.end)
					) as BufferWithDataView;

					await new Promise<void>(resolve => {
						expect(db.listeners(key)).toBe(0);
						const sharedBuffer = db.getUserSharedBuffer('with-callback2', sharedNumber.buffer, {
							callback() {
								// wait so notify() returns true
								setImmediate(() => resolve());
							},
						});
						weakRef = new WeakRef(sharedBuffer);
						expect(sharedBuffer.notify()).toBe(true);
						expect(db.listeners(key)).toBe(1);
					});

					// this can be flaky, especially when running all tests
					globalThis.gc?.();
					for (let i = 0; i < 20 && db.listeners(key) > 0; i++) {
						globalThis.gc?.();
						await delay(250);
					}

					assert(weakRef);
					expect(weakRef.deref()).toBeUndefined();
					const listenerCount = db.listeners(key);
					if (listenerCount > 0) {
						throw new Error(
							`${listenerCount} listener${listenerCount === 1 ? '' : 's'} still present!`
						);
					}
				}),
			20000
		);

		it('should share buffer across worker threads with same database', () =>
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
					worker.on('message', event => {
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

				if (process.versions.deno) {
					// deno doesn't emit an `exit` event when the worker quits, but
					// `terminate()` will trigger the `exit` event
					await delay(100);
					worker.terminate();
				}

				await resolver.promise;

				expect(getNextId()).toBe(5n);
			}), 10000);

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
				expect(() => db.getUserSharedBuffer('foo', new ArrayBuffer(1), { callback: 123 as any }))
					.toThrow('Callback must be a function');
			}));
	});
});
