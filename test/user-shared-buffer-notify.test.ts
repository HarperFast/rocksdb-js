import type { BufferWithDataView } from '../src/encoding.js';
import { dbRunner } from './lib/util.js';
import { setTimeout as delay } from 'node:timers/promises';
import { assert, describe, expect, it } from 'vitest';

// Deno's Vitest fork pool crashes with a V8 HandleScope error when callback
// registration tests run in the same child process after other getUserSharedBuffer
// tests. Isolating them in a separate file gives them their own fork.
describe('User Shared Buffer notify', () => {
	describe('getUserSharedBuffer()', () => {
		it('should notify callbacks', () =>
			dbRunner(async ({ db }) => {
				const sharedNumber = new Float64Array(1);
				await new Promise<void>((resolve) => {
					const sharedBuffer = db.getUserSharedBuffer('with-callback', sharedNumber.buffer, {
						callback() {
							// wait so notify() returns true
							setTimeout(() => resolve(), 100);
						},
					});
					expect(sharedBuffer.notify()).toBe(true);
				});
			}));

		it.skipIf(!globalThis.gc)(
			'should cleanup callbacks on GC',
			() =>
				dbRunner(async ({ db }) => {
					const sharedNumber = new Float64Array(1);
					let weakRef: WeakRef<ArrayBuffer> | undefined;

					const encodedKey = db.store.encodeKey('with-callback2');
					const key = Buffer.from(
						encodedKey.subarray(encodedKey.start, encodedKey.end)
					) as BufferWithDataView;

					await new Promise<void>((resolve) => {
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
	});
});
