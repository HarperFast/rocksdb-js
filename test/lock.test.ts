import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';
import { describe, expect, it, vi } from 'vitest';
import { createWorkerBootstrapScript, dbRunner } from './lib/util.js';

describe('Lock', () => {
	describe('tryLock()', () => {
		it('should error attempting to lock if key is not specified', () =>
			dbRunner(async ({ db }) => {
				expect(() => (db.tryLock as any)()).toThrow('Key is required');
			}));

		it('should error if callback is not a function', () =>
			dbRunner(async ({ db }) => {
				expect(() => db.tryLock('foo', 'bar' as any)).toThrow('Callback must be a function');
			}));

		it('should error attempting to lock if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				expect(() => db.tryLock('foo')).toThrow('Database not open');
			}));

		it('should lock and unlock', () =>
			dbRunner(async ({ db }) => {
				expect(db.hasLock('foo')).toBe(false);
				expect(db.unlock('foo')).toBe(false);

				let unlockCounter = 0;
				expect(db.tryLock('foo', () => {
					unlockCounter++;
				})).toBe(true);

				const promises = [
					new Promise<void>(resolve => {
						expect(db.tryLock('foo', () => {
							unlockCounter++;
							resolve();
						})).toBe(false);
					}),
					new Promise<void>(resolve => {
						expect(db.tryLock('foo', () => {
							unlockCounter++;
							resolve();
						})).toBe(false);
					}),
				];

				expect(unlockCounter).toBe(0);
				expect(db.hasLock('foo')).toBe(true);
				expect(db.unlock('foo')).toBe(true);
				expect(db.hasLock('foo')).toBe(false);
				await Promise.all(promises);
				expect(unlockCounter).toBe(2);
			}));

		it('should lock in a lock', () =>
			dbRunner(async ({ db }) => {
				expect(db.tryLock('foo', () => {})).toBe(true);
				expect(db.tryLock('foo', () => {
					expect(db.tryLock('foo', () => {})).toBe(true);
				})).toBe(false);
				expect(db.unlock('foo')).toBe(true);
				await delay(100);
				expect(db.unlock('foo')).toBe(true);
				expect(db.unlock('foo')).toBe(false);
			}));

		it('should not unlock if another database is closed', () =>
			dbRunner(async ({ db }, { db: db2 }) => {
				expect(db.hasLock('foo')).toBe(false);
				expect(db2.hasLock('foo')).toBe(false);
				expect(db.tryLock('foo', () => {})).toBe(true);
				expect(db2.hasLock('foo')).toBe(true);
				db2.close();
				expect(db.hasLock('foo')).toBe(true);
				db.unlock('foo');
				expect(db.hasLock('foo')).toBe(false);
			}));

		it('should lock in a worker thread', () =>
			dbRunner(async ({ db, dbPath }) => {
				const spy = vi.fn();

				expect(db.tryLock('foo', () => spy())).toBe(true);
				expect(db.hasLock('foo')).toBe(true); // main thread has lock

				const worker = new Worker(
					createWorkerBootstrapScript('./test/workers/try-lock-worker.mts'),
					{ eval: true, workerData: { path: dbPath } }
				);

				let onWorkerLock: () => void;
				let onWorkerUnlock: () => void;

				await new Promise<void>((resolve, reject) => {
					worker.on('error', reject);
					worker.on('message', event => {
						try {
							if (event.started) {
								expect(event.hasLock).toBe(true);
								resolve();
							} else if (event.locked) {
								onWorkerLock();
							}
						} catch (error) {
							reject(error);
						}
					});
				});

				expect(db.hasLock('foo')).toBe(true); // main thread has lock

				db.unlock('foo'); // worker should be waiting for lock
				await new Promise<void>(resolve => onWorkerLock = resolve); // wait for worker to lock

				expect(db.tryLock('foo', () => {
					spy();
					onWorkerUnlock();
				})).toBe(false);

				worker.postMessage({ unlock: true });
				await new Promise<void>(resolve => onWorkerUnlock = resolve); // wait for worker to unlock

				expect(spy).toHaveBeenCalledTimes(1);
				expect(db.hasLock('foo')).toBe(false);

				worker.postMessage({ lock: true });
				await new Promise<void>(resolve => onWorkerLock = resolve); // wait for worker to lock

				expect(db.hasLock('foo')).toBe(true); // worker has lock

				await new Promise<void>(resolve => {
					expect(db.tryLock('foo', () => resolve())).toBe(false);
					worker.postMessage({ close: true });
				});
			}));
	});

	describe('unlock()', () => {
		it('should error attempting to unlock if key is not specified', () =>
			dbRunner(async ({ db }) => {
				expect(() => (db.unlock as any)()).toThrow('Key is required');
			}));

		it('should error attempting to unlock if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				expect(() => db.unlock('foo')).toThrow('Database not open');
			}));
	});

	describe('hasLock()', () => {
		it('should error attempting to unlock if key is not specified', () =>
			dbRunner(async ({ db }) => {
				expect(() => (db.hasLock as any)()).toThrow('Key is required');
			}));

		it('should error attempting to unlock if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				expect(() => db.hasLock('foo')).toThrow('Database not open');
			}));
	});

	describe('withLock()', () => {
		it('should error if callback is not a function', () =>
			dbRunner(async ({ db }) => {
				await expect(db.withLock('foo', 'bar' as any)).rejects.toThrow(
					'Callback must be a function'
				);
			}));

		it('should error if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				await expect(db.withLock('foo', () => {})).rejects.toThrow('Database not open');
			}));

		it('should lock and unlock', () =>
			dbRunner(async ({ db }) => {
				const spy = vi.fn();

				await db.withLock('foo', async () => {
					spy();

					await db.withLock('bar', async () => {
						// async
						await delay(100);
						spy();

						await db.withLock('baz', () => {
							// sync
							spy();
						});
					});
				});

				expect(spy).toHaveBeenCalledTimes(3);
			}));

		it('should block until the lock is released', () =>
			dbRunner(async ({ db }) => {
				const spy = vi.fn();

				const promise = db.withLock('foo', async () => {
					spy();
					expect(db.hasLock('foo')).toBe(true);
					await delay(100);
				});

				expect(db.hasLock('foo')).toBe(true);
				await promise;
				expect(spy).toHaveBeenCalledTimes(1);
				expect(db.hasLock('foo')).toBe(false);

				await db.withLock('foo', async () => {
					spy();
					expect(db.hasLock('foo')).toBe(true);
				});

				expect(db.hasLock('foo')).toBe(false);
				expect(spy).toHaveBeenCalledTimes(2);
			}));

		it('should lock in a worker thread', () =>
			dbRunner(async ({ db, dbPath }) => {
				const spy = vi.fn();

				const worker = new Worker(
					createWorkerBootstrapScript('./test/workers/with-lock-worker.mts'),
					{ eval: true, workerData: { path: dbPath } }
				);

				let onWorkerUnlock: () => void;
				let workerLocked = false;

				await new Promise<void>((resolve, reject) => {
					worker.on('error', reject);
					worker.on('message', event => {
						try {
							if (event.started) {
								expect(event.hasLock).toBe(true);
								resolve();
							} else if (event.locked) {
								workerLocked = true;
							} else if (event.unlocked) {
								workerLocked = false;
								onWorkerUnlock();
							}
						} catch (error) {
							reject(error);
						}
					});
				});

				expect(db.hasLock('foo')).toBe(true); // worker thread has lock

				await new Promise<void>(resolve => onWorkerUnlock = resolve); // wait for worker to unlock

				// queue up 3 locks: 2 immediate, 1 delayed so the worker can
				// sneak in and lock before the 3rd one
				const promise = Promise.all([
					db.withLock('foo', async () => {
						spy();
						expect(db.hasLock('foo')).toBe(true);
						expect(workerLocked).toBe(false);
					}),
					db.withLock('foo', async () => {
						spy();
						expect(db.hasLock('foo')).toBe(true);
						worker.postMessage({ lock: true });
						await delay(250);
						expect(workerLocked).toBe(false);
					}),
					(async () => {
						await delay(100);
						await db.withLock('foo', async () => {
							spy();
							expect(db.hasLock('foo')).toBe(true);
							await delay(100);
							expect(workerLocked).toBe(false);
						});
					})(),
				]);

				await promise;
				expect(workerLocked).toBe(false);
				expect(spy).toHaveBeenCalledTimes(3);

				worker.postMessage({ close: true });

				if (process.versions.deno) {
					// deno doesn't emit an `exit` event when the worker quits, but
					// `terminate()` will trigger the `exit` event
					await delay(100);
					worker.terminate();
				}
			}));
	});
});
