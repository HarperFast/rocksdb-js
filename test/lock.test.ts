import { describe, expect, it, vi } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker, threadId } from 'node:worker_threads';

describe('Lock', () => {
	describe('tryLock()', () => {
		it('should error attempting to lock if key is not specified', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				expect(() => (db!.tryLock as any)()).toThrow('Key is required');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error if callback is not a function', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				expect(() => db!.tryLock('foo', 'bar' as any)).toThrow('Callback must be a function');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error attempting to lock if database is not open', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = new RocksDatabase(dbPath);
				expect(() => db!.tryLock('foo')).toThrow('Database not open');
			} finally {
				await rimraf(dbPath);
			}
		});

		it('should lock and unlock', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				expect(db!.hasLock('foo')).toBe(false);
				expect(db!.unlock('foo')).toBe(false);

				let unlockCounter = 0;
				expect(db!.tryLock('foo', () => {
					unlockCounter++;
				})).toBe(true);

				const promises = [
					new Promise<void>(resolve => {
						expect(db!.tryLock('foo', () => {
							unlockCounter++;
							resolve();
						})).toBe(false);
					}),
					new Promise<void>(resolve => {
						expect(db!.tryLock('foo', () => {
							unlockCounter++;
							resolve();
						})).toBe(false);
					})
				];

				expect(unlockCounter).toBe(0);
				expect(db!.hasLock('foo')).toBe(true);
				expect(db!.unlock('foo')).toBe(true);
				expect(db!.hasLock('foo')).toBe(false);
				await Promise.all(promises);
				expect(unlockCounter).toBe(2);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should lock in a lock', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				expect(db!.tryLock('foo', () => {})).toBe(true);
				expect(db!.tryLock('foo', () => {
					expect(db!.tryLock('foo', () => {})).toBe(true);
				})).toBe(false);
				expect(db!.unlock('foo')).toBe(true);
				await delay(100);
				expect(db!.unlock('foo')).toBe(true);
				expect(db!.unlock('foo')).toBe(false);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should not unlock if another database is closed', async () => {
			let db: RocksDatabase | null = null;
			let db2: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				db2 = RocksDatabase.open(dbPath);
				expect(db.hasLock('foo')).toBe(false);
				expect(db2.hasLock('foo')).toBe(false);
				expect(db!.tryLock('foo', () => {})).toBe(true);
				expect(db2.hasLock('foo')).toBe(true);
				db2.close();
				expect(db.hasLock('foo')).toBe(true);
				db.unlock('foo');
				expect(db.hasLock('foo')).toBe(false);
			} finally {
				db?.close();
				db2?.close();
				await rimraf(dbPath);
			}
		});

		it('should lock in a worker thread', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				const spy = vi.fn();

				expect(db.tryLock('foo', () => spy())).toBe(true);
				expect(db.hasLock('foo')).toBe(true); // main thread has lock

				// Node.js 18 and older doesn't properly eval ESM code
				const majorVersion = parseInt(process.versions.node.split('.')[0]);
				const script = majorVersion < 20
					?	`
						const tsx = require('tsx/cjs/api');
						tsx.require('./test/fixtures/try-lock-worker.mts', __dirname);
						`
					:	`
						import { register } from 'tsx/esm/api';
						register();
						import('./test/fixtures/try-lock-worker.mts');
						`;

				const worker = new Worker(
					script,
					{
						eval: true,
						workerData: {
							path: dbPath,
						}
					}
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
					expect(db!.tryLock('foo', () => resolve())).toBe(false);
					worker.terminate();
				});
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	describe('unlock()', () => {
		it('should error attempting to unlock if key is not specified', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				expect(() => (db!.unlock as any)()).toThrow('Key is required');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error attempting to unlock if database is not open', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = new RocksDatabase(dbPath);
				expect(() => db!.unlock('foo')).toThrow('Database not open');
			} finally {
				await rimraf(dbPath);
			}
		});
	});

	describe('hasLock()', () => {
		it('should error attempting to unlock if key is not specified', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				expect(() => (db!.hasLock as any)()).toThrow('Key is required');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error attempting to unlock if database is not open', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = new RocksDatabase(dbPath);
				expect(() => db!.hasLock('foo')).toThrow('Database not open');
			} finally {
				await rimraf(dbPath);
			}
		});
	});

	describe('withLock()', () => {
		it('should error if callback is not a function', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				await expect(db!.withLock('foo', 'bar' as any)).rejects.toThrow('Callback must be a function');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error if database is not open', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = new RocksDatabase(dbPath);
				await expect(() => db!.withLock('foo', () => {})).rejects.toThrow('Database not open');
			} finally {
				await rimraf(dbPath);
			}
		});

		it('should lock and unlock', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				const spy = vi.fn();

				await db.withLock('foo', async () => {
					spy();

					await db!.withLock('bar', async () => {
						// async
						await delay(100);
						spy();

						await db!.withLock('baz', () => {
							// sync
							spy();
						});
					});
				});

				expect(spy).toHaveBeenCalledTimes(3);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should block until the lock is released', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				const spy = vi.fn();

				const promise = db.withLock('foo', async () => {
					spy();
					expect(db!.hasLock('foo')).toBe(true);
					await delay(100);
				});

				expect(db.hasLock('foo')).toBe(true);
				expect(spy).toHaveBeenCalledTimes(1);

				await promise;

				await db.withLock('foo', async () => {
					spy();
				});

				expect(db.hasLock('foo')).toBe(false);
				expect(spy).toHaveBeenCalledTimes(2);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it.only('should lock in a worker thread', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				const spy = vi.fn();

				// Node.js 18 and older doesn't properly eval ESM code
				const majorVersion = parseInt(process.versions.node.split('.')[0]);
				const script = majorVersion < 20
					?	`
						const tsx = require('tsx/cjs/api');
						tsx.require('./test/fixtures/with-lock-worker.mts', __dirname);
						`
					:	`
						import { register } from 'tsx/esm/api';
						register();
						import('./test/fixtures/with-lock-worker.mts');
						`;

				const worker = new Worker(
					script,
					{
						eval: true,
						workerData: {
							path: dbPath,
						}
					}
				);

				let onWorkerUnlock: () => void;
				let workerLocked = false;

				await new Promise<void>((resolve, reject) => {
					worker.on('error', reject);
					worker.on('message', event => {
						console.log(`main thread ${threadId} message`, event);
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

				expect(workerLocked).toBe(true);
				expect(db.hasLock('foo')).toBe(true); // worker thread has lock

				await new Promise<void>(resolve => onWorkerUnlock = resolve); // wait for worker to unlock

				const promise = Promise.all([
					db.withLock('foo', async () => {
						console.log(`main thread ${threadId} lock 1`);
						spy();
						expect(db!.hasLock('foo')).toBe(true);
						expect(workerLocked).toBe(false);
						console.log(`main thread ${threadId} unlock 1`);
					}),
					db.withLock('foo', async () => {
						console.log(`main thread ${threadId} lock 2`);
						spy();
						expect(db!.hasLock('foo')).toBe(true);
						worker.postMessage({ lock: true });
						await delay(1000);
						expect(workerLocked).toBe(false);
						console.log(`main thread ${threadId} unlock 2`);
					}),
					(async () => {
						console.log(`main thread ${threadId} locking in 100ms`);
						await delay(100),
						console.log(`main thread ${threadId} locking`);
						await db!.withLock('foo', async () => {
							console.log(`main thread ${threadId} lock 3`);
							spy();
							expect(db!.hasLock('foo')).toBe(true);
							await delay(100);
							expect(workerLocked).toBe(false);
							console.log(`main thread ${threadId} unlock 3`);
						})
					})()
				]);

				await promise;
				expect(workerLocked).toBe(false);
				expect(spy).toHaveBeenCalledTimes(3);

				worker.terminate();
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		// thread id test?
	});
});
