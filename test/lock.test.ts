import { describe, expect, it, vi } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

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

				expect(db!.tryLock('foo', () => {
					unlockCounter++;
				})).toBe(true);

				let unlockCounter = 0;
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

		it('should propagate errors from callback', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);

				expect(db.tryLock('foo')).toBe(true);

				// queue up the callback
				expect(db!.tryLock('foo', () => {
					throw new Error('test');
				})).toBe(false);

				expect(() => db!.unlock('foo')).toThrow('test');
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
				expect(db!.unlock('foo')).toBe(true);
				expect(db!.unlock('foo')).toBe(false);
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
				let onworkerlock: () => void;
				let onworkerunlock: () => void;

				expect(db!.tryLock('foo', () => spy())).toBe(true);

				const worker = new Worker(
					`
					import { createRequire } from 'node:module';
					import { register } from 'tsx/esm/api';
					register();
					import('./test/fixtures/lock-worker.ts');
					`,
					{
						eval: true,
						workerData: {
							path: dbPath,
						}
					}
				);

				await new Promise<void>((resolve, reject) => {
					worker.on('error', error => {
						console.log(error);
						reject(error);
					});
					worker.on('message', event => {
						try {
							console.log('message from worker', event);
							if (event.started) {
								expect(event.hasLock).toBe(true);
								resolve();
							}
							if (event.locked && onworkerlock) {
								onworkerlock();
							}
						} catch (error) {
							reject(error);
						}
					});
				});

				db.unlock('foo');
				await new Promise<void>(resolve => {
					onworkerlock = resolve;
				});
				expect(db.tryLock('foo', () => {
					spy();
					onworkerunlock?.();
				})).toBe(false);

				worker.postMessage({ unlock: true });
				await new Promise<void>(resolve => {
					onworkerunlock = resolve;
				});
				expect(spy).toHaveBeenCalledTimes(1);

				worker.postMessage({ lock: true });
				await new Promise<void>(resolve => {
					onworkerlock = resolve;
				});
				await new Promise<void>((resolve, reject) => {
					expect(db!.tryLock('foo', () => {
						spy();
						try {
							expect(spy).toHaveBeenCalledTimes(2);
							resolve();
						} catch (error) {
							reject(error);
						}
					})).toBe(false);
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

	describe('lock()', () => {
		it('should error if callback is not a function', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				await expect(db!.lock('foo', 'bar' as any)).rejects.toThrow('Callback must be a function');
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
				await expect(() => db!.lock('foo', () => {})).rejects.toThrow('Database not open');
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

				await db.lock('foo', async () => {
					spy();

					await db!.lock('bar', async () => {
						// async
						await delay(100);
						spy();

						await db!.lock('baz', () => {
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

				const promise = db.lock('foo', async () => {
					spy();
					expect(db!.hasLock('foo')).toBe(true);
					await delay(100);
				});

				expect(db.hasLock('foo')).toBe(true);
				expect(spy).toHaveBeenCalledTimes(1);

				await promise;

				await db.lock('foo', async () => {
					spy();
				});

				expect(db.hasLock('foo')).toBe(false);
				expect(spy).toHaveBeenCalledTimes(2);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should propagate errors from callback', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);

				await expect(db.lock('foo', async () => {
					throw new Error('test');
				})).rejects.toThrow('test');

				expect(db.hasLock('foo')).toBe(false);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should lock in a worker thread', async () => {
			// TODO
		});
	});
});
