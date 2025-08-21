import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';
import { withResolvers } from '../src/util.js';

describe('User Shared Buffer', () => {
	describe('getUserSharedBuffer()', () => {
		it('should create a new user shared buffer', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath);

				const defaultIncrementer = new BigInt64Array(1);
				const sharedBuffer1 = db.getUserSharedBuffer('incrementer-test', defaultIncrementer.buffer);
				expect(sharedBuffer1).toBeInstanceOf(ArrayBuffer);

				const incrementer = new BigInt64Array(sharedBuffer1);
				incrementer[0] = 4n;
				expect(Atomics.add(incrementer, 0, 1n)).toBe(4n);

				const secondDefaultIncrementer = new BigInt64Array(1); // should not get used
				const sharedBuffer2 = db.getUserSharedBuffer('incrementer-test', secondDefaultIncrementer.buffer);
				expect(sharedBuffer2).toBeInstanceOf(ArrayBuffer);

				const nextIncrementer = new BigInt64Array(sharedBuffer2); // should return same incrementer
				expect(incrementer[0]).toBe(5n);
				expect(Atomics.add(nextIncrementer, 0, 1n)).toBe(5n);
				expect(incrementer[0]).toBe(6n);
				expect(secondDefaultIncrementer[0]).toBe(0n);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should get next id using shared buffer', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath);

				const incrementer = new BigInt64Array(
					db.getUserSharedBuffer('next-id', new BigInt64Array(1).buffer)
				);
				incrementer[0] = 1n;

				const getNextId = () => Atomics.add(incrementer, 0, 1n);

				expect(getNextId()).toBe(1n);
				expect(getNextId()).toBe(2n);
				expect(getNextId()).toBe(3n);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should notify callbacks', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath);

				const sharedNumber = new Float64Array(1);
				await new Promise<void>((resolve) => {
					const sharedBuffer = db!.getUserSharedBuffer(
						'with-callback',
						sharedNumber.buffer,
						{
							callback() {
								// wait so notify() returns true
								setTimeout(() => resolve(), 100);
							},
						},
					);
					expect(sharedBuffer.notify()).toBe(true);
				});
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		(globalThis.gc ? it : it.skip)('should cleanup callbacks on GC', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath);

				const sharedNumber = new Float64Array(1);
				await new Promise<void>((resolve) => {
					expect(db!.listeners('with-callback')).toBe(0);
					const sharedBuffer = db!.getUserSharedBuffer(
						'with-callback2',
						sharedNumber.buffer,
						{
							callback() {
								// wait so notify() returns true
								setImmediate(() => resolve());
							},
						},
					);
					expect(sharedBuffer.notify()).toBe(true);
					expect(db!.listeners('with-callback2')).toBe(1);
				});

				globalThis.gc?.();
				await delay(100);
				expect(db!.listeners('with-callback2')).toBe(0);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should share buffer across threads', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath);

				const incrementer = new BigInt64Array(
					db.getUserSharedBuffer('next-id-worker', new BigInt64Array(1).buffer)
				);
				incrementer[0] = 1n;

				const getNextId = () => Atomics.add(incrementer, 0, 1n);

				// Node.js 18 and older doesn't properly eval ESM code
				const majorVersion = parseInt(process.versions.node.split('.')[0]);
				const script = majorVersion < 20
					?	`
						const tsx = require('tsx/cjs/api');
						tsx.require('./test/fixtures/user-shared-buffer-worker.mts', __dirname);
						`
					:	`
						import { register } from 'tsx/esm/api';
						register();
						import('./test/fixtures/user-shared-buffer-worker.mts');
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

				let resolver = withResolvers();

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

				resolver = withResolvers();
				worker.postMessage({ close: true });
				await resolver.promise;

				expect(getNextId()).toBe(5n);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should throw an error if the default buffer is not an ArrayBuffer', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath);
				expect(() => db!.getUserSharedBuffer('incrementer-test', undefined as any))
					.toThrow('Default buffer must be an ArrayBuffer');
				expect(() => db!.getUserSharedBuffer('incrementer-test', 'hello' as any))
					.toThrow('Default buffer must be an ArrayBuffer');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error if database is not open', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = new RocksDatabase(dbPath);
				expect(() => db!.getUserSharedBuffer('foo', new ArrayBuffer(1))).toThrow('Database not open');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error if options are invalid', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath);
				expect(() => db!.getUserSharedBuffer('foo', new ArrayBuffer(1), 'foo' as any))
					.toThrow('Options must be an object');
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
				expect(() => db!.getUserSharedBuffer('foo', new ArrayBuffer(1), { callback: 123 as any }))
					.toThrow('Callback must be a function');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});
});
