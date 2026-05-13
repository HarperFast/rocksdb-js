import { RocksDatabase, registryStatus, shutdown } from '../src/index.js';
import { createWorkerBootstrapScript, dbRunner, generateDBPath } from './lib/util.js';
import { spawn } from 'node:child_process';
import { mkdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';
import { describe, expect, it } from 'vitest';

describe('Shutdown', () => {
	it('should shutdown rocksdb-js', () =>
		dbRunner({ dbOptions: [{}, { name: 'test' }] }, async ({ db }, { db: db2 }) => {
			expect(db.isOpen()).toBe(true);
			expect(db2.isOpen()).toBe(true);
			let status = registryStatus();
			expect(status.length).toBe(1);
			expect(Object.keys(status[0].columnFamilies).length).toBe(2);
			shutdown();
			expect(db.isOpen()).toBe(false);
			expect(db2.isOpen()).toBe(false);
			status = registryStatus();
			expect(status.length).toBe(0);
		}));

	it('should handle multiple shutdowns', () =>
		dbRunner(async ({ db }) => {
			expect(db.isOpen()).toBe(true);
			let status = registryStatus();
			expect(status.length).toBe(1);
			expect(Object.keys(status[0].columnFamilies).length).toBe(1);
			shutdown();
			expect(db.isOpen()).toBe(false);
			status = registryStatus();
			expect(status.length).toBe(0);
			shutdown();
			expect(db.isOpen()).toBe(false);
			status = registryStatus();
			expect(status.length).toBe(0);
		}));

	it('should open 10 databases, shutdown, and open them again', async () =>
		dbRunner(
			{
				dbOptions: [
					/*  1 */ {},
					/*  2 */ { path: generateDBPath() },
					/*  3 */ { path: generateDBPath() },
					/*  4 */ { path: generateDBPath() },
					/*  5 */ { path: generateDBPath() },
					/*  6 */ { path: generateDBPath() },
					/*  7 */ { path: generateDBPath() },
					/*  8 */ { path: generateDBPath() },
					/*  9 */ { path: generateDBPath() },
					/* 10 */ { path: generateDBPath() },
				],
			},
			async (
				{ db },
				{ db: db2 },
				{ db: db3 },
				{ db: db4 },
				{ db: db5 },
				{ db: db6 },
				{ db: db7 },
				{ db: db8 },
				{ db: db9 },
				{ db: db10 }
			) => {
				for (let i = 0; i < 10; i++) {
					db.putSync(i, i);
					db2.putSync(i, i);
					db3.putSync(i, i);
					db4.putSync(i, i);
					db5.putSync(i, i);
					db6.putSync(i, i);
					db7.putSync(i, i);
					db8.putSync(i, i);
					db9.putSync(i, i);
					db10.putSync(i, i);
				}

				shutdown();

				expect(db.isOpen()).toBe(false);
				expect(db2.isOpen()).toBe(false);
				expect(db3.isOpen()).toBe(false);
				expect(db4.isOpen()).toBe(false);
				expect(db5.isOpen()).toBe(false);
				expect(db6.isOpen()).toBe(false);
				expect(db7.isOpen()).toBe(false);
				expect(db8.isOpen()).toBe(false);
				expect(db9.isOpen()).toBe(false);
				expect(db10.isOpen()).toBe(false);

				db.open();
				db2.open();
				db3.open();
				db4.open();
				db5.open();
				db6.open();
				db7.open();
				db8.open();
				db9.open();
				db10.open();

				expect(db.isOpen()).toBe(true);
				expect(db2.isOpen()).toBe(true);
				expect(db3.isOpen()).toBe(true);
				expect(db4.isOpen()).toBe(true);
				expect(db5.isOpen()).toBe(true);
				expect(db6.isOpen()).toBe(true);
				expect(db7.isOpen()).toBe(true);
				expect(db8.isOpen()).toBe(true);
				expect(db9.isOpen()).toBe(true);
				expect(db10.isOpen()).toBe(true);

				for (let i = 0; i < 10; i++) {
					expect(db.getSync(i)).toBe(i);
					expect(db2.getSync(i)).toBe(i);
					expect(db3.getSync(i)).toBe(i);
					expect(db4.getSync(i)).toBe(i);
					expect(db5.getSync(i)).toBe(i);
					expect(db6.getSync(i)).toBe(i);
					expect(db7.getSync(i)).toBe(i);
					expect(db8.getSync(i)).toBe(i);
					expect(db9.getSync(i)).toBe(i);
					expect(db10.getSync(i)).toBe(i);
				}
			}
		));

	it('should spawn process that opens 20 databases, shutdown on exit, forks, an open them again', async () => {
		const dbPath = generateDBPath();
		await mkdir(dbPath, { recursive: true });

		await new Promise<void>((resolve, reject) => {
			const args =
				process.versions.bun || process.versions.deno
					? [join(__dirname, 'fixtures', 'fork-shutdown.mts'), dbPath]
					: [
							'node_modules/tsx/dist/cli.mjs',
							join(__dirname, 'fixtures', 'fork-shutdown.mts'),
							dbPath,
						];

			const child = spawn(process.execPath, args, {
				env: { ...process.env, DO_FORK: '1' },
				// stdio: 'inherit',
			});
			child.on('close', (code) => {
				try {
					expect(code).toBe(0);
					resolve();
				} catch (error) {
					reject(error);
				}
			});
			child.on('error', reject);
		});
	});

	it('should flush disableWAL writes when worker is terminated and shutdown is called', async () => {
		const dbPath = generateDBPath();
		await mkdir(dbPath, { recursive: true });

		// Start worker that writes with disableWAL=true
		const worker = new Worker(
			createWorkerBootstrapScript('./test/workers/disable-wal-shutdown-worker.mts'),
			{
				eval: true,
				workerData: { path: dbPath },
			}
		);

		// Wait for worker to start and write some data
		let lastWriteCount = 0;
		await new Promise<void>((resolve, reject) => {
			const timeout = setTimeout(() => reject(new Error('Worker start timeout')), 10000);
			worker.on('message', (event) => {
				if (event.started) {
					clearTimeout(timeout);
					resolve();
				}
				if (event.writeCount) {
					lastWriteCount = event.writeCount;
				}
			});
			worker.on('error', reject);
		});

		// Let the worker write data for a bit
		await delay(100);

		// Get the last known write count before terminating
		const writeCountBeforeTerminate = lastWriteCount;

		// Terminate the worker abruptly (simulates crash/kill)
		await worker.terminate();

		// Main thread calls shutdown to flush all databases
		shutdown();

		// Re-open the database and verify data integrity
		const db = RocksDatabase.open(dbPath);

		// Verify that at least some data was written and persisted
		// We check up to writeCountBeforeTerminate since we know at least that many were written
		let verifiedCount = 0;
		for (let i = 0; i < writeCountBeforeTerminate; i++) {
			const value = db.getSync(`key-${i}`) as { index: number; data: string } | undefined;
			if (value !== undefined) {
				expect(value.index).toBe(i);
				expect(value.data).toBe(`value-${i}`);
				verifiedCount++;
			}
		}

		// Ensure we actually verified some data (worker should have written at least a few entries)
		expect(verifiedCount).toBeGreaterThan(0);

		db.close();

		// Cleanup
		if (!process.env.KEEP_FILES) {
			await rm(dbPath, { force: true, recursive: true });
		}
	});

	it('should handle shutdown() while worker is still actively writing', async () => {
		const dbPath = generateDBPath();
		await mkdir(dbPath, { recursive: true });

		// Start worker that writes with disableWAL=true
		const worker = new Worker(
			createWorkerBootstrapScript('./test/workers/disable-wal-shutdown-worker.mts'),
			{
				eval: true,
				workerData: { path: dbPath },
			}
		);

		// Wait for worker to start
		let lastWriteCount = 0;
		await new Promise<void>((resolve, reject) => {
			const timeout = setTimeout(() => reject(new Error('Worker start timeout')), 10000);
			worker.on('message', (event) => {
				if (event.started) {
					clearTimeout(timeout);
					resolve();
				}
				if (event.writeCount) {
					lastWriteCount = event.writeCount;
				}
			});
			worker.on('error', reject);
		});

		// Let the worker write some data
		await delay(50);

		// Get the write count before shutdown
		const writeCountBeforeShutdown = lastWriteCount;

		// Call shutdown() while worker is STILL RUNNING (not terminated)
		// This should not crash
		shutdown();

		// Now terminate the worker
		await worker.terminate();

		// Re-open the database and verify data integrity
		const db = RocksDatabase.open(dbPath);

		// Verify that at least some data was written and persisted
		let verifiedCount = 0;
		for (let i = 0; i < writeCountBeforeShutdown; i++) {
			const value = db.getSync(`key-${i}`) as { index: number; data: string } | undefined;
			if (value !== undefined) {
				expect(value.index).toBe(i);
				expect(value.data).toBe(`value-${i}`);
				verifiedCount++;
			}
		}

		// Ensure we actually verified some data
		expect(verifiedCount).toBeGreaterThan(0);

		db.close();

		// Cleanup
		if (!process.env.KEEP_FILES) {
			await rm(dbPath, { force: true, recursive: true });
		}
	});

	it('should throw "Database is closing" when sync operations are attempted during shutdown', async () => {
		const dbPath = generateDBPath();
		await mkdir(dbPath, { recursive: true });

		// Start worker that opens the database and writes continuously
		const worker = new Worker(
			createWorkerBootstrapScript('./test/workers/shutdown-error-worker.mts'),
			{
				eval: true,
				workerData: { path: dbPath },
			}
		);

		// Wait for worker to start writing
		await new Promise<void>((resolve, reject) => {
			const timeout = setTimeout(() => reject(new Error('Worker start timeout')), 10000);
			worker.on('message', (event) => {
				if (event.started) {
					clearTimeout(timeout);
					resolve();
				}
			});
			worker.on('error', reject);
		});

		// Let the worker write some data
		await delay(50);

		// Set up promise to capture the error from the worker
		const errorPromise = new Promise<{ message: string }>((resolve, reject) => {
			const timeout = setTimeout(() => reject(new Error('Worker error timeout')), 10000);
			worker.on('message', (event) => {
				if (event.error) {
					clearTimeout(timeout);
					resolve({ message: event.message });
				}
			});
		});

		// Call shutdown() - this should cause the worker to get "Database is closing" error
		shutdown();

		// Wait for the worker to report the error
		const error = await errorPromise;
		expect(error.message).toBe('Database is closing');

		// Clean up worker
		await worker.terminate();

		// Cleanup
		if (!process.env.KEEP_FILES) {
			await rm(dbPath, { force: true, recursive: true });
		}
	});
});
