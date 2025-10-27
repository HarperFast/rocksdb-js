import { describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';
import { mkdir, writeFile, utimes } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { withResolvers } from '../src/util.js';
import { Worker } from 'node:worker_threads';
import assert from 'node:assert';
import type { TransactionLog } from '../src/load-binding.js';

describe('Transaction Log', () => {
	describe('useLog()', () => {
		it('should detect existing transaction logs', () => dbRunner({
			skipOpen: true
		}, async ({ db, dbPath }) => {
			await mkdir(join(dbPath, 'transaction_logs', 'foo'), { recursive: true });
			await writeFile(join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog'), '');

			db.open();

			expect(db.listLogs()).toEqual(['foo']);

			const fooLog = db.useLog('foo');
			expect(fooLog).toBeDefined();

			const barLog = db.useLog('bar');
			expect(barLog).toBeDefined();
			expect(barLog).not.toBe(fooLog);

			const fooLog2 = db.useLog('foo');
			expect(fooLog2).toBe(fooLog);

			expect(db.listLogs()).toEqual(['bar', 'foo']);
		}));

		it('should support numeric log names', () => dbRunner(async ({ db }) => {
			db.open();

			expect(db.listLogs()).toEqual([]);

			const fooLog = db.useLog(612);
			expect(fooLog).toBeDefined();

			const fooLog2 = db.useLog(612);
			expect(fooLog2).toBe(fooLog);

			expect(db.listLogs()).toEqual(['612']);
		}));

		(globalThis.gc ? it : it.skip)('should cleanup transaction log instance on GC', () => dbRunner(async ({ db }) => {
			let weakRef: WeakRef<TransactionLog> | undefined;

			await new Promise<void>((resolve) => {
				const log = db.useLog('foo');
				weakRef = new WeakRef(log);
				resolve();
			});

			assert(weakRef);
			assert(globalThis.gc);

			// this is flaky
			const until = Date.now() + 3000;
			while (Date.now() < until) {
				globalThis.gc();
				await delay(250);
				globalThis.gc();
				await delay(250);
				if (!weakRef.deref()) {
					break;
				}
			}

			expect(weakRef.deref()).toBeUndefined();
		}));

		it.only('should add entries to a transaction log', () => dbRunner(async ({ db, dbPath }) => {
			const log = db.useLog('foo');

			await db.transaction(async (txn) => {
				const value = Buffer.from('world');
				txn.put(Buffer.from('hello'), value);
				log.addEntry(value, txn.id);
			});
		}));

		it('should rotate a transaction log', () => dbRunner(async ({ db, dbPath }) => {
			//
		}));

		// it('should write to same log from multiple workers', () => dbRunner(async ({ db, dbPath }) => {
		// 	// Node.js 18 and older doesn't properly eval ESM code
		// 	const majorVersion = parseInt(process.versions.node.split('.')[0]);
		// 	const script = process.versions.deno || process.versions.bun
		// 		?	`
		// 			import { pathToFileURL } from 'node:url';
		// 			import(pathToFileURL('./test/workers/transaction-log-worker.mts'));
		// 			`
		// 		:	majorVersion < 20
		// 			?	`
		// 				const tsx = require('tsx/cjs/api');
		// 				tsx.require('./test/workers/transaction-log-worker.mts', __dirname);
		// 				`
		// 			:	`
		// 				import { register } from 'tsx/esm/api';
		// 				register();
		// 				import('./test/workers/transaction-log-worker.mts');
		// 				`;

		// 	const worker = new Worker(
		// 		script,
		// 		{
		// 			eval: true,
		// 			workerData: {
		// 				path: dbPath,
		// 			}
		// 		}
		// 	);

		// 	let resolver = withResolvers<void>();

		// 	await new Promise<void>((resolve, reject) => {
		// 		worker.on('error', reject);
		// 		worker.on('message', event => {
		// 			try {
		// 				if (event.started) {
		// 					resolve();
		// 				} else if (event.done) {
		// 					resolver.resolve();
		// 				}
		// 			} catch (error) {
		// 				reject(error);
		// 			}
		// 		});
		// 		worker.on('exit', () => resolver.resolve());
		// 	});

		// 	worker.postMessage({ addManyEntries: true, count: 1000 });

		// 	for (let i = 0; i < 1000; i++) {
		// 		const log = db.useLog('foo');
		// 		log.addEntry(Date.now(), Buffer.from('hello'));
		// 		if (i > 0 && i % 10 === 0) {
		// 			db.purgeLogs({ destroy: true });
		// 		}
		// 	}

		// 	await resolver.promise;

		// 	resolver = withResolvers<void>();
		// 	worker.postMessage({ close: true });

		// 	if (process.versions.deno) {
		// 		// deno doesn't emit an `exit` event when the worker quits, but
		// 		// `terminate()` will trigger the `exit` event
		// 		await delay(100);
		// 		worker.terminate();
		// 	}

		// 	await resolver.promise;
		// }), 60000);

		it('should queue all entries for a transaction', () => dbRunner(async ({ db, dbPath }) => {
			// TODO
		}));
	});

	describe('purgeLogs', () => {
		it('should purge all transaction log files', () => dbRunner({
			skipOpen: true
		}, async ({ db, dbPath }) => {
			const logDirectory = join(dbPath, 'transaction_logs', 'foo');
			const logFile = join(logDirectory, 'foo.1.txnlog');
			await mkdir(logDirectory, { recursive: true });
			await writeFile(logFile, '');
			expect(existsSync(logFile)).toBe(true);

			db.open();
			expect(db.listLogs()).toEqual(['foo']);
			expect(existsSync(logFile)).toBe(true);
			expect(db.purgeLogs({ destroy: true })).toEqual([logFile]);
			expect(existsSync(logDirectory)).toBe(false);
		}));

		it('should destroy a specific transaction log file', () => dbRunner({
			skipOpen: true
		}, async ({ db, dbPath }) => {
			const fooLogDirectory = join(dbPath, 'transaction_logs', 'foo');
			const fooLogFile = join(fooLogDirectory, 'foo.1.txnlog');
			await mkdir(fooLogDirectory, { recursive: true });
			await writeFile(fooLogFile, '');

			const barLogDirectory = join(dbPath, 'transaction_logs', 'bar');
			const barLogFile = join(barLogDirectory, 'bar.1.txnlog');
			await mkdir(barLogDirectory, { recursive: true });
			await writeFile(barLogFile, '');

			db.open();
			expect(db.listLogs().sort()).toEqual(['bar', 'foo']);
			expect(existsSync(fooLogFile)).toBe(true);
			expect(existsSync(barLogFile)).toBe(true);
			expect(db.purgeLogs({ destroy: true, name: 'foo' })).toEqual([fooLogFile]);
			expect(db.listLogs().sort()).toEqual(['bar']);
			expect(existsSync(fooLogFile)).toBe(false);
			expect(existsSync(fooLogDirectory)).toBe(false);
			expect(existsSync(barLogFile)).toBe(true);
			expect(existsSync(barLogDirectory)).toBe(true);
		}));

		it('should purge old log file on load', () => dbRunner({
			skipOpen: true
		}, async ({ db, dbPath }) => {
			const logDirectory = join(dbPath, 'transaction_logs', 'foo');
			const logFile = join(logDirectory, 'foo.1.txnlog');
			await mkdir(logDirectory, { recursive: true });
			await writeFile(logFile, '');

			const oneWeekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
			await utimes(logFile, oneWeekAgo, oneWeekAgo);

			db.open();
			expect(db.listLogs()).toEqual(['foo']);
			expect(existsSync(logFile)).toBe(false);
		}));
	});
});
