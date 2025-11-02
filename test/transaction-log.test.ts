import { describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';
import { mkdir, readdir, writeFile, utimes } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { withResolvers } from '../src/util.js';
import { Worker } from 'node:worker_threads';
import assert from 'node:assert';
import type { TransactionLog } from '../src/load-binding.js';
import { BLOCK_HEADER_SIZE, CONTINUATION_FLAG, FILE_HEADER_SIZE, parseTransactionLog, TRANSACTION_HEADER_SIZE } from '../src/transaction-log.js';
import { execSync } from 'node:child_process';

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

		it('should add a single small entry within a single block by reference', () => dbRunner(async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			const value = Buffer.alloc(10, 'a');

			await db.transaction(async (txn) => {
				log.addEntry(value, txn.id);
			});

			const logPath = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const info = parseTransactionLog(logPath);
			expect(info.size).toBe(FILE_HEADER_SIZE + BLOCK_HEADER_SIZE + TRANSACTION_HEADER_SIZE + 10);
			expect(info.version).toBe(1);
			expect(info.blockSize).toBe(4096);
			expect(info.blockCount).toBe(1);
			expect(info.blocks.length).toBe(1);
			expect(info.blocks[0].startTimestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.blocks[0].flags).toBe(0);
			expect(info.blocks[0].dataOffset).toBe(0);
			expect(info.entries.length).toBe(1);
			expect(info.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[0].length).toBe(10);
			expect(info.entries[0].data).toEqual(value);
		}));

		it('should add a single small entry within a single block by copy', () => dbRunner(async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			const value = Buffer.alloc(10, 'a');

			await db.transaction(async (txn) => {
				log.addEntryCopy(value, txn.id);
			});

			const logPath = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const info = parseTransactionLog(logPath);
			expect(info.size).toBe(FILE_HEADER_SIZE + BLOCK_HEADER_SIZE + TRANSACTION_HEADER_SIZE + 10);
			expect(info.version).toBe(1);
			expect(info.blockSize).toBe(4096);
			expect(info.blockCount).toBe(1);
			expect(info.blocks.length).toBe(1);
			expect(info.blocks[0].startTimestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.blocks[0].flags).toBe(0);
			expect(info.blocks[0].dataOffset).toBe(0);
			expect(info.entries.length).toBe(1);
			expect(info.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[0].length).toBe(10);
			expect(info.entries[0].data).toEqual(value);
		}));

		it('should add multiple small entries within a single block', () => dbRunner(async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			const valueA = Buffer.alloc(10, 'a');
			const valueB = Buffer.alloc(10, 'b');
			const valueC = Buffer.alloc(10, 'c');

			await db.transaction(async (txn) => {
				log.addEntry(valueA, txn.id);
				log.addEntry(valueB, txn.id);
				log.addEntry(valueC, txn.id);
			});

			const logPath = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const info = parseTransactionLog(logPath);
			expect(info.size).toBe(FILE_HEADER_SIZE + BLOCK_HEADER_SIZE + (TRANSACTION_HEADER_SIZE + 10) * 3);
			expect(info.version).toBe(1);
			expect(info.blockSize).toBe(4096);
			expect(info.blockCount).toBe(1);
			expect(info.blocks.length).toBe(1);
			expect(info.blocks[0].startTimestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.blocks[0].flags).toBe(0);
			expect(info.blocks[0].dataOffset).toBe(0);
			expect(info.entries.length).toBe(3);
			expect(info.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[0].length).toBe(10);
			expect(info.entries[0].data).toEqual(valueA);
			expect(info.entries[1].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[1].length).toBe(10);
			expect(info.entries[1].data).toEqual(valueB);
			expect(info.entries[2].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[2].length).toBe(10);
			expect(info.entries[2].data).toEqual(valueC);
		}));

		it('should add a large entry across two blocks', () => dbRunner(async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			const value = Buffer.alloc(5000, 'a');

			await db.transaction(async (txn) => {
				log.addEntry(value, txn.id);
			});

			const logPath = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const info = parseTransactionLog(logPath);
			expect(info.size).toBe(FILE_HEADER_SIZE + (BLOCK_HEADER_SIZE * 2) + TRANSACTION_HEADER_SIZE + 5000);
			expect(info.version).toBe(1);
			expect(info.blockSize).toBe(4096);
			expect(info.blockCount).toBe(2);
			expect(info.blocks.length).toBe(2);
			expect(info.blocks[0].startTimestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.blocks[0].flags).toBe(0);
			expect(info.blocks[0].dataOffset).toBe(0);
			expect(info.blocks[1].startTimestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.blocks[1].flags).toBe(CONTINUATION_FLAG);
			expect(info.blocks[1].dataOffset).toBe(0);
			expect(info.entries.length).toBe(1);
			expect(info.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[0].length).toBe(5000);
			expect(info.entries[0].data).toEqual(value);
		}));

		it('should add an extra large entry across three blocks', () => dbRunner(async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			const value = Buffer.alloc(10000, 'a');

			await db.transaction(async (txn) => {
				log.addEntry(value, txn.id);
			});

			const logPath = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const info = parseTransactionLog(logPath);
			expect(info.size).toBe(FILE_HEADER_SIZE + (BLOCK_HEADER_SIZE * 3) + TRANSACTION_HEADER_SIZE + 10000);
			expect(info.version).toBe(1);
			expect(info.blockSize).toBe(4096);
			expect(info.blockCount).toBe(3);
			expect(info.blocks.length).toBe(3);
			expect(info.blocks[0].startTimestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.blocks[0].flags).toBe(0);
			expect(info.blocks[0].dataOffset).toBe(0);
			expect(info.blocks[1].startTimestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.blocks[1].flags).toBe(CONTINUATION_FLAG);
			expect(info.blocks[1].dataOffset).toBe(0);
			expect(info.blocks[2].startTimestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.blocks[2].flags).toBe(CONTINUATION_FLAG);
			expect(info.blocks[2].dataOffset).toBe(0);
			expect(info.entries.length).toBe(1);
			expect(info.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[0].length).toBe(10000);
			expect(info.entries[0].data).toEqual(value);
		}));

		it('should add a small entry and a large entry across two blocks', () => dbRunner(async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			const valueA = Buffer.alloc(10, 'a');
			const valueB = Buffer.alloc(5000, 'b');

			await db.transaction(async (txn) => {
				log.addEntry(valueA, txn.id);
				log.addEntry(valueB, txn.id);
			});

			const logPath = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const info = parseTransactionLog(logPath);
			expect(info.size).toBe(FILE_HEADER_SIZE + (BLOCK_HEADER_SIZE * 2) + (TRANSACTION_HEADER_SIZE + 10) + (TRANSACTION_HEADER_SIZE + 5000));
			expect(info.version).toBe(1);
			expect(info.blockSize).toBe(4096);
			expect(info.blockCount).toBe(2);
			expect(info.blocks.length).toBe(2);

			expect(info.blocks[0].startTimestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.blocks[0].flags).toBe(0);
			expect(info.blocks[0].dataOffset).toBe(0);
			expect(info.blocks[1].startTimestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.blocks[1].flags).toBe(CONTINUATION_FLAG);
			expect(info.blocks[1].dataOffset).toBe(0);
			expect(info.entries.length).toBe(2);
			expect(info.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[0].length).toBe(10);
			expect(info.entries[0].data).toEqual(valueA);
			expect(info.entries[1].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[1].length).toBe(5000);
			expect(info.entries[1].data).toEqual(valueB);
		}));

		it('should split a transaction header across multiple blocks', () => dbRunner(async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			// the transaction header is 12 bytes, but there's only room for 4 bytes in the first block
			const valueALength = 4096 - BLOCK_HEADER_SIZE - TRANSACTION_HEADER_SIZE - 4;
			const valueA = Buffer.alloc(valueALength, 'a');
			const valueB = Buffer.alloc(100, 'b');

			await db.transaction(async (txn) => {
				log.addEntry(valueA, txn.id);
				log.addEntry(valueB, txn.id);
			});

			const logPath = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const info = parseTransactionLog(logPath);
			expect(info.size).toBe(FILE_HEADER_SIZE + (BLOCK_HEADER_SIZE * 2) + (TRANSACTION_HEADER_SIZE + valueALength) + (TRANSACTION_HEADER_SIZE + 100));
			expect(info.version).toBe(1);
			expect(info.blockSize).toBe(4096);
			expect(info.blockCount).toBe(2);
			expect(info.blocks.length).toBe(2);

			expect(info.blocks[0].startTimestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.blocks[0].flags).toBe(0);
			expect(info.blocks[0].dataOffset).toBe(0);
			expect(info.blocks[1].startTimestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.blocks[1].flags).toBe(CONTINUATION_FLAG);
			expect(info.blocks[1].dataOffset).toBe(0);
			expect(info.entries.length).toBe(2);
			expect(info.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[0].length).toBe(valueALength);
			expect(info.entries[0].data).toEqual(valueA);
			expect(info.entries[1].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[1].length).toBe(100);
			expect(info.entries[1].data).toEqual(valueB);
		}));

		it('should not commit the log if the transaction is aborted', () => dbRunner(async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			const value = Buffer.alloc(100, 'a');

			await db.transaction(async (txn) => {
				log.addEntry(value, txn.id);
				txn.abort();
			});

			const logPath = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			expect(existsSync(logPath)).toBe(false);
		}));

		it.only('should add multiple entries from separate transactions', () => dbRunner(async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			const valueA = Buffer.alloc(10, 'a');
			const valueB = Buffer.alloc(10, 'b');

			await db.transaction(async (txn) => {
				log.addEntry(valueA, txn.id);
			});

			await db.transaction(async (txn) => {
				log.addEntry(valueB, txn.id);
			});

			const logPath = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const info = parseTransactionLog(logPath);
			expect(info.size).toBe(FILE_HEADER_SIZE + BLOCK_HEADER_SIZE + ((TRANSACTION_HEADER_SIZE + 10) * 2));
			expect(info.version).toBe(1);
			expect(info.blockSize).toBe(4096);
			expect(info.blockCount).toBe(1);
			expect(info.blocks.length).toBe(1);
			expect(info.blocks[0].startTimestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.blocks[0].flags).toBe(0);
			expect(info.blocks[0].dataOffset).toBe(0);
			expect(info.entries.length).toBe(2);
			expect(info.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[0].length).toBe(10);
			expect(info.entries[0].data).toEqual(valueA);
			expect(info.entries[1].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[1].length).toBe(10);
			expect(info.entries[1].data).toEqual(valueB);
		}));

		it('should rotate a transaction log', () => dbRunner({
			dbOptions: [{ transactionLogMaxSize: 1000 }],
		}, async ({ db, dbPath }) => {
			const log = db.useLog('foo');

			for (let i = 0; i < 25; i++) {
				await db.transaction(async (txn) => {
					log.addEntry(Buffer.alloc(100, 'a'), txn.id);
				});
			}

			const logStorePath = join(dbPath, 'transaction_logs', 'foo');
			const logFiles = await readdir(logStorePath);
			expect(logFiles.sort()).toEqual(['foo.1.txnlog', 'foo.2.txnlog', 'foo.3.txnlog']);
		}));

		it.skip('should write to same log from multiple workers', () => dbRunner(async ({ db, dbPath }) => {
			// Node.js 18 and older doesn't properly eval ESM code
			const majorVersion = parseInt(process.versions.node.split('.')[0]);
			const script = process.versions.deno || process.versions.bun
				?	`
					import { pathToFileURL } from 'node:url';
					import(pathToFileURL('./test/workers/transaction-log-worker.mts'));
					`
				:	majorVersion < 20
					?	`
						const tsx = require('tsx/cjs/api');
						tsx.require('./test/workers/transaction-log-worker.mts', __dirname);
						`
					:	`
						import { register } from 'tsx/esm/api';
						register();
						import('./test/workers/transaction-log-worker.mts');
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

			let resolver = withResolvers<void>();

			await new Promise<void>((resolve, reject) => {
				worker.on('error', reject);
				worker.on('message', event => {
					try {
						if (event.started) {
							resolve();
						} else if (event.done) {
							resolver.resolve();
						}
					} catch (error) {
						reject(error);
					}
				});
				worker.on('exit', () => resolver.resolve());
			});

			worker.postMessage({ addManyEntries: true, count: 1000 });

			for (let i = 0; i < 1000; i++) {
				const log = db.useLog('foo');
				await db.transaction(async (txn) => {
					log.addEntry(Buffer.from('hello'), txn.id);
				});
				if (i > 0 && i % 10 === 0) {
					db.purgeLogs({ destroy: true });
				}
			}

			await resolver.promise;

			resolver = withResolvers<void>();
			worker.postMessage({ close: true });

			if (process.versions.deno) {
				// deno doesn't emit an `exit` event when the worker quits, but
				// `terminate()` will trigger the `exit` event
				await delay(100);
				worker.terminate();
			}

			await resolver.promise;
		}), 60000);
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
