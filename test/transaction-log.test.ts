import { describe, expect, it } from 'vitest';
import { createWorkerBootstrapScript, dbRunner } from './lib/util.js';
import { mkdir, readdir, writeFile, utimes } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { existsSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { withResolvers } from '../src/util.js';
import { Worker } from 'node:worker_threads';
import assert from 'node:assert';
import { constants, type TransactionLog } from '../src/load-binding.js';
import { parseTransactionLog } from '../src/parse-transaction-log.js';

const {
	TRANSACTION_LOG_FILE_HEADER_SIZE,
	TRANSACTION_LOG_ENTRY_HEADER_SIZE,
} = constants;

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

		it('should error if log already bound to a transaction', () => dbRunner(async ({ db }) => {
			const log1 = db.useLog('log1');
			const log2 = db.useLog('log2');
			await db.transaction(async (txn) => {
				log1.addEntry(Buffer.from('hello'), txn.id);
				log1.addEntry(Buffer.from('world'), txn.id);
				expect(() => log2.addEntry(Buffer.from('nope'), txn.id)).toThrowError(new Error('Log already bound to a transaction'));
			});

			await db.transaction(async (txn) => {
				txn.useLog('log3');
				txn.useLog('log3'); // do it twice
				expect(() => txn.useLog('log4')).toThrowError(new Error('Log already bound to a transaction'));
			});
		}));
	});

	describe('addEntry()', () => {
		it('should add a single small entry within a single block', () => dbRunner(async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			const value = Buffer.alloc(10, 'a');

			await db.transaction(async (txn) => {
				log.addEntry(value, txn.id);
			});

			const logPath = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const info = parseTransactionLog(logPath);
			expect(info.size).toBe(TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10);
			expect(info.version).toBe(1);
			expect(info.entries.length).toBe(1);
			expect(info.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[0].length).toBe(10);
			expect(info.entries[0].data).toEqual(value);
		}));

		it('should add multiple small entries within a single file', () => dbRunner(async ({ db, dbPath }) => {
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
			expect(info.size).toBe(TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10) * 3);
			expect(info.version).toBe(1);
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

		it('should rotate to next sequence number', () => dbRunner({
			dbOptions: [{ transactionLogMaxSize: 1000 }],
		}, async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			const value = Buffer.alloc(100, 'a');

			for (let i = 0; i < 20; i++) {
				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
				});
			}

			const logStorePath = join(dbPath, 'transaction_logs', 'foo');
			const logFiles = await readdir(logStorePath);
			expect(logFiles.sort()).toEqual(['foo.1.txnlog', 'foo.2.txnlog', 'foo.3.txnlog']);

			const log1Path = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const log2Path = join(dbPath, 'transaction_logs', 'foo', 'foo.2.txnlog');
			const log3Path = join(dbPath, 'transaction_logs', 'foo', 'foo.3.txnlog');
			const info1 = parseTransactionLog(log1Path);
			const info2 = parseTransactionLog(log2Path);
			const info3 = parseTransactionLog(log3Path);

			expect(info1.size).toBe(TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 100) * 8);
			expect(info1.entries.length).toBe(8);
			for (const { length, data } of info1.entries) {
				expect(length).toBe(100);
				expect(data).toEqual(value);
			}

			expect(info2.size).toBe(TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 100) * 8);
			expect(info2.entries.length).toBe(8);
			for (const { length, data } of info2.entries) {
				expect(length).toBe(100);
				expect(data).toEqual(value);
			}

			expect(info3.size).toBe(TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 100) * 4);
			expect(info3.entries.length).toBe(4);
			for (const { length, data } of info3.entries) {
				expect(length).toBe(100);
				expect(data).toEqual(value);
			}
		}));

		it('should be able to rotate with entries that span a transaction', () => dbRunner({
			dbOptions: [{ transactionLogMaxSize: 1000 }],
		}, async ({ db, dbPath }) => {
			let log = db.useLog('foo');
			const value = Buffer.alloc(100, 'a');
			await db.transaction(async (txn) => {
				log.addEntry(value, txn.id);
			});
			for (let i = 0; i < 20; i++) {
				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
					log.addEntry(value, txn.id);
				});
			}
		}));

		it('should allow unlimited transaction log size', () => dbRunner({
			dbOptions: [{ transactionLogMaxSize: 0 }],
		}, async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			const value = Buffer.alloc(10000, 'a');

			for (let i = 0; i < 2000; i++) {
				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
				});
			}

			const totalSize = TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10000) * 2000;
			const logStorePath = join(dbPath, 'transaction_logs', 'foo');
			const logFiles = await readdir(logStorePath);
			expect(logFiles).toEqual(['foo.1.txnlog']);
			expect(statSync(join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog')).size).toBe(totalSize);
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

		it('should add multiple entries from separate transactions', () => dbRunner(async ({ db, dbPath }) => {
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
			expect(info.size).toBe(TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10) * 2);
			expect(info.version).toBe(1);
			expect(info.entries.length).toBe(2);
			expect(info.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[0].length).toBe(10);
			expect(info.entries[0].data).toEqual(valueA);
			expect(info.entries[1].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[1].length).toBe(10);
			expect(info.entries[1].data).toEqual(valueB);
		}));

		it('should rotate if not enough room for the next transaction header', () => dbRunner({
			dbOptions: [{ transactionLogMaxSize: 1000 }],
		}, async ({ db, dbPath }) => {
			const log = db.useLog('foo');

			for (let i = 0; i < 2; i++) {
				await db.transaction(async (txn) => {
					log.addEntry(Buffer.alloc(990, 'a'), txn.id);
				});
			}

			const logStorePath = join(dbPath, 'transaction_logs', 'foo');
			const logFiles = await readdir(logStorePath);
			expect(logFiles.sort()).toEqual(['foo.1.txnlog', 'foo.2.txnlog']);

			const log1Path = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const log2Path = join(dbPath, 'transaction_logs', 'foo', 'foo.2.txnlog');
			const info1 = parseTransactionLog(log1Path);
			const info2 = parseTransactionLog(log2Path);

			expect(info1.size).toBe(TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 990);
			expect(info1.entries.length).toBe(1);
			expect(info1.entries[0].length).toBe(990);
			expect(info1.entries[0].data).toEqual(Buffer.alloc(990, 'a'));

			expect(info2.size).toBe(TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 990);
			expect(info2.entries.length).toBe(1);
			expect(info2.entries[0].length).toBe(990);
			expect(info2.entries[0].data).toEqual(Buffer.alloc(990, 'a'));
		}));

		it('should rotate if room for the transaction header, but not the entry', () => dbRunner({
			dbOptions: [{ transactionLogMaxSize: 1000 }],
		}, async ({ db, dbPath }) => {
			const log = db.useLog('foo');

			// fill up the first file with just enough space for the next
			// transaction header
			const targetSize = 1000 - TRANSACTION_LOG_ENTRY_HEADER_SIZE;
			const targetData = Buffer.alloc(targetSize, 'a');
			await db.transaction(async (txn) => {
				log.addEntry(targetData, txn.id);
			});

			// add a second entry which writes the header to the first file and
			// continues in the second file
			await db.transaction(async (txn) => {
				log.addEntry(Buffer.alloc(100, 'a'), txn.id);
			});

			const logStorePath = join(dbPath, 'transaction_logs', 'foo');
			const logFiles = await readdir(logStorePath);
			expect(logFiles.sort()).toEqual(['foo.1.txnlog', 'foo.2.txnlog']);

			const log1Path = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const log2Path = join(dbPath, 'transaction_logs', 'foo', 'foo.2.txnlog');
			const info1 = parseTransactionLog(log1Path);
			const info2 = parseTransactionLog(log2Path);

			expect(info1.size).toBe(TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + targetSize);
			expect(info1.entries.length).toBe(1);
			expect(info1.entries[0].length).toBe(targetSize);
			expect(info1.entries[0].data).toEqual(targetData);

			expect(info2.size).toBe(TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 100);
			expect(info2.entries.length).toBe(1);
			expect(info2.entries[0].length).toBe(100);
			expect(info2.entries[0].data).toEqual(Buffer.alloc(100, 'a'));
		}));

		it('should write to same log from multiple workers', () => dbRunner(async ({ db, dbPath }) => {
			const worker = new Worker(
				createWorkerBootstrapScript('./test/workers/transaction-log-worker.mts'),
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

		it('should rotate if file exceeds max age threshold', () => dbRunner({
			dbOptions: [{
				transactionLogRetention: 2000,
				transactionLogMaxAgeThreshold: 0.9
			}],
		}, async ({ db, dbPath }) => {
			// we need to add the entry within 3 seconds
			const log = db.useLog('foo');
			await db.transaction(async (txn) => {
				log.addEntry(Buffer.alloc(10, 'a'), txn.id);
			});

			await delay(200);

			// we should be in the second half of the retention period and
			// trigger a new file rotation
			await db.transaction(async (txn) => {
				log.addEntry(Buffer.alloc(10, 'a'), txn.id);
			});

			const logStorePath = join(dbPath, 'transaction_logs', 'foo');
			const logFiles = await readdir(logStorePath);
			expect(logFiles.sort()).toEqual(['foo.1.txnlog', 'foo.2.txnlog']);

			const log1Path = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const log2Path = join(dbPath, 'transaction_logs', 'foo', 'foo.2.txnlog');
			const info1 = parseTransactionLog(log1Path);
			const info2 = parseTransactionLog(log2Path);

			expect(info1.size).toBe(TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10);
			expect(info1.version).toBe(1);
			expect(info1.entries.length).toBe(1);
			expect(info1.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 10000);
			expect(info1.entries[0].length).toBe(10);
			expect(info1.entries[0].data).toEqual(Buffer.alloc(10, 'a'));

			expect(info2.size).toBe(TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10);
			expect(info2.version).toBe(1);
			expect(info2.entries.length).toBe(1);
			expect(info2.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info2.entries[0].length).toBe(10);
			expect(info2.entries[0].data).toEqual(Buffer.alloc(10, 'a'));
		}));

		it('should append to existing log file', () => dbRunner(async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			const valueA = Buffer.alloc(10, 'a');
			const valueB = Buffer.alloc(10, 'b');

			await db.transaction(async (txn) => {
				log.addEntry(valueA, txn.id);
			});

			const logPath = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			let info = parseTransactionLog(logPath);
			expect(info.size).toBe(TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10);
			expect(info.version).toBe(1);
			expect(info.entries.length).toBe(1);
			expect(info.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[0].length).toBe(10);
			expect(info.entries[0].data).toEqual(valueA);

			db.close();

			db.open();
			await db.transaction(async (txn) => {
				log.addEntry(valueB, txn.id);
			});

			info = parseTransactionLog(logPath);
			expect(info.size).toBe(TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10) * 2);
			expect(info.version).toBe(1);
			expect(info.entries.length).toBe(2);
			expect(info.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 10000);
			expect(info.entries[0].length).toBe(10);
			expect(info.entries[0].data).toEqual(valueA);
			expect(info.entries[1].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
			expect(info.entries[1].length).toBe(10);
			expect(info.entries[1].data).toEqual(valueB);
		}));

		it('should error if the log name is invalid', () => dbRunner(async ({ db }) => {
			expect(() => db.useLog(undefined as any)).toThrowError(new TypeError('Log name must be a string or number'));
			expect(() => db.useLog([] as any)).toThrowError(new TypeError('Log name must be a string or number'));
			await expect(db.transaction(txn => txn.useLog(undefined as any))).rejects.toThrowError(new TypeError('Log name must be a string or number'));
		}));

		it('should error if entry data is invalid', () => dbRunner(async ({ db }) => {
			const log = db.useLog('foo');
			await db.transaction(async (txn) => {
				expect(() => log.addEntry(undefined as any, txn.id)).toThrowError(new TypeError('Invalid log entry, expected a Buffer or ArrayBuffer'));
				expect(() => log.addEntry([] as any, txn.id)).toThrowError(new TypeError('Invalid log entry, expected a Buffer or ArrayBuffer'));
			});
		}));

		it('should error if transaction id is invalid', () => dbRunner(async ({ db }) => {
			const log = db.useLog('foo');
			await db.transaction(async (_txn) => {
				expect(() => log.addEntry(Buffer.from('hello'), undefined as any)).toThrowError(new TypeError('Missing argument, transaction id is required'));
				expect(() => log.addEntry(Buffer.from('hello'), [] as any)).toThrowError(new TypeError('Invalid argument, transaction id must be a non-negative integer'));
			});
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
