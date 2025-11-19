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
	BLOCK_HEADER_SIZE,
	BLOCK_SIZE,
	CONTINUATION_FLAG,
	FILE_HEADER_SIZE,
	TXN_HEADER_SIZE,
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
			expect(info.size).toBe(FILE_HEADER_SIZE + BLOCK_HEADER_SIZE + TXN_HEADER_SIZE + 10);
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
			expect(info.size).toBe(FILE_HEADER_SIZE + BLOCK_HEADER_SIZE + (TXN_HEADER_SIZE + 10) * 3);
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

		it('should add several entries', () => dbRunner(async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			const value = Buffer.alloc(100, 'a');

			await db.transaction(async (txn) => {
				for (let i = 0; i < 1000; i++) {
					log.addEntry(value, txn.id);
				}
			});

			const logPath = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const info = parseTransactionLog(logPath);
			expect(info.size).toBe(FILE_HEADER_SIZE + (BLOCK_HEADER_SIZE * 28) + (TXN_HEADER_SIZE + 100) * 1000);
			expect(info.version).toBe(1);
			expect(info.blockSize).toBe(4096);
			expect(info.blockCount).toBe(28);
			expect(info.entries.length).toBe(1000);
		}));

		it('should add a large entry across two blocks', () => dbRunner({
			dbOptions: [{ transactionLogMaxSize: 10000 }]
		}, async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			const value = Buffer.alloc(5000, 'a');

			await db.transaction(async (txn) => {
				log.addEntry(value, txn.id);
			});

			const logPath = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const info = parseTransactionLog(logPath);
			expect(info.size).toBe(FILE_HEADER_SIZE + (BLOCK_HEADER_SIZE * 2) + TXN_HEADER_SIZE + 5000);
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
			expect(info.size).toBe(FILE_HEADER_SIZE + (BLOCK_HEADER_SIZE * 3) + TXN_HEADER_SIZE + 10000);
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
			expect(info.size).toBe(FILE_HEADER_SIZE + (BLOCK_HEADER_SIZE * 2) + (TXN_HEADER_SIZE + 10) + (TXN_HEADER_SIZE + 5000));
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
			const valueALength = 4096 - BLOCK_HEADER_SIZE - TXN_HEADER_SIZE - 4;
			const valueA = Buffer.alloc(valueALength, 'a');
			const valueB = Buffer.alloc(100, 'b');

			await db.transaction(async (txn) => {
				log.addEntry(valueA, txn.id);
				log.addEntry(valueB, txn.id);
			});

			const logPath = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const info = parseTransactionLog(logPath);
			expect(info.size).toBe(FILE_HEADER_SIZE + (BLOCK_HEADER_SIZE * 2) + (TXN_HEADER_SIZE + valueALength) + (TXN_HEADER_SIZE + 100));
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

			const txnSize = (TXN_HEADER_SIZE + 10000) * 2000;
			const numBlocks = Math.ceil(txnSize / (BLOCK_SIZE - BLOCK_HEADER_SIZE));
			const totalSize = FILE_HEADER_SIZE + (BLOCK_HEADER_SIZE * numBlocks) + txnSize;
			const logStorePath = join(dbPath, 'transaction_logs', 'foo');
			const logFiles = await readdir(logStorePath);
			expect(logFiles.sort()).toEqual(['foo.1.txnlog']);
			expect(statSync(join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog')).size).toBe(totalSize);
		}), 60000);

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
			expect(info.size).toBe(FILE_HEADER_SIZE + BLOCK_HEADER_SIZE + ((TXN_HEADER_SIZE + 10) * 2));
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

			const log1Path = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const log2Path = join(dbPath, 'transaction_logs', 'foo', 'foo.2.txnlog');
			const log3Path = join(dbPath, 'transaction_logs', 'foo', 'foo.3.txnlog');
			const info1 = parseTransactionLog(log1Path);
			const info2 = parseTransactionLog(log2Path);
			const info3 = parseTransactionLog(log3Path);

			expect(info1.size).toBe(1000);
			expect(info1.blocks.length).toBe(1);
			expect(info1.blocks[0].dataOffset).toBe(0);
			expect(info1.entries.length).toBe(9);
			for (let i = 0; i < info1.entries.length - 1; i++) {
				expect(info1.entries[i].length).toBe(100);
				expect(info1.entries[i].data).toEqual(Buffer.alloc(100, 'a'));
			}
			expect(info1.entries[info1.entries.length - 1].length).toBe(70);
			expect(info1.entries[info1.entries.length - 1].data).toEqual(Buffer.alloc(70, 'a'));
			expect(info1.entries[info1.entries.length - 1].partial).toBe(true);

			expect(info2.size).toBe(1000);
			expect(info2.blocks.length).toBe(1);
			expect(info2.blocks[0].dataOffset).toBe(30);
			expect(info2.entries.length).toBe(10);
			expect(info2.entries[0].length).toBe(30);
			expect(info2.entries[0].data).toEqual(Buffer.alloc(30, 'a'));
			expect(info2.entries[0].continuation).toBe(true);
			for (let i = 1; i < info2.entries.length - 1; i++) {
				expect(info2.entries[i].length).toBe(100);
				expect(info2.entries[i].data).toEqual(Buffer.alloc(100, 'a'));
			}
			expect(info2.entries[info2.entries.length - 1].length).toBe(40);
			expect(info2.entries[info2.entries.length - 1].data).toEqual(Buffer.alloc(40, 'a'));

			expect(info3.size).toBe(866);
			expect(info3.blocks.length).toBe(1);
			expect(info3.blocks[0].dataOffset).toBe(60);
			expect(info3.entries.length).toBe(8);
			expect(info3.entries[0].length).toBe(60);
			expect(info3.entries[0].data).toEqual(Buffer.alloc(60, 'a'));
			expect(info3.entries[0].continuation).toBe(true);
			for (let i = 1; i < info3.entries.length; i++) {
				expect(info3.entries[i].length).toBe(100);
				expect(info3.entries[i].data).toEqual(Buffer.alloc(100, 'a'));
			}
		}));

		it('should rotate if not enough room for the next transaction header', () => dbRunner({
			dbOptions: [{ transactionLogMaxSize: 1000 }],
		}, async ({ db, dbPath }) => {
			const log = db.useLog('foo');

			for (let i = 0; i < 2; i++) {
				await db.transaction(async (txn) => {
					log.addEntry(Buffer.alloc(963, 'a'), txn.id);
				});
			}

			const logStorePath = join(dbPath, 'transaction_logs', 'foo');
			const logFiles = await readdir(logStorePath);
			expect(logFiles.sort()).toEqual(['foo.1.txnlog', 'foo.2.txnlog']);

			const log1Path = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const log2Path = join(dbPath, 'transaction_logs', 'foo', 'foo.2.txnlog');
			const info1 = parseTransactionLog(log1Path);
			const info2 = parseTransactionLog(log2Path);

			expect(info1.size).toBe(997);
			expect(info1.blocks.length).toBe(1);
			expect(info1.blocks[0].dataOffset).toBe(0);
			expect(info1.entries.length).toBe(1);
			expect(info1.entries[0].length).toBe(963);
			expect(info1.entries[0].data).toEqual(Buffer.alloc(963, 'a'));

			expect(info2.size).toBe(997);
			expect(info2.blocks.length).toBe(1);
			expect(info2.blocks[0].dataOffset).toBe(0);
			expect(info2.entries.length).toBe(1);
			expect(info2.entries[0].length).toBe(963);
			expect(info2.entries[0].data).toEqual(Buffer.alloc(963, 'a'));
		}));

		it('should rotate if room for the transaction header, but not the entry', () => dbRunner({
			dbOptions: [{ transactionLogMaxSize: 1000 }],
		}, async ({ db, dbPath }) => {
			const log = db.useLog('foo');

			// fill up the first file with just enough space for the next
			// transaction header
			const targetSize = 1000 - FILE_HEADER_SIZE - BLOCK_HEADER_SIZE - (TXN_HEADER_SIZE * 2);
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

			expect(info1.size).toBe(1000);
			expect(info1.blocks.length).toBe(1);
			expect(info1.blocks[0].dataOffset).toBe(0);
			expect(info1.entries.length).toBe(2);
			expect(info1.entries[0].length).toBe(targetSize);
			expect(info1.entries[0].data).toEqual(targetData);
			expect(info1.entries[1].length).toBe(0);
			expect(info1.entries[1].data).toEqual(Buffer.alloc(0));
			expect(info1.entries[1].partial).toBe(true);

			expect(info2.size).toBe(FILE_HEADER_SIZE + BLOCK_HEADER_SIZE + 100);
			expect(info2.blocks.length).toBe(1);
			expect(info2.blocks[0].dataOffset).toBe(100);
			expect(info2.entries.length).toBe(1);
			expect(info2.entries[0].length).toBe(100);
			expect(info2.entries[0].data).toEqual(Buffer.alloc(100, 'a'));
			expect(info2.entries[0].continuation).toBe(true);
		}));

		it('should split entry and rotate if not enough room for the entire transaction', () => dbRunner({
			dbOptions: [{ transactionLogMaxSize: 1000 }],
		}, async ({ db, dbPath }) => {
			const log = db.useLog('foo');

			await db.transaction(async (txn) => {
				log.addEntry(Buffer.alloc(1024, 'a'), txn.id);
			});

			const logStorePath = join(dbPath, 'transaction_logs', 'foo');
			const logFiles = await readdir(logStorePath);
			expect(logFiles.sort()).toEqual(['foo.1.txnlog', 'foo.2.txnlog']);

			const log1Path = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const log2Path = join(dbPath, 'transaction_logs', 'foo', 'foo.2.txnlog');
			const info1 = parseTransactionLog(log1Path);
			const info2 = parseTransactionLog(log2Path);

			expect(info1.size).toBe(1000);
			expect(info1.blocks.length).toBe(1);
			expect(info1.blocks[0].dataOffset).toBe(0);
			expect(info1.entries.length).toBe(1);
			expect(info1.entries[0].length).toBe(966);
			expect(info1.entries[0].data).toEqual(Buffer.alloc(966, 'a'));
			expect(info1.entries[0].partial).toBe(true);

			expect(info2.size).toBe(80);
			expect(info2.blocks.length).toBe(1);
			expect(info2.blocks[0].dataOffset).toBe(58);
			expect(info2.entries.length).toBe(1);
			expect(info2.entries[0].length).toBe(58);
			expect(info2.entries[0].data).toEqual(Buffer.alloc(58, 'a'));
			expect(info2.entries[0].continuation).toBe(true);
		}));

		it('should split huge entry and rotate across multiple files', () => dbRunner({
			dbOptions: [{ transactionLogMaxSize: 1000 }],
		}, async ({ db, dbPath }) => {
			const log = db.useLog('foo');

			await db.transaction(async (txn) => {
				log.addEntry(Buffer.alloc(3500, 'a'), txn.id);
			});

			const logStorePath = join(dbPath, 'transaction_logs', 'foo');
			const logFiles = await readdir(logStorePath);
			expect(logFiles.sort()).toEqual(['foo.1.txnlog', 'foo.2.txnlog', 'foo.3.txnlog', 'foo.4.txnlog']);

			const log1Path = join(dbPath, 'transaction_logs', 'foo', 'foo.1.txnlog');
			const log2Path = join(dbPath, 'transaction_logs', 'foo', 'foo.2.txnlog');
			const log3Path = join(dbPath, 'transaction_logs', 'foo', 'foo.3.txnlog');
			const log4Path = join(dbPath, 'transaction_logs', 'foo', 'foo.4.txnlog');
			const info1 = parseTransactionLog(log1Path);
			const info2 = parseTransactionLog(log2Path);
			const info3 = parseTransactionLog(log3Path);
			const info4 = parseTransactionLog(log4Path);

			expect(info1.size).toBe(1000);
			expect(info1.blocks.length).toBe(1);
			expect(info1.blocks[0].dataOffset).toBe(0);
			expect(info1.entries.length).toBe(1);
			expect(info1.entries[0].length).toBe(966);
			expect(info1.entries[0].data).toEqual(Buffer.alloc(966, 'a'));
			expect(info1.entries[0].partial).toBe(true);

			expect(info2.size).toBe(1000);
			expect(info2.blocks.length).toBe(1);
			expect(info2.blocks[0].dataOffset).toBe(2534);
			expect(info2.entries.length).toBe(1);
			expect(info2.entries[0].length).toBe(978);
			expect(info2.entries[0].data).toEqual(Buffer.alloc(978, 'a'));
			expect(info2.entries[0].continuation).toBe(true);

			expect(info3.size).toBe(1000);
			expect(info3.blocks.length).toBe(1);
			expect(info3.blocks[0].dataOffset).toBe(1556);
			expect(info3.entries.length).toBe(1);
			expect(info3.entries[0].length).toBe(978);
			expect(info3.entries[0].data).toEqual(Buffer.alloc(978, 'a'));
			expect(info3.entries[0].continuation).toBe(true);

			expect(info4.size).toBe(600);
			expect(info4.blocks.length).toBe(1);
			expect(info4.blocks[0].dataOffset).toBe(578);
			expect(info4.entries.length).toBe(1);
			expect(info4.entries[0].length).toBe(578);
			expect(info4.entries[0].data).toEqual(Buffer.alloc(578, 'a'));
			expect(info4.entries[0].continuation).toBe(true);
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
