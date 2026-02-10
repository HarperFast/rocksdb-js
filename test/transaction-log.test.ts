import assert from 'node:assert';
import { existsSync, readFileSync, statSync } from 'node:fs';
import { mkdir, readdir, stat, utimes, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';
import { describe, expect, it } from 'vitest';
import { RocksDatabase, Transaction } from '../src/index.js';
import { constants, type TransactionLog } from '../src/load-binding.js';
import { parseTransactionLog } from '../src/parse-transaction-log.js';
import { withResolvers } from '../src/util.js';
import { createWorkerBootstrapScript, dbRunner, generateDBPath } from './lib/util.js';

const { TRANSACTION_LOG_FILE_HEADER_SIZE, TRANSACTION_LOG_ENTRY_HEADER_SIZE } = constants;

describe('Transaction Log', () => {
	describe('useLog()', () => {
		it.only('should detect existing transaction logs', () =>
			dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
				await mkdir(join(dbPath, 'transaction_logs', 'foo'), { recursive: true });
				await writeFile(join(dbPath, 'transaction_logs', 'foo', '1.txnlog'), '');

				db.open();

				expect(db.listLogs()).toEqual(['foo']);

				const fooLog = db.useLog('foo');
				expect(fooLog).toBeDefined();
				expect(fooLog.path).toBe(join(dbPath, 'transaction_logs', 'foo'));

				const barLog = db.useLog('bar');
				expect(barLog).toBeDefined();
				expect(barLog).not.toBe(fooLog);

				const fooLog2 = db.useLog('foo');
				expect(fooLog2).toBe(fooLog);

				expect(db.listLogs()).toEqual(['bar', 'foo']);
			}));

		it('should support numeric log names', () =>
			dbRunner(async ({ db }) => {
				db.open();

				expect(db.listLogs()).toEqual([]);

				const fooLog = db.useLog(612);
				expect(fooLog).toBeDefined();

				const fooLog2 = db.useLog(612);
				expect(fooLog2).toBe(fooLog);

				expect(db.listLogs()).toEqual(['612']);
			}));

		(globalThis.gc ? it : it.skip)(
			'should cleanup transaction log instance on GC',
			() =>
				dbRunner(async ({ db }) => {
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
				})
		);

		it('should error if log already bound to a transaction', () =>
			dbRunner(async ({ db }) => {
				const log1 = db.useLog('log1');
				const log2 = db.useLog('log2');
				await db.transaction(async (txn) => {
					log1.addEntry(Buffer.from('hello'), txn.id);
					log1.addEntry(Buffer.from('world'), txn.id);
					expect(() => log2.addEntry(Buffer.from('nope'), txn.id)).toThrowError(
						new Error('Log already bound to a transaction')
					);
				});

				await db.transaction(async (txn) => {
					txn.useLog('log3');
					txn.useLog('log3'); // do it twice
					expect(() => txn.useLog('log4')).toThrowError(
						new Error('Log already bound to a transaction')
					);
				});
			}));

		it('should isolate transaction logs between different database paths', () =>
			dbRunner(
				{ dbOptions: [{ path: generateDBPath() }, { path: generateDBPath() }] },
				async ({ db, dbPath }, { db: db2, dbPath: dbPath2 }) => {
					expect(dbPath).not.toBe(dbPath2);

					const value = Buffer.alloc(10000, 'a');
					const log1 = db.useLog('foo');
					for (let i = 0; i < 20; i++) {
						await db.transaction(async (txn) => {
							log1.addEntry(value, txn.id);
						});
					}

					expect(db.listLogs()).toContain('foo');
					expect(db2.listLogs()).not.toContain('foo');

					const getSize = async (logPath: string) => {
						let size = 0;
						for (const file of await readdir(logPath)) {
							const info = await stat(join(logPath, file)).catch(() => undefined);
							if (info) {
								size += info.size;
							}
						}
						return size;
					};

					let size = await getSize(join(dbPath, 'transaction_logs', 'foo'));
					expect(size).toBe(
						TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10000) * 20
					);

					const log2 = db2.useLog('foo');
					for (let i = 0; i < 20; i++) {
						await db2.transaction(async (txn) => {
							log2.addEntry(value, txn.id);
						});
					}

					size = await getSize(join(dbPath, 'transaction_logs', 'foo'));
					expect(size).toBe(
						TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10000) * 20
					);

					size = await getSize(join(dbPath2, 'transaction_logs', 'foo'));
					expect(size).toBe(
						TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10000) * 20
					);
				}
			));
	});

	describe('_getLastCommittedPosition()/_getMemoryMapOfFile', () => {
		it('should get a list of sequence files and get a memory map', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('foo-seq');
				const value = Buffer.alloc(10, 'a');

				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
				});
				const positionBuffer = log._getLastCommittedPosition();
				const dataView = new DataView(positionBuffer.buffer);
				expect(dataView.getUint32(0)).toBeGreaterThan(10);
				const sequenceNumber = dataView.getUint32(1);
				expect(sequenceNumber).toBe(1);

				const buffer = log._getMemoryMapOfFile(1);
				expect(buffer).toBeDefined();
				expect(buffer?.subarray(0, 4).toString()).toBe('WOOF');
			}));
	});

	describe('Transaction log visibility after commits', () => {
		it('Should not treat transaction logs as visible until successfully committed', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('foo');
				db.putSync('key1', 'value1');
				let firstTransactionCompletions: Promise<void>[] = [];
				let fullTransactionCompletions: Promise<void>[] = [];
				for (let i = 0; i < 3; i++) {
					let transaction = new Transaction(db.store);
					let firstTxnCommit = (async () => {
						db.getSync('key1', { transaction });
						const value = Buffer.alloc(10, i.toString());
						log.addEntry(value, transaction.id);
						await delay(10);
						// should be a conflicted write
						db.putSync('key1', 'updated' + i, { transaction });
						await transaction.commit();
					})();
					firstTransactionCompletions.push(firstTxnCommit);
					const catchFailedCommit = async (error) => {
						if (error.code === 'ERR_BUSY') {
							db.getSync('key1', { transaction });
							await delay(10);
							db.putSync('key1', 'updated' + i, { transaction });
							await transaction.commit().catch(catchFailedCommit);
						} else {
							throw error;
						}
					};
					let fullTxnCompletion = firstTxnCommit.catch(catchFailedCommit);

					fullTransactionCompletions.push(fullTxnCompletion);
				}
				let transactionResults = await Promise.allSettled(firstTransactionCompletions);
				expect(transactionResults.filter(result => result.status === 'rejected').length)
					.toBeGreaterThanOrEqual(1); // at least one should fail
				expect(Array.from(log.query({ start: 0 })).length).toBeLessThan(3); // The entries should not be all visible at this point (only one)
				await Promise.all(fullTransactionCompletions); // wait for all the retries to finish
				expect(Array.from(log.query({ start: 0 })).length).toBe(3); // now all the transactions should be visible in the log
			}));
	});

	describe('query() from TransactionLog', () => {
		it('should query an empty transaction log', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('foo');
				const queryIterable = log.query({ start: 1 });
				const queryResults = Array.from(queryIterable);
				expect(queryResults.length).toBe(0);
			}));

		it('should query a transaction log', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('foo');
				const value = Buffer.alloc(10, 'a');
				const startTime = Date.now() - 1000;
				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
				});

				const queryIterable = log.query({ start: startTime, end: Date.now() + 1000 });
				const queryResults = Array.from(queryIterable);
				expect(queryResults.length).toBe(1);
			}));

		it('should query a transaction log with different options', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('foo');
				const value = Buffer.alloc(10, 'a');
				for (let i = 0; i < 5; i++) {
					await db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
					});
				}
				let allTimestamps = Array.from(log.query({ start: 0 })).map(({ timestamp }) => timestamp);
				expect(Array.from(log.query({ start: allTimestamps[1] })).length).toBe(4);
				expect(Array.from(log.query({ start: allTimestamps[1], exclusiveStart: true })).length)
					.toBe(3);
				expect(Array.from(log.query({ start: allTimestamps[1], exactStart: true })).length).toBe(4);
				expect(
					Array.from(
						log.query({ start: allTimestamps[1], exactStart: true, end: allTimestamps[4] })
					).length
				).toBe(3);
			}));

		it('should query an out-of-order transaction log with different options', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('foo');
				const value = Buffer.alloc(10, 'a');
				const start = Date.now();
				for (let i = 0; i < 5; i++) {
					await db.transaction(async (txn) => {
						txn.setTimestamp(start - i);
						log.addEntry(value, txn.id);
					});
				}
				expect(Array.from(log.query({ start: start - 1 })).length).toBe(2);
				expect(Array.from(log.query({ start: start - 1, exclusiveStart: true })).length).toBe(1);
				expect(Array.from(log.query({ start: start - 1, exactStart: true })).length).toBe(4);
				expect(
					Array.from(log.query({ start: start - 1, exactStart: true, exclusiveStart: true })).length
				).toBe(3);
				expect(Array.from(log.query({ start: start - 1, exactStart: true, end: start - 2 })).length)
					.toBe(3);
			}));

		it('should query a transaction log with multiple log instances', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('foo');
				const value = Buffer.alloc(10, 'a');
				const startTime = Date.now() - 1000;
				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
				});
				const log2 = db.useLog('foo');

				let queryResults = Array.from(log.query({ start: startTime, end: Date.now() + 1000 }));
				expect(queryResults.length).toBe(1);
				queryResults = Array.from(log2.query({ start: startTime, end: Date.now() + 1000 }));
				expect(queryResults.length).toBe(1);
				queryResults = Array.from(log2.query({ start: startTime, end: Date.now() + 1000 }));
				expect(queryResults.length).toBe(1);
				expect(queryResults[0].data).toEqual(value);
				expect(queryResults[0].endTxn).toBe(true);
			}));

		it('should query a transaction log after re-opening database', () =>
			dbRunner(async ({ db, dbPath }) => {
				try {
					let log = db.useLog('foo');
					const value = Buffer.alloc(10, 'a');
					const startTime = Date.now() - 1000;
					await db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
					});
					let queryResults = Array.from(log.query({ start: startTime, end: Date.now() + 1000 }));
					expect(queryResults.length).toBe(1);
					db.close();
					db = RocksDatabase.open(dbPath);
					let log2 = db.useLog('foo');
					log._getMemoryMapOfFile(1);
					let queryResults2 = Array.from(
						log2.query({ start: startTime, end: Date.now() + 1000, readUncommitted: true })
					);
					expect(queryResults2.length).toBe(1);
					queryResults = Array.from(log.query({ start: startTime, end: Date.now() + 1000 }));
					expect(queryResults.length).toBe(1);
				} finally {
					db.close();
				}
			}));

		it('should be able to reuse a query iterator to resume reading a transaction log', () =>
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, async ({ db }) => {
				const log = db.useLog('foo');
				const value = Buffer.alloc(100, 'a');
				for (let i = 0; i < 10; i++) {
					const queryIterator = log.query({});
					const queryIterator2 = log.query({ start: 0 });
					await db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
					});
					expect(Array.from(queryIterator).length).toBe(1); // this should be starting after the last commit
					expect(Array.from(queryIterator2).length).toBe(i * 11 + 1); // this should be starting after the last commit

					let count = 0;
					let count2 = 0;
					for (let j = 0; j < 10; j++) {
						const txnPromise = db.transaction(async (txn) => {
							log.addEntry(value, txn.id);
						});
						count += Array.from(queryIterator).length;
						count2 += Array.from(queryIterator2).length;
						await txnPromise;
					}
					count += Array.from(queryIterator).length;
					count2 += Array.from(queryIterator2).length;
					expect(count).toBe(10);
					expect(count2).toBe(10);
				}
			}));

		it('should be able to reuse a query iterator to resume reading a transaction log with multiple entries', () =>
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, async ({ db }) => {
				let log = db.useLog('foo');
				const value = Buffer.alloc(100, 'a');
				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
				});
				let queryIterator = log.query({});
				let queryIterator2 = log.query({ start: 0 });
				expect(Array.from(queryIterator).length).toBe(0); // this should be starting after the last commit
				expect(Array.from(queryIterator2).length).toBe(1); // this should be starting after the last commit
				let count = 0;
				let count2 = 0;
				for (let i = 0; i < 200; i++) {
					let txnPromise = db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
						log.addEntry(value, txn.id);
					});
					count += Array.from(queryIterator).length;
					count2 += Array.from(queryIterator2).length;
					await txnPromise;
				}
				count += Array.from(queryIterator).length;
				count2 += Array.from(queryIterator2).length;
				expect(count).toBe(400);
				expect(count2).toBe(400);
			}));

		it('should be able to reuse a query iterator that starts after the latest log', () =>
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, async ({ db }) => {
				let log = db.useLog('foo');
				const value = Buffer.alloc(100, 'a');
				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
				});
				let queryIterator = log.query({ start: 0 });
				const start = Array.from(queryIterator)[0].timestamp + 1;
				queryIterator = log.query({ start });
				expect(Array.from(queryIterator).length).toBe(0); // shouldn't return anything because we are staring after last log
				await delay(2);
				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
				});
				expect(Array.from(queryIterator).length).toBe(1); // latest should show up now
			}));
	});

	describe('addEntry()', () => {
		it('should add a single small entry within a single block', () =>
			dbRunner(async ({ db, dbPath }) => {
				const log = db.useLog('foo');
				const value = Buffer.alloc(10, 'a');

				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
				});

				const logPath = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				const info = parseTransactionLog(logPath);
				expect(info.size).toBe(
					TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10
				);
				expect(info.version).toBe(1);
				expect(info.entries.length).toBe(1);
				expect(info.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
				expect(info.entries[0].length).toBe(10);
				expect(info.entries[0].data).toEqual(value);

				const queryResults = Array.from(log.query({ start: 0 }));
				expect(queryResults.length).toBe(1);
				expect(queryResults[0].data).toEqual(value);
				expect(queryResults[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
				expect(queryResults[0].endTxn).toBe(true);
			}));

		it('should add multiple small entries within a single file', () =>
			dbRunner(async ({ db, dbPath }) => {
				const log = db.useLog('foo');
				const valueA = Buffer.alloc(10, 'a');
				const valueB = Buffer.alloc(10, 'b');
				const valueC = Buffer.alloc(10, 'c');
				const startTime = Date.now() - 1000;

				await db.transaction(async (txn) => {
					log.addEntry(valueA, txn.id);
					log.addEntry(valueB, txn.id);
					log.addEntry(valueC, txn.id);
				});

				const logPath = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				const info = parseTransactionLog(logPath);
				expect(info.size).toBe(
					TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10) * 3
				);
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

				const queryResults = Array.from(log.query({ start: startTime, end: Date.now() + 1000 }));
				expect(queryResults.length).toBe(3);
				expect(queryResults[0].data).toEqual(valueA);
				expect(queryResults[0].endTxn).toBe(false);
				expect(queryResults[1].data).toEqual(valueB);
				expect(queryResults[1].endTxn).toBe(false);
				expect(queryResults[2].data).toEqual(valueC);
				expect(queryResults[2].endTxn).toBe(true);
			}));

		it('should rotate to next sequence number', () =>
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, async ({ db, dbPath }) => {
				const log = db.useLog('foo');
				const value = Buffer.alloc(100, 'a');
				const startTime = Date.now() - 1000;

				for (let i = 0; i < 20; i++) {
					await db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
					});
				}

				const logStorePath = join(dbPath, 'transaction_logs', 'foo');
				const logFiles = await readdir(logStorePath);
				expect(logFiles.sort()).toEqual(['1.txnlog', '2.txnlog', '3.txnlog']);
				const queryResults = Array.from(log.query({ start: startTime, end: Date.now() + 1000 }));
				expect(queryResults.length).toBe(20);
				expect(queryResults[0].data).toEqual(value);
				expect(queryResults[1].data).toEqual(value);
				expect(queryResults[19].data).toEqual(value);

				const file1Size = TRANSACTION_LOG_FILE_HEADER_SIZE
					+ (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 100) * 8;
				const file2Size = TRANSACTION_LOG_FILE_HEADER_SIZE
					+ (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 100) * 8;
				const file3Size = TRANSACTION_LOG_FILE_HEADER_SIZE
					+ (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 100) * 4;

				expect(log.getLogFileSize()).toBe(file1Size + file2Size + file3Size);
				expect(() => log.getLogFileSize(0)).toThrow(
					'Expected sequence number to be a positive integer greater than 0'
				);
				expect(log.getLogFileSize(1)).toBe(file1Size);
				expect(log.getLogFileSize(2)).toBe(file2Size);
				expect(log.getLogFileSize(3)).toBe(file3Size);
				expect(log.getLogFileSize(4)).toBe(0);

				const log1Path = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				const log2Path = join(dbPath, 'transaction_logs', 'foo', '2.txnlog');
				const log3Path = join(dbPath, 'transaction_logs', 'foo', '3.txnlog');
				const info1 = parseTransactionLog(log1Path);
				const info2 = parseTransactionLog(log2Path);
				const info3 = parseTransactionLog(log3Path);

				expect(info1.size).toBe(file1Size);
				expect(info1.entries.length).toBe(8);
				for (const { length, data } of info1.entries) {
					expect(length).toBe(100);
					expect(data).toEqual(value);
				}

				expect(info2.size).toBe(file2Size);
				expect(info2.entries.length).toBe(8);
				for (const { length, data } of info2.entries) {
					expect(length).toBe(100);
					expect(data).toEqual(value);
				}

				expect(info3.size).toBe(file3Size);
				expect(info3.entries.length).toBe(4);
				for (const { length, data } of info3.entries) {
					expect(length).toBe(100);
					expect(data).toEqual(value);
				}
			}));

		it('should allow unlimited transaction log size', () =>
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 0 }] }, async ({ db, dbPath }) => {
				const log = db.useLog('foo');
				const value = Buffer.alloc(10000, 'a');

				for (let i = 0; i < 2000; i++) {
					await db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
					});
				}

				const totalSize = TRANSACTION_LOG_FILE_HEADER_SIZE
					+ (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10000) * 2000;
				const logStorePath = join(dbPath, 'transaction_logs', 'foo');
				const logFiles = await readdir(logStorePath);
				expect(logFiles).toEqual(['1.txnlog']);
				expect(statSync(join(dbPath, 'transaction_logs', 'foo', '1.txnlog')).size).toBe(totalSize);
			}));

		it('should not commit the log if the transaction is aborted', () =>
			dbRunner(async ({ db, dbPath }) => {
				const log = db.useLog('foo');
				const value = Buffer.alloc(100, 'a');

				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
					txn.abort();
				});

				const logPath = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				expect(existsSync(logPath)).toBe(false);
				const queryResults = Array.from(log.query({ start: 0 }));
				expect(queryResults.length).toBe(0);
			}));

		it('should add multiple entries from separate transactions', () =>
			dbRunner(async ({ db, dbPath }) => {
				const log = db.useLog('foo');
				const valueA = Buffer.alloc(10, 'a');
				const valueB = Buffer.alloc(10, 'b');

				await db.transaction(async (txn) => {
					log.addEntry(valueA, txn.id);
				});

				await db.transaction(async (txn) => {
					log.addEntry(valueB, txn.id);
				});

				const logPath = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				const info = parseTransactionLog(logPath);
				expect(info.size).toBe(
					TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10) * 2
				);
				expect(info.version).toBe(1);
				expect(info.entries.length).toBe(2);
				expect(info.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
				expect(info.entries[0].length).toBe(10);
				expect(info.entries[0].data).toEqual(valueA);
				expect(info.entries[1].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
				expect(info.entries[1].length).toBe(10);
				expect(info.entries[1].data).toEqual(valueB);

				const queryResults = Array.from(log.query({ start: 0 }));
				expect(queryResults.length).toBe(2);
			}));

		it('should rotate if not enough room for the next transaction header', () =>
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, async ({ db, dbPath }) => {
				const log = db.useLog('foo');

				for (let i = 0; i < 2; i++) {
					await db.transaction(async (txn) => {
						log.addEntry(Buffer.alloc(990, 'a'), txn.id);
					});
				}

				const logStorePath = join(dbPath, 'transaction_logs', 'foo');
				const logFiles = await readdir(logStorePath);
				expect(logFiles.sort()).toEqual(['1.txnlog', '2.txnlog']);

				const log1Path = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				const log2Path = join(dbPath, 'transaction_logs', 'foo', '2.txnlog');
				const info1 = parseTransactionLog(log1Path);
				const info2 = parseTransactionLog(log2Path);

				expect(info1.size).toBe(
					TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 990
				);
				expect(info1.entries.length).toBe(1);
				expect(info1.entries[0].length).toBe(990);
				expect(info1.entries[0].data).toEqual(Buffer.alloc(990, 'a'));

				expect(info2.size).toBe(
					TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 990
				);
				expect(info2.entries.length).toBe(1);
				expect(info2.entries[0].length).toBe(990);
				expect(info2.entries[0].data).toEqual(Buffer.alloc(990, 'a'));
			}));

		it('should rotate if room for the transaction header, but not the entry', () =>
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, async ({ db, dbPath }) => {
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
				expect(logFiles.sort()).toEqual(['1.txnlog', '2.txnlog']);

				const log1Path = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				const log2Path = join(dbPath, 'transaction_logs', 'foo', '2.txnlog');
				const info1 = parseTransactionLog(log1Path);
				const info2 = parseTransactionLog(log2Path);

				expect(info1.size).toBe(
					TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + targetSize
				);
				expect(info1.entries.length).toBe(1);
				expect(info1.entries[0].length).toBe(targetSize);
				expect(info1.entries[0].data).toEqual(targetData);

				expect(info2.size).toBe(
					TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 100
				);
				expect(info2.entries.length).toBe(1);
				expect(info2.entries[0].length).toBe(100);
				expect(info2.entries[0].data).toEqual(Buffer.alloc(100, 'a'));

				const queryResults = Array.from(log.query({ start: 0 }));
				expect(queryResults.length).toBe(2);
			}));

		it('should continue batch in next file', () =>
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, async ({ db, dbPath }) => {
				const log = db.useLog('foo');
				const value = Buffer.alloc(100, 'a');
				await db.transaction(async (txn) => {
					for (let i = 0; i < 15; i++) {
						log.addEntry(value, txn.id);
					}
				});

				const logStorePath = join(dbPath, 'transaction_logs', 'foo');
				const logFiles = await readdir(logStorePath);
				expect(logFiles.sort()).toEqual(['1.txnlog', '2.txnlog']);

				const log1Path = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				const log2Path = join(dbPath, 'transaction_logs', 'foo', '2.txnlog');
				const info1 = parseTransactionLog(log1Path);
				const info2 = parseTransactionLog(log2Path);

				expect(info1.size).toBe(
					TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 100) * 8
				);
				expect(info1.entries.length).toBe(8);
				expect(info1.entries[0].length).toBe(100);
				expect(info1.entries[0].data).toEqual(Buffer.alloc(100, 'a'));

				expect(info2.size).toBe(
					TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 100) * 7
				);
				expect(info2.entries.length).toBe(7);
				expect(info2.entries[0].length).toBe(100);
				expect(info2.entries[0].data).toEqual(Buffer.alloc(100, 'a'));

				const queryResults = Array.from(log.query({ start: 0 }));
				expect(queryResults.length).toBe(15);
				expect(queryResults[0].endTxn).toBe(false);
				expect(queryResults[14].endTxn).toBe(true);
			}));

		it('should be able to rotate with entries that span a transaction', () =>
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, async ({ db, dbPath }) => {
				let log = db.useLog('foo');
				const value = Buffer.alloc(100, 'a');
				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
				});
				for (let i = 0; i < 5; i++) {
					await db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
						log.addEntry(value, txn.id);
					});
				}

				const logStorePath = join(dbPath, 'transaction_logs', 'foo');
				const logFiles = await readdir(logStorePath);
				expect(logFiles.sort()).toEqual(['1.txnlog', '2.txnlog']);

				const log1Path = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				const log2Path = join(dbPath, 'transaction_logs', 'foo', '2.txnlog');
				const info1 = parseTransactionLog(log1Path);
				const info2 = parseTransactionLog(log2Path);

				expect(info1.size).toBe(
					TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 100) * 8
				);
				expect(info1.entries.length).toBe(8);
				expect(info1.entries[0].length).toBe(100);
				expect(info1.entries[0].data).toEqual(Buffer.alloc(100, 'a'));

				expect(info2.size).toBe(
					TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 100) * 3
				);
				expect(info2.entries.length).toBe(3);
				expect(info2.entries[0].length).toBe(100);
				expect(info2.entries[0].data).toEqual(Buffer.alloc(100, 'a'));

				const queryResults = Array.from(log.query({ start: 0 }));
				expect(queryResults.length).toBe(11);
			}));

		it('should write to same log from multiple workers', () =>
			dbRunner(async ({ db, dbPath }) => {
				const worker = new Worker(
					createWorkerBootstrapScript('./test/workers/transaction-log-worker.mts'),
					{ eval: true, workerData: { path: dbPath } }
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

		it('should rotate if file exceeds max age threshold', () =>
			dbRunner({
				dbOptions: [{ transactionLogRetention: 2000, transactionLogMaxAgeThreshold: 0.9 }],
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
				expect(logFiles.sort()).toEqual(['1.txnlog', '2.txnlog']);

				const log1Path = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				const log2Path = join(dbPath, 'transaction_logs', 'foo', '2.txnlog');
				const info1 = parseTransactionLog(log1Path);
				const info2 = parseTransactionLog(log2Path);

				expect(info1.size).toBe(
					TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10
				);
				expect(info1.version).toBe(1);
				expect(info1.entries.length).toBe(1);
				expect(info1.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 10000);
				expect(info1.entries[0].length).toBe(10);
				expect(info1.entries[0].data).toEqual(Buffer.alloc(10, 'a'));

				expect(info2.size).toBe(
					TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10
				);
				expect(info2.version).toBe(1);
				expect(info2.entries.length).toBe(1);
				expect(info2.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
				expect(info2.entries[0].length).toBe(10);
				expect(info2.entries[0].data).toEqual(Buffer.alloc(10, 'a'));
			}));

		it('should append to existing log file', () =>
			dbRunner(async ({ db, dbPath }) => {
				const log = db.useLog('foo');
				const valueA = Buffer.alloc(10, 'a');
				const valueB = Buffer.alloc(10, 'b');

				await db.transaction(async (txn) => {
					log.addEntry(valueA, txn.id);
				});

				const logPath = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				let info = parseTransactionLog(logPath);
				expect(info.size).toBe(
					TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10
				);
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
				expect(info.size).toBe(
					TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10) * 2
				);
				expect(info.version).toBe(1);
				expect(info.entries.length).toBe(2);
				expect(info.entries[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 10000);
				expect(info.entries[0].length).toBe(10);
				expect(info.entries[0].data).toEqual(valueA);
				expect(info.entries[1].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
				expect(info.entries[1].length).toBe(10);
				expect(info.entries[1].data).toEqual(valueB);

				const queryResults = Array.from(log.query({ start: 0 }));
				expect(queryResults.length).toBe(2);
			}));

		it('should write earliest timestamp in file headers', () =>
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, async ({ db, dbPath }) => {
				const log = db.useLog('foo');

				await Promise.all([
					db.transaction(async (txn) => {
						// this transaction started first, but will take longer
						await delay(100);
						log.addEntry(Buffer.alloc(990, 'a'), txn.id); // 2.txnlog
					}),
					db.transaction(async (txn) => {
						log.addEntry(Buffer.alloc(990, 'a'), txn.id); // 1.txnlog
					}),
				]);

				const log1Path = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				const log2Path = join(dbPath, 'transaction_logs', 'foo', '2.txnlog');
				const info1 = parseTransactionLog(log1Path);
				const info2 = parseTransactionLog(log2Path);
				expect(info1.timestamp).toBe(info2.timestamp);

				await db.transaction(async (txn) => {
					log.addEntry(Buffer.alloc(990, 'a'), txn.id); // 3.txnlog
				});
				const log3Path = join(dbPath, 'transaction_logs', 'foo', '3.txnlog');
				const info3 = parseTransactionLog(log3Path);
				expect(info3.timestamp).toBeGreaterThan(info2.timestamp);

				await Promise.all([
					db.transaction(async (txn) => {
						// this transaction started first, but will take longer
						await delay(200);
						log.addEntry(Buffer.alloc(990, 'a'), txn.id); // 6.txnlog
					}),
					db.transaction(async (txn) => {
						log.addEntry(Buffer.alloc(990, 'a'), txn.id); // 4.txnlog
					}),
					db.transaction(async (txn) => {
						await delay(100);
						log.addEntry(Buffer.alloc(990, 'a'), txn.id); // 5.txnlog
					}),
				]);

				const log4Path = join(dbPath, 'transaction_logs', 'foo', '4.txnlog');
				const log5Path = join(dbPath, 'transaction_logs', 'foo', '5.txnlog');
				const log6Path = join(dbPath, 'transaction_logs', 'foo', '6.txnlog');
				const info4 = parseTransactionLog(log4Path);
				const info5 = parseTransactionLog(log5Path);
				const info6 = parseTransactionLog(log6Path);
				expect(info4.timestamp).toBeGreaterThan(info3.timestamp);
				expect(info5.timestamp).toBeGreaterThan(info4.timestamp);
				expect(info6.timestamp).toBe(info5.timestamp);
			}));

		it('should error if the log name is invalid', () =>
			dbRunner(async ({ db }) => {
				expect(() => db.useLog(undefined as any)).toThrowError(
					new TypeError('Log name must be a string or number')
				);
				expect(() => db.useLog([] as any)).toThrowError(
					new TypeError('Log name must be a string or number')
				);
				await expect(db.transaction(txn => txn.useLog(undefined as any))).rejects.toThrowError(
					new TypeError('Log name must be a string or number')
				);
			}));

		it('should error if entry data is invalid', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('foo');
				await db.transaction(async (txn) => {
					expect(() => log.addEntry(undefined as any, txn.id)).toThrowError(
						new TypeError('Invalid log entry, expected a Buffer or ArrayBuffer')
					);
					expect(() => log.addEntry([] as any, txn.id)).toThrowError(
						new TypeError('Invalid log entry, expected a Buffer or ArrayBuffer')
					);
				});
			}));

		it('should error if transaction id is invalid', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('foo');
				await db.transaction(async (_txn) => {
					expect(() => log.addEntry(Buffer.from('hello'), undefined as any)).toThrowError(
						new TypeError('Missing argument, transaction id is required')
					);
					expect(() => log.addEntry(Buffer.from('hello'), [] as any)).toThrowError(
						new TypeError('Invalid argument, transaction id must be a non-negative integer')
					);
				});
			}));
	});

	describe('purgeLogs', () => {
		it('should purge all transaction log files', () =>
			dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
				const logDirectory = join(dbPath, 'transaction_logs', 'foo');
				const logFile = join(logDirectory, '1.txnlog');
				await mkdir(logDirectory, { recursive: true });
				await writeFile(logFile, '');
				expect(existsSync(logFile)).toBe(true);

				db.open();
				expect(db.listLogs()).toEqual(['foo']);
				expect(existsSync(logFile)).toBe(true);
				expect(db.purgeLogs({ destroy: true })).toEqual([logFile]);
				expect(existsSync(logDirectory)).toBe(false);
			}));

		it('should destroy a specific transaction log file', () =>
			dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
				const fooLogDirectory = join(dbPath, 'transaction_logs', 'foo');
				const fooLogFile = join(fooLogDirectory, '1.txnlog');
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

		it('should purge old log file on load', () =>
			dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
				const logDirectory = join(dbPath, 'transaction_logs', 'foo');
				const logFile = join(logDirectory, '1.txnlog');
				await mkdir(logDirectory, { recursive: true });
				await writeFile(logFile, '');

				const oneWeekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
				await utimes(logFile, oneWeekAgo, oneWeekAgo);

				db.open();
				expect(db.listLogs()).toEqual(['foo']);
				expect(existsSync(logFile)).toBe(false);
			}));
	});

	describe('flushSync()', () => {
		it('should increase the latest flushed position after flushSync calls', () =>
			dbRunner(
				{ dbOptions: [{ name: 'data1' }, { name: 'data2' }] },
				async ({ db, dbPath }, { db: db2 }) => {
					const log = db.useLog('foo');
					const value = Buffer.alloc(10, 'a');

					await db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
						db.putSync('foo', value, { transaction: txn });
						db2.putSync('foo', value, { transaction: txn });
					});

					let queryResults = Array.from(log.query({ startFromLastFlushed: true }));
					expect(queryResults.length).toBe(1);
					expect(queryResults[0].data).toEqual(value);
					expect(queryResults[0].endTxn).toBe(true);

					db.flushSync();
					const contents = readFileSync(join(dbPath, 'transaction_logs', 'foo', 'txn.state'));
					const u32s = new Uint32Array(contents.buffer, contents.byteOffset);
					expect(u32s[1]).toBe(1);
					expect(u32s[0]).toBeGreaterThan(1);

					queryResults = Array.from(log.query({ startFromLastFlushed: true }));
					expect(queryResults.length).toBe(0);
					await db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
						db.putSync('foo', value, { transaction: txn });
					});
					queryResults = Array.from(log.query({ startFromLastFlushed: true }));
					expect(queryResults.length).toBe(1);
				}
			));
	});

	describe('flush()', () => {
		it('should increase the latest flushed position after flush calls', () =>
			dbRunner(async ({ db, dbPath }) => {
				const log = db.useLog('foo');
				const value = Buffer.alloc(10, 'a');

				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
					db.putSync('foo', value, { transaction: txn });
				});

				let queryResults = Array.from(log.query({ startFromLastFlushed: true }));
				expect(queryResults.length).toBe(1);
				expect(queryResults[0].data).toEqual(value);
				expect(queryResults[0].endTxn).toBe(true);

				await db.flush();
				let contents = readFileSync(join(dbPath, 'transaction_logs', 'foo', 'txn.state'));
				let u32s = new Uint32Array(contents.buffer, contents.byteOffset);
				expect(u32s[1]).toBe(1);
				expect(u32s[0]).toBeGreaterThan(1);

				queryResults = Array.from(log.query({ startFromLastFlushed: true }));
				expect(queryResults.length).toBe(0);
				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
					db.putSync('foo', value, { transaction: txn });
				});
				queryResults = Array.from(log.query({ startFromLastFlushed: true }));
				expect(queryResults.length).toBe(1);
				let lastFlush: Promise<void> | undefined;
				for (let i = 0; i < 10; i++) {
					if (i % 3 === 1) {
						await lastFlush;
					}
					await db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
						db.putSync('foo' + Math.random(), Math.random(), { transaction: txn });
					});
					// make some of this concurrent
					lastFlush = db.flush();
				}
				await lastFlush;
				// do one last commit and flush
				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
					db.putSync('foo' + Math.random(), Math.random(), { transaction: txn });
				});
				// make some of this concurrent
				await db.flush();

				queryResults = Array.from(log.query({ startFromLastFlushed: true }));
				expect(queryResults.length).toBe(0);
				contents = readFileSync(join(dbPath, 'transaction_logs', 'foo', 'txn.state'));
				u32s = new Uint32Array(contents.buffer, contents.byteOffset);
				expect(u32s[1]).toBe(1);
				expect(u32s[0]).toBeGreaterThan(200);
			}));
	});
});
