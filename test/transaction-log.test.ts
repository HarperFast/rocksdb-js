import { RocksDatabase, Transaction } from '../src/index.js';
import { constants, coolTransactionLogs, type TransactionLog } from '../src/load-binding.js';
import { parseTransactionLog } from '../src/parse-transaction-log.js';
import { withResolvers } from '../src/util.js';
import {
	createWorkerBootstrapScript,
	dbRunner,
	generateDBPath,
	terminateWorker,
} from './lib/util.js';
import assert from 'node:assert';
import { existsSync, readFileSync, statSync } from 'node:fs';
import { mkdir, readdir, stat, utimes, writeFile } from 'node:fs/promises';
import { release } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';
import { describe, expect, it } from 'vitest';

const {
	TRANSACTION_LOG_FILE_HEADER_SIZE,
	TRANSACTION_LOG_ENTRY_HEADER_SIZE,
	TRANSACTION_LOG_TOKEN,
} = constants;

describe('Transaction Log', () => {
	describe('useLog()', () => {
		it('should detect existing transaction logs', () =>
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

		it('should emit "new-transaction-log" event when a new log is created', () =>
			dbRunner(async ({ db }) => {
				const events: { name: string }[] = [];
				let eventResolver: (() => void) | undefined;
				let eventPromise = new Promise<void>((resolve) => {
					eventResolver = resolve;
				});

				const listener = (name: string) => {
					events.push({ name });
					eventResolver?.();
				};
				db.addListener('new-transaction-log', listener);

				// First call should create the log and emit the event with log name
				const log1 = db.useLog('foo');
				expect(log1).toBeDefined();
				await eventPromise;
				expect(events.length).toBe(1);
				expect(events[0].name).toBe('foo');

				// Second call should reuse existing log and not emit
				const log2 = db.useLog('foo');
				expect(log2).toBe(log1);
				await delay(10); // Give time for any potential event
				expect(events.length).toBe(1);

				// Creating a different log should emit again with the new log name
				eventPromise = new Promise<void>((resolve) => {
					eventResolver = resolve;
				});
				const log3 = db.useLog('bar');
				expect(log3).toBeDefined();
				expect(log3).not.toBe(log1);
				await eventPromise;
				expect(events.length).toBe(2);
				expect(events[1].name).toBe('bar');

				db.removeListener('new-transaction-log', listener);
			}));

		it.skipIf(!globalThis.gc)('should cleanup transaction log instance on GC', () =>
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

		it('should error if transaction is already bound to an unbounded log', () =>
			dbRunner(async ({ db }) => {
				const log1 = db.useLog('log1');
				const log2 = db.useLog('log2');

				await db.transaction(async (txn) => {
					log1.addEntry(Buffer.from('hello'), txn.id);
					log1.addEntry(Buffer.from('world'), txn.id);
					expect(() => log2.addEntry(Buffer.from('nope'), txn.id)).toThrow(
						new Error(`Transaction ${txn.id} is already bound to the log store "log1"`)
					);
				});
			}));

		it('should error if transaction is already bound to a different bounded log', () =>
			dbRunner(async ({ db }) => {
				await db.transaction(async (txn) => {
					txn.useLog('log3');
					txn.useLog('log3'); // do it twice
					expect(() => txn.useLog('log4')).toThrow(
						new Error(`Transaction ${txn.id} is already bound to the log store "log3"`)
					);
				});
			}));

		it('should error if transaction is already bound to a bounded log', () =>
			dbRunner(async ({ db }) => {
				const log1 = db.useLog('log1');

				await db.transaction(async (txn) => {
					txn.useLog('log4');
					expect(() => log1.addEntry(Buffer.from('world'), txn.id)).toThrow(
						new Error(`Transaction ${txn.id} is already bound to the log store "log4"`)
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
				expect(
					transactionResults.filter((result) => result.status === 'rejected').length
				).toBeGreaterThanOrEqual(1); // at least one should fail
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
				expect(
					Array.from(log.query({ start: allTimestamps[1], exclusiveStart: true })).length
				).toBe(3);
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
				expect(
					Array.from(log.query({ start: start - 1, exactStart: true, end: start - 2 })).length
				).toBe(3);
			}));

		it('keeps every committed entry findable by an exactStart point read as the log grows (#1148)', () =>
			dbRunner(async ({ db }) => {
				// Regression guard for HarperFast/harper#1148: findPositionByTimestamp must not corrupt the
				// append-owned file size while indexing (which would freeze the index and intermittently
				// return empty for a committed entry). Append in batches and, after each batch, point-read
				// every previously committed timestamp via an exactStart query — all must still be found,
				// including entries committed before the most recent appends. Includes out-of-order
				// timestamps, which exactStart is specifically designed to resolve.
				const log = db.useLog('foo');
				const value = Buffer.alloc(10, 'a');
				const base = Date.now();
				const committed: number[] = [];
				for (let i = 0; i < 40; i++) {
					// alternate forward and backward timestamps so the log is not monotonic
					const ts = base + (i % 2 === 0 ? i : -i);
					await db.transaction(async (txn) => {
						txn.setTimestamp(ts);
						log.addEntry(value, txn.id);
					});
					committed.push(ts);
					for (const t of committed) {
						const found = Array.from(log.query({ start: t, exactStart: true })).some(
							(entry) => entry.timestamp === t
						);
						expect(found, `committed entry at ${t} must be found after ${i + 1} appends`).toBe(
							true
						);
					}
				}
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

		it('should not throw when the memory map for the latest log cannot be acquired', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('foo');
				const value = Buffer.alloc(10, 'a');
				for (let i = 0; i < 3; i++) {
					await db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
					});
				}
				// prime the query path so _logBuffers and _lastCommittedPosition are initialized
				expect(Array.from(log.query({ start: 0 })).length).toBe(3);

				// simulate the inconsistent state where _lastCommittedPosition reports a
				// non-zero size for a logId whose memory map cannot be acquired (transient
				// state during rotation, 0-byte file at mmap time, FS race, etc.)
				log._logBuffers.clear();
				(log as { _currentLogBuffer?: unknown })._currentLogBuffer = undefined;
				const realGetMmapDescriptor = Object.getOwnPropertyDescriptor(
					Object.getPrototypeOf(log),
					'_getMemoryMapOfFile'
				)!;
				Object.defineProperty(log, '_getMemoryMapOfFile', {
					value: () => undefined,
					configurable: true,
					writable: true,
				});
				try {
					// should terminate iteration cleanly rather than throw RangeError
					const results = Array.from(log.query({ start: 0 }));
					expect(results.length).toBe(0);
				} finally {
					Object.defineProperty(log, '_getMemoryMapOfFile', realGetMmapDescriptor);
				}
				// once the mmap works again, the data must still be readable
				expect(Array.from(log.query({ start: 0 })).length).toBe(3);
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
				const log = db.useLog('foo');
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

				const file1Size =
					TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 100) * 8;
				const file2Size =
					TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 100) * 8;
				const file3Size =
					TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 100) * 4;

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

				const totalSize =
					TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10000) * 2000;
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
					expect(txn.abort()).toBeUndefined();
					// second call should detect already aborted and return `undefined`
					expect(txn.abort()).toBeUndefined();
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

		it(
			'should write to same log from multiple workers',
			() =>
				dbRunner(async ({ db, dbPath }) => {
					const worker = new Worker(
						createWorkerBootstrapScript('./test/workers/transaction-log-worker.mts'),
						{ eval: true, workerData: { path: dbPath } }
					);

					let resolver = withResolvers<void>();

					await new Promise<void>((resolve, reject) => {
						worker.on('error', reject);
						worker.on('message', (event) => {
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

					worker.postMessage({ addManyEntries: true, count: 500 });

					for (let i = 0; i < 500; i++) {
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
					await terminateWorker(worker);
					await resolver.promise;
				}),
			60000
		);

		it('should rotate if file exceeds max age threshold', () =>
			dbRunner(
				{
					dbOptions: [{ transactionLogRetention: 2000, transactionLogMaxAgeThreshold: 0.9 }],
				},
				async ({ db, dbPath }) => {
					// we need to add the entry within 3 seconds
					const log = db.useLog('foo');
					await db.transaction(async (txn) => {
						log.addEntry(Buffer.alloc(10, 'a'), txn.id);
					});

					await delay(250);

					// File should now be 250ms old, exceeding the rotation threshold of
					// 200ms (2000ms retention × (1 - 0.9 threshold) = 200ms)
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
				}
			));

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
				expect(() => db.useLog(undefined as any)).toThrow(
					new TypeError('Log name must be a string or number')
				);
				expect(() => db.useLog([] as any)).toThrow(
					new TypeError('Log name must be a string or number')
				);
				await expect(db.transaction((txn) => txn.useLog(undefined as any))).rejects.toThrow(
					new TypeError('Log name must be a string or number')
				);
			}));

		it('should error if entry data is invalid', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('foo');
				await db.transaction(async (txn) => {
					expect(() => log.addEntry(undefined as any, txn.id)).toThrow(
						new TypeError('Invalid log entry, expected a Buffer or ArrayBuffer')
					);
					expect(() => log.addEntry([] as any, txn.id)).toThrow(
						new TypeError('Invalid log entry, expected a Buffer or ArrayBuffer')
					);
				});
			}));

		it('should error if transaction id is invalid', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('foo');
				await db.transaction(async (_txn) => {
					expect(() => log.addEntry(Buffer.from('hello'), undefined as any)).toThrow(
						new TypeError('Missing argument, transaction id is required')
					);
					expect(() => log.addEntry(Buffer.from('hello'), [] as any)).toThrow(
						new TypeError('Invalid argument, transaction id must be a non-negative integer')
					);
				});
			}));

		it('should error if async transaction is abandoned after a failed commit', () =>
			dbRunner(async ({ db, dbPath }) => {
				const log = db.useLog('foo');

				const firstTxn = db.transaction(
					async (txn, attempt) => {
						log.addEntry(Buffer.from('hello'), txn.id);
						await txn.put('foo', Buffer.from('hello'));
						if (attempt === 1) {
							await delay(50);
							// the second transaction will be complete by now, IsBusy will happen
						}
					},
					{ retryOnBusy: false }
				);

				await db.transaction(async (txn) => {
					log.addEntry(Buffer.from('hello2'), txn.id);
					// this will be committed before the first transaction causing
					// the first one to experience a IsBusy error
					await txn.put('foo', Buffer.from('hello2'));
				});

				await expect(firstTxn).rejects.toThrow(
					'Transaction was abandoned after writing to the transaction log'
				);

				const logPath = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				const info = parseTransactionLog(logPath);
				expect(info.entries.length).toBe(2);
			}));

		it('should error if sync transaction is abandoned after a failed commit', () =>
			dbRunner(async ({ db, dbPath }) => {
				const log = db.useLog('foo');

				const firstTxn = db.transactionSync(
					async (txn, attempt) => {
						log.addEntry(Buffer.from('hello'), txn.id);
						await txn.put('foo', Buffer.from('hello'));
						if (attempt === 1) {
							await delay(50);
							// the second transaction will be complete by now, IsBusy will happen
						}
					},
					{ retryOnBusy: false }
				);

				db.transactionSync((txn) => {
					log.addEntry(Buffer.from('hello2'), txn.id);
					// this will be committed before the first transaction causing
					// the first one to experience a IsBusy error
					txn.putSync('foo', Buffer.from('hello2'));
				});

				await expect(firstTxn).rejects.toThrow(
					'Transaction was abandoned after writing to the transaction log'
				);

				const logPath = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				const info = parseTransactionLog(logPath);
				expect(info.entries.length).toBe(2);
			}));

		it('should recover from a failed async commit', () =>
			dbRunner(async ({ db, dbPath }) => {
				const log = db.useLog('foo');

				const firstTxn = db.transaction(async (txn, attempt) => {
					if (attempt === 1) {
						log.addEntry(Buffer.from('hello'), txn.id);
					}
					await txn.put('foo', Buffer.from('hello'));
					if (attempt === 1) {
						await delay(50);
						// the second transaction will be complete by now, IsBusy will happen
					}
				});

				await db.transaction(async (txn) => {
					log.addEntry(Buffer.from('hello2'), txn.id);
					// this will be committed before the first transaction causing
					// the first one to experience a IsBusy error
					await txn.put('foo', Buffer.from('hello2'));
				});

				await firstTxn;
				expect(await db.get('foo')).toEqual(Buffer.from('hello'));

				const logPath = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				const info = parseTransactionLog(logPath);
				expect(info.entries.length).toBe(2);
			}));

		it('should recover from a failed sync commit', () =>
			dbRunner(async ({ db, dbPath }) => {
				const log = db.useLog('foo');

				const firstTxn = db.transactionSync(async (txn, attempt) => {
					if (attempt === 1) {
						log.addEntry(Buffer.from('hello'), txn.id);
					}
					await txn.put('foo', Buffer.from('hello'));
					if (attempt === 1) {
						await delay(50);
						// the second transaction will be complete by now, IsBusy will happen
					}
				});

				db.transactionSync((txn) => {
					log.addEntry(Buffer.from('hello2'), txn.id);
					// this will be committed before the first transaction causing
					// the first one to experience a IsBusy error
					txn.putSync('foo', Buffer.from('hello2'));
				});

				await firstTxn;
				expect(await db.get('foo')).toEqual(Buffer.from('hello'));

				const logPath = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				const info = parseTransactionLog(logPath);
				expect(info.entries.length).toBe(2);
			}));

		it('should bind a transaction log to a transaction', () =>
			dbRunner(async ({ db, dbPath }) => {
				const value = Buffer.alloc(10, 'a');

				await db.transaction(async (txn) => {
					const log = txn.useLog('foo');
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

				const log = db.useLog('foo');
				const queryResults = Array.from(log.query({ start: 0 }));
				expect(queryResults.length).toBe(1);
				expect(queryResults[0].data).toEqual(value);
				expect(queryResults[0].timestamp).toBeGreaterThanOrEqual(Date.now() - 1000);
				expect(queryResults[0].endTxn).toBe(true);
			}));
	});

	describe('writev framing integrity', () => {
		// Regression for HarperFast/rocksdb-js#572. The POSIX writeBatchToFile
		// previously advanced through iovecs by count regardless of actual bytes
		// written, so any short writev silently dropped a tail and produced
		// framing corruption. These tests exercise the high-iovcnt batching path
		// end-to-end and assert byte-exact round-trip integrity for every entry.

		it('should round-trip many entries with varied sizes across a single batch', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('foo');
				const startTime = Date.now() - 1000;
				const numEntries = 200;
				const expected: Buffer[] = [];

				await db.transaction(async (txn) => {
					for (let i = 0; i < numEntries; i++) {
						// vary sizes (1..256 bytes) so iovecs land on non-aligned boundaries
						const size = (i % 256) + 1;
						const value = Buffer.alloc(size);
						for (let j = 0; j < size; j++) {
							value[j] = (i * 37 + j) & 0xff;
						}
						expected.push(value);
						log.addEntry(value, txn.id);
					}
				});

				const results = Array.from(log.query({ start: startTime, end: Date.now() + 1000 }));
				expect(results.length).toBe(numEntries);
				for (let i = 0; i < numEntries; i++) {
					expect(Buffer.from(results[i].data)).toEqual(expected[i]);
				}
			}));

		it('should round-trip > MAX_IOVS (1024) entries in a single batch', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('foo');
				const startTime = Date.now() - 1000;
				// > 1024 entries forces writeBatchToFile to chunk through the
				// MAX_IOVS-bounded inner loop. Pre-fix code mis-tracked progress
				// here when any partial writev occurred.
				const numEntries = 1100;
				const expected: Buffer[] = [];

				await db.transaction(async (txn) => {
					for (let i = 0; i < numEntries; i++) {
						const value = Buffer.alloc(16);
						value.writeUInt32BE(i >>> 0, 0);
						value.writeUInt32BE((i ^ 0xdeadbeef) >>> 0, 4);
						value.writeUInt32BE((i + 1) >>> 0, 8);
						value.writeUInt32BE((i * 7) >>> 0, 12);
						expected.push(value);
						log.addEntry(value, txn.id);
					}
				});

				const results = Array.from(log.query({ start: startTime, end: Date.now() + 1000 }));
				expect(results.length).toBe(numEntries);
				for (let i = 0; i < numEntries; i++) {
					expect(Buffer.from(results[i].data)).toEqual(expected[i]);
				}
			}));
	});

	describe('corruption handling', () => {
		// A torn/corrupt entry can declare a length far larger than the bytes
		// actually present (e.g. a partial write that left a header pointing past
		// its data). The readers must fail with a bounded, diagnosable error
		// rather than driving an unbounded allocUnsafe (OOM), building a multi-GB
		// hex string, or dereferencing an undefined buffer. Regression for the
		// race_alerts crash-loop investigation.

		function buildLogBuffer(
			entries: { timestamp: number; length: number; flags: number; data: Buffer }[]
		): Buffer {
			const header = Buffer.alloc(TRANSACTION_LOG_FILE_HEADER_SIZE);
			header.writeUInt32BE(TRANSACTION_LOG_TOKEN >>> 0, 0);
			header.writeUInt8(1, 4); // version
			header.writeDoubleBE(Date.now(), 5);
			const parts: Buffer[] = [header];
			for (const entry of entries) {
				const entryHeader = Buffer.alloc(TRANSACTION_LOG_ENTRY_HEADER_SIZE);
				entryHeader.writeDoubleBE(entry.timestamp, 0);
				entryHeader.writeUInt32BE(entry.length >>> 0, 8);
				entryHeader.writeUInt8(entry.flags, 12);
				parts.push(entryHeader, entry.data);
			}
			return Buffer.concat(parts);
		}

		it('parseTransactionLog() throws a bounded error when an entry length overruns the file', async () => {
			const path = `${generateDBPath()}.txnlog`;
			// one valid entry, then a header claiming ~2GB with no data behind it
			const buffer = buildLogBuffer([
				{ timestamp: Date.now(), length: 4, flags: 1, data: Buffer.from([1, 2, 3, 4]) },
				{ timestamp: Date.now(), length: 0x7fffffff, flags: 1, data: Buffer.alloc(0) },
			]);
			await writeFile(path, buffer);

			let error: Error | undefined;
			try {
				parseTransactionLog(path);
			} catch (caught) {
				error = caught as Error;
			}
			expect(error).toBeInstanceOf(Error);
			expect(error!.message).toMatch(/Corrupt entry at offset/);
			// must not regress into the unbounded-allocation / oversized-string symptom
			expect(error!.message).not.toMatch(/Cannot create a string longer than/);
			expect(error!.message.length).toBeLessThan(1000);
		});

		it('query() throws a bounded RangeError when an entry length overruns the log buffer', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('foo');
				const value = Buffer.alloc(10, 'a');
				for (let i = 0; i < 3; i++) {
					await db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
					});
				}
				// prime the query path so _lastCommittedPosition / _logBuffers init
				expect(Array.from(log.query({ start: 0 })).length).toBe(3);

				// Build a standalone-ArrayBuffer copy of the real memory map and corrupt
				// the length field of the 2nd entry so it overruns the buffer. Returning
				// the copy from _getMemoryMapOfFile lets us exercise the reader's framing
				// path without a read-only mmap we can't mutate in place.
				const real = log._getMemoryMapOfFile(1)!;
				const copyBuffer = Buffer.from(new ArrayBuffer(real.length));
				real.copy(copyBuffer);
				const secondEntryLengthOffset =
					TRANSACTION_LOG_FILE_HEADER_SIZE + (TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10) + 8;
				copyBuffer.writeUInt32BE(0x7fffffff, secondEntryLengthOffset);

				log._logBuffers.clear();
				(log as { _currentLogBuffer?: unknown })._currentLogBuffer = undefined;
				const realDescriptor = Object.getOwnPropertyDescriptor(
					Object.getPrototypeOf(log),
					'_getMemoryMapOfFile'
				)!;
				Object.defineProperty(log, '_getMemoryMapOfFile', {
					value: () => copyBuffer,
					configurable: true,
					writable: true,
				});
				try {
					expect(() => Array.from(log.query({ start: 0 }))).toThrow(
						/Corrupt transaction log entry/
					);
				} finally {
					Object.defineProperty(log, '_getMemoryMapOfFile', realDescriptor);
				}
				// once the real mmap is restored, the valid data must still be readable
				log._logBuffers.clear();
				(log as { _currentLogBuffer?: unknown })._currentLogBuffer = undefined;
				expect(Array.from(log.query({ start: 0 })).length).toBe(3);
			}));
	});

	describe('crash recovery (truncate-on-open)', () => {
		// Simulates the POSIX O_APPEND torn-tail scenario that the writev fix
		// (#573) left out of scope: a crash mid-append leaves a partial entry
		// whose header claims more data than was written. On reopen the store
		// must drop the torn tail back to the last valid entry while preserving
		// every committed entry.

		const logPathFor = (dbPath: string, name: string, seq = 1) =>
			join(dbPath, 'transaction_logs', name, `${seq}.txnlog`);

		// A torn partial entry: a full 13-byte header claiming `declaredLength`
		// data bytes, followed by only `actualData` bytes (the rest "lost" to a
		// crash mid-write).
		function tornEntry(declaredLength: number, actualData: number): Buffer {
			const buf = Buffer.alloc(TRANSACTION_LOG_ENTRY_HEADER_SIZE + actualData);
			buf.writeDoubleBE(Date.now(), 0);
			buf.writeUInt32BE(declaredLength, 8);
			buf.writeUInt8(1, 12);
			return buf;
		}

		// POSIX-only: the torn-tail scenario is the O_APPEND short-write case, and
		// truncateFile() is a deliberate no-op on Windows (which pre-extends and
		// zero-pads its logs), so there is nothing to truncate to assert there.
		it.skipIf(process.platform === 'win32')(
			'truncates a torn tail on reopen and preserves valid entries',
			() =>
				dbRunner(async ({ db, dbPath }) => {
					let database = db;
					try {
						const log = database.useLog('foo');
						const value = Buffer.alloc(24, 'x');
						for (let i = 0; i < 3; i++) {
							await database.transaction(async (txn) => {
								log.addEntry(value, txn.id);
							});
						}
						expect(Array.from(log.query({ start: 0 })).length).toBe(3);
						database.close();

						const logPath = logPathFor(dbPath, 'foo');
						const validSize = statSync(logPath).size;
						// append a torn partial entry: declares 5000 bytes, writes 16
						await writeFile(logPath, Buffer.concat([readFileSync(logPath), tornEntry(5000, 16)]));
						expect(statSync(logPath).size).toBeGreaterThan(validSize);

						// reopen: opening the log triggers store load + tail recovery,
						// which should truncate the torn tail back to validSize
						database = RocksDatabase.open(dbPath);
						const reopened = database.useLog('foo');
						expect(statSync(logPath).size).toBe(validSize);
						// the committed position isn't persisted without a RocksDB flush,
						// so read uncommitted to verify the entries survived on disk
						expect(Array.from(reopened.query({ start: 0, readUncommitted: true })).length).toBe(3);

						// the log must remain writable and consistent after recovery
						await database.transaction(async (txn) => {
							reopened.addEntry(Buffer.alloc(24, 'y'), txn.id);
						});
						expect(Array.from(reopened.query({ start: 0, readUncommitted: true })).length).toBe(4);
					} finally {
						database.close();
					}
				})
		);

		it('leaves a clean log file untouched on reopen', () =>
			dbRunner(async ({ db, dbPath }) => {
				let database = db;
				let validSize = 0;
				try {
					const log = database.useLog('foo');
					const value = Buffer.alloc(24, 'x');
					for (let i = 0; i < 3; i++) {
						await database.transaction(async (txn) => {
							log.addEntry(value, txn.id);
						});
					}
					validSize = statSync(logPathFor(dbPath, 'foo')).size;
					database.close();

					database = RocksDatabase.open(dbPath);
					const reopened = database.useLog('foo');
					expect(statSync(logPathFor(dbPath, 'foo')).size).toBe(validSize);
					expect(Array.from(reopened.query({ start: 0, readUncommitted: true })).length).toBe(3);
				} finally {
					database.close();
				}
			}));
	});

	describe('purgeLogs', () => {
		it('should purge all transaction log files', () =>
			dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
				const logDirectory = join(dbPath, 'transaction_logs', 'foo');
				const logFile = join(logDirectory, '1.txnlog');
				await mkdir(logDirectory, { recursive: true });

				const header = Buffer.alloc(TRANSACTION_LOG_FILE_HEADER_SIZE);
				header.writeUInt32BE(TRANSACTION_LOG_TOKEN, 0);
				header.writeUInt8(1, 4);
				header.writeDoubleBE(0, 5);
				await writeFile(logFile, header);

				await writeFile(join(logDirectory, 'txn.state'), Buffer.from([0, 0, 0, 0, 2, 0, 0, 0]));

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

				const header = Buffer.alloc(TRANSACTION_LOG_FILE_HEADER_SIZE);
				header.writeUInt32BE(TRANSACTION_LOG_TOKEN, 0);
				header.writeUInt8(1, 4);
				header.writeDoubleBE(0, 5);
				await writeFile(fooLogFile, header);

				await writeFile(join(fooLogDirectory, 'txn.state'), Buffer.from([0, 0, 0, 0, 2, 0, 0, 0]));

				const barLogDirectory = join(dbPath, 'transaction_logs', 'bar');
				const barLogFile = join(barLogDirectory, 'bar.1.txnlog');
				await mkdir(barLogDirectory, { recursive: true });

				header.writeUInt32BE(TRANSACTION_LOG_TOKEN, 0);
				header.writeUInt8(1, 4);
				header.writeDoubleBE(0, 5);
				await writeFile(barLogFile, header);

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

				const header = Buffer.alloc(TRANSACTION_LOG_FILE_HEADER_SIZE);
				header.writeUInt32BE(TRANSACTION_LOG_TOKEN, 0);
				header.writeUInt8(1, 4);
				header.writeDoubleBE(0, 5);
				await writeFile(logFile, header);

				const oneWeekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
				await utimes(logFile, oneWeekAgo, oneWeekAgo);

				db.open();
				expect(db.listLogs()).toEqual(['foo']);
				expect(existsSync(logFile)).toBe(false);
			}));

		it('should purge log files before a specific timestamp', () =>
			dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
				const logDirectory = join(dbPath, 'transaction_logs', 'foo');
				const logFile = join(logDirectory, '1.txnlog');
				await mkdir(logDirectory, { recursive: true });

				const header = Buffer.alloc(TRANSACTION_LOG_FILE_HEADER_SIZE);
				header.writeUInt32BE(TRANSACTION_LOG_TOKEN, 0);
				header.writeUInt8(1, 4);
				header.writeDoubleBE(0, 5);
				await writeFile(logFile, header);

				const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
				const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000);
				const threeHoursAgo = new Date(Date.now() - 3 * 60 * 60 * 1000);
				await utimes(logFile, twoHoursAgo, twoHoursAgo);

				await writeFile(join(logDirectory, 'txn.state'), Buffer.from([0, 0, 0, 0, 2, 0, 0, 0]));

				db.open();
				expect(db.listLogs()).toEqual(['foo']);
				expect(existsSync(logFile)).toBe(true);
				expect(db.purgeLogs({ before: threeHoursAgo.getTime() })).toEqual([]);
				expect(existsSync(logFile)).toBe(true);
				expect(db.purgeLogs({ before: oneHourAgo.getTime() })).toEqual([logFile]);
				expect(existsSync(logFile)).toBe(false);
			}));

		it('should return valid lastCommittedPosition after purging earlier log files and reopening', () =>
			dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
				const logDirectory = join(dbPath, 'transaction_logs', 'foo');
				await mkdir(logDirectory, { recursive: true });

				// Create log file 1 with header
				const logFile1 = join(logDirectory, '1.txnlog');
				const header1 = Buffer.alloc(TRANSACTION_LOG_FILE_HEADER_SIZE);
				header1.writeUInt32BE(TRANSACTION_LOG_TOKEN, 0);
				header1.writeUInt8(1, 4);
				header1.writeDoubleBE(0, 5);
				await writeFile(logFile1, header1);

				db.open();
				const log = db.useLog('foo');

				// Get initial lastCommittedPosition - should be valid even though no commits yet
				let lastCommittedBuffer = log._getLastCommittedPosition();
				let lastCommittedPosUint32 = new Uint32Array(lastCommittedBuffer.buffer, 0, 2);
				expect(lastCommittedPosUint32[1]).toBeGreaterThanOrEqual(1);
				expect(lastCommittedPosUint32[0]).toBeGreaterThanOrEqual(10);

				// Purge the first log file
				expect(existsSync(logFile1)).toBe(true);
				db.purgeLogs({ destroy: true, name: 'foo', before: Date.now() - 1000 });
				expect(existsSync(logFile1)).toBe(false);

				// Create log file 2 with header
				await mkdir(logDirectory, { recursive: true });
				const logFile2 = join(logDirectory, '2.txnlog');
				const header2 = Buffer.alloc(TRANSACTION_LOG_FILE_HEADER_SIZE);
				header2.writeUInt32BE(TRANSACTION_LOG_TOKEN, 0);
				header2.writeUInt8(1, 4);
				header2.writeDoubleBE(0, 5);
				await writeFile(logFile2, header2);

				// Write some data to log file 2 to make it have content beyond header
				const entry = Buffer.alloc(TRANSACTION_LOG_ENTRY_HEADER_SIZE + 10);
				entry.writeUInt32BE(10, 0); // data length
				entry.writeDoubleBE(Date.now(), 4); // timestamp
				entry.writeBigUInt64BE(1n, 12); // transaction id
				await writeFile(logFile2, Buffer.concat([header2, entry]));

				// Close and reopen database
				db.close();
				db.open();
				const log2 = db.useLog('foo');

				// After reopening, lastCommittedPosition should be valid and point to log file 2
				lastCommittedBuffer = log2._getLastCommittedPosition();
				lastCommittedPosUint32 = new Uint32Array(lastCommittedBuffer.buffer, 0, 2);
				expect(lastCommittedPosUint32[1]).toBeGreaterThanOrEqual(2);
				expect(lastCommittedPosUint32[0]).toBeGreaterThanOrEqual(10);
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

	describe('coolTransactionLogs()', () => {
		// MADV_COLD is Linux 5.4+; on macOS/Windows/older kernels adviseCold()
		// no-ops and reports zero, so only assert it did work where supported.
		const [major = 0, minor = 0] =
			process.platform === 'linux' ? release().split('.').map(Number) : [];
		const madvColdSupported = major > 5 || (major === 5 && minor >= 4);

		it('should advise mapped logs cold without corrupting their contents', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('cool');
				// Write enough to span many pages so the floored advice length is
				// non-zero even on large (16K/64K) page sizes.
				const value = Buffer.alloc(4096, 'z');
				const entryCount = 64;
				await db.transaction(async (txn) => {
					for (let i = 0; i < entryCount; i++) {
						log.addEntry(value, txn.id);
					}
				});

				// Querying maps the log file (the file-backed mmap exposed to JS).
				const before = Array.from(log.query({ start: 0 }));
				expect(before.length).toBe(entryCount);

				const result = coolTransactionLogs();
				expect(typeof result.maps).toBe('number');
				expect(typeof result.bytes).toBe('number');
				if (madvColdSupported) {
					expect(result.maps).toBeGreaterThanOrEqual(1);
					expect(result.bytes).toBeGreaterThan(0);
				}

				// MADV_COLD is non-destructive: the log must still read back intact.
				const after = Array.from(log.query({ start: 0 }));
				expect(after.length).toBe(entryCount);
				for (let i = 0; i < entryCount; i++) {
					expect(after[i].data.equals(value)).toBe(true);
				}
			}));

		it('should be a safe no-op when no logs are mapped', () =>
			dbRunner(async ({ db }) => {
				db.open();
				// No log written/mapped for this db; cooling must not throw and must
				// return a well-formed result.
				const result = coolTransactionLogs();
				expect(typeof result.maps).toBe('number');
				expect(typeof result.bytes).toBe('number');
				expect(result.bytes).toBeGreaterThanOrEqual(0);
			}));
	});
});
