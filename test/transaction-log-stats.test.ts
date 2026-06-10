import { constants, stats as nativeStats } from '../src/load-binding.js';
import { dbRunner, generateDBPath } from './lib/util.js';
import { utimes } from 'node:fs/promises';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const { TRANSACTION_LOG_FILE_HEADER_SIZE, TRANSACTION_LOG_ENTRY_HEADER_SIZE } = constants;

describe('Transaction Log Stats', () => {
	describe('log.getStats()', () => {
		it('should report file, transaction, and lifetime counters', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('stats');
				const value = Buffer.alloc(100, 'a');
				const count = 5;
				for (let i = 0; i < count; i++) {
					await db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
					});
				}

				const stats = log.getStats();
				expect(stats.name).toBe('stats');
				expect(stats.path).toBe(join(db.path, 'transaction_logs', 'stats'));
				expect(stats.fileCount).toBe(1);
				expect(stats.currentSequenceNumber).toBe(1);
				expect(stats.oldestSequenceNumber).toBe(1);

				const entryBytes = (TRANSACTION_LOG_ENTRY_HEADER_SIZE + value.length) * count;
				expect(stats.totalSizeBytes).toBe(TRANSACTION_LOG_FILE_HEADER_SIZE + entryBytes);
				expect(stats.currentFileSize).toBe(stats.totalSizeBytes);

				// lifetime counters: one transaction/entry per commit; bytesWritten
				// counts entry payload+header but not the file header.
				expect(stats.totals.transactionsWritten).toBe(count);
				expect(stats.totals.entriesWritten).toBe(count);
				expect(stats.totals.bytesWritten).toBe(entryBytes);
				expect(stats.totals.writeFailures).toBe(0);

				// no rotation, no purge yet
				expect(stats.totals.rotations).toBe(0);
				expect(stats.totals.purgeRuns).toBe(0);
				expect(stats.totals.filesPurged).toBe(0);

				// positions
				expect(stats.nextLogPosition.sequence).toBe(1);
				expect(stats.nextLogPosition.offset).toBe(stats.totalSizeBytes);
				expect(stats.lastFlushedPosition).toEqual({ sequence: 0, offset: 0 });

				// config reflects defaults
				expect(stats.config.maxFileSize).toBeGreaterThan(0);
			}));

		it('should report memory-map usage after a file is mapped', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('mem');
				const value = Buffer.alloc(100, 'a');
				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
				});

				// querying maps the file
				expect(Array.from(log.query({ start: 0 })).length).toBe(1);

				const stats = log.getStats();
				expect(stats.memory.activeMaps).toBeGreaterThanOrEqual(1);
				// the active write file is mapped at the full configured maxFileSize
				// on POSIX, so mapped bytes are at least the on-disk content.
				expect(stats.memory.mappedBytes).toBeGreaterThanOrEqual(stats.totalSizeBytes);
			}));

		it('should count rotations and aggregate across multiple files', () =>
			dbRunner(
				{ dbOptions: [{ path: generateDBPath(), transactionLogMaxSize: 4096 }] },
				async ({ db }) => {
					const log = db.useLog('rotate');
					const value = Buffer.alloc(1000, 'a');
					for (let i = 0; i < 20; i++) {
						await db.transaction(async (txn) => {
							log.addEntry(value, txn.id);
						});
					}

					const stats = log.getStats();
					expect(stats.fileCount).toBeGreaterThan(1);
					expect(stats.totals.rotations).toBeGreaterThanOrEqual(1);
					expect(stats.currentSequenceNumber).toBeGreaterThan(1);
					expect(stats.oldestSequenceNumber).toBe(1);
					// total spans all sequence files
					expect(stats.totalSizeBytes).toBeGreaterThan(4096);
				}
			));

		it('should report retained-but-unflushed files past retention', () =>
			dbRunner(
				{ dbOptions: [{ path: generateDBPath(), transactionLogRetention: 1000 }] },
				async ({ db }) => {
					const log = db.useLog('retain');
					const value = Buffer.alloc(100, 'a');
					await db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
					});

					// age the file well past the 1ms retention window
					const filePath = join(db.path, 'transaction_logs', 'retain', '1.txnlog');
					const old = new Date(Date.now() - 60_000);
					await utimes(filePath, old, old);

					const stats = log.getStats();
					// nothing has been flushed to RocksDB, so the old file is retained
					// rather than purgeable (it would be unsafe to drop).
					expect(stats.purge.retainedUnflushedFiles).toBeGreaterThanOrEqual(1);
					expect(stats.purge.purgeableFiles).toBe(0);
					expect(stats.purge.oldestFileAgeMs).toBeGreaterThan(1000);
				}
			));

		it('should increment purgeRuns when logs are purged', () =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('purge');
				const value = Buffer.alloc(100, 'a');
				await db.transaction(async (txn) => {
					log.addEntry(value, txn.id);
				});

				expect(log.getStats().totals.purgeRuns).toBe(0);
				db.purgeLogs({ name: 'purge' });
				expect(log.getStats().totals.purgeRuns).toBeGreaterThanOrEqual(1);
				expect(log.getStats().purge.lastPurgeMs).toBeGreaterThan(0);
			}));
	});

	describe('db.getStats() transaction log summary', () => {
		it('should aggregate txnlog.* keys across all logs (even with stats disabled)', () =>
			dbRunner(async ({ db }) => {
				const value = Buffer.alloc(100, 'a');
				const logA = db.useLog('a');
				const logB = db.useLog('b');
				for (let i = 0; i < 3; i++) {
					await db.transaction(async (txn) => {
						logA.addEntry(value, txn.id);
					});
				}
				for (let i = 0; i < 2; i++) {
					await db.transaction(async (txn) => {
						logB.addEntry(value, txn.id);
					});
				}

				const stats = db.getStats();
				expect(stats['txnlog.logCount']).toBe(2);
				expect(stats['txnlog.transactionsWritten']).toBe(5);

				const statsA = logA.getStats();
				const statsB = logB.getStats();
				expect(stats['txnlog.totalSizeBytes']).toBe(statsA.totalSizeBytes + statsB.totalSizeBytes);
				expect(stats['txnlog.fileCount']).toBe(statsA.fileCount + statsB.fileCount);
			}));

		it('should report zeroed txnlog summary when there are no logs', () =>
			dbRunner(async ({ db }) => {
				const stats = db.getStats();
				expect(stats['txnlog.logCount']).toBe(0);
				expect(stats['txnlog.totalSizeBytes']).toBe(0);
				expect(stats['txnlog.mappedBytes']).toBe(0);
			}));
	});

	describe('txnlog tickers and db.getStat()', () => {
		it('should list txnlog summary keys in stats.tickers', () => {
			expect(nativeStats.tickers).toContain('txnlog.logCount');
			expect(nativeStats.tickers).toContain('txnlog.totalSizeBytes');
			expect(nativeStats.tickers).toContain('txnlog.mappedBytes');
			expect(nativeStats.tickers).toContain('txnlog.replayGapBytes');
			// the RocksDB tickers are still present
			expect(nativeStats.tickers).toContain('rocksdb.number.keys.written');
		});

		it('should resolve txnlog summary keys via db.getStat()', () =>
			dbRunner(async ({ db }) => {
				const value = Buffer.alloc(100, 'a');
				const log = db.useLog('getstat');
				for (let i = 0; i < 3; i++) {
					await db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
					});
				}

				expect(db.getStat('txnlog.logCount')).toBe(1);
				expect(db.getStat('txnlog.transactionsWritten')).toBe(3);
				// matches the aggregate exposed by getStats()
				expect(db.getStat('txnlog.totalSizeBytes')).toBe(db.getStats()['txnlog.totalSizeBytes']);
				// unknown txnlog key resolves to undefined
				expect(db.getStat('txnlog.bogus')).toBeUndefined();
			}));
	});
});
