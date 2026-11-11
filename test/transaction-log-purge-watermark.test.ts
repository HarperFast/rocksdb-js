import { backups, RocksDatabase, validateTransactionLogStore } from '../src/index.ts';
import { dbRunner, generateDBPath, runFixture } from './lib/util.ts';
import { cpSync, existsSync, mkdirSync, rmSync, utimesSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

// test/fixtures/txnlog-2.13.0: segment 1 (deleted by 2.13.0) held AHEAD_1; segment 2 holds 2000 and
// AHEAD_2; segment 3 holds 3000.
const RELEASE_213_STORE = join(__dirname, 'fixtures', 'txnlog-2.13.0', 'store');
const AHEAD_1 = Date.UTC(2100, 0, 1);
const AHEAD_2 = AHEAD_1 + 1000;

function copyRelease213Store(dbPath: string): string {
	const storePath = join(dbPath, 'transaction_logs', 'legacy');
	mkdirSync(join(dbPath, 'transaction_logs'), { recursive: true });
	cpSync(RELEASE_213_STORE, storePath, { recursive: true });
	return storePath;
}

const tempPaths: string[] = [];

function tempPath(): string {
	const p = generateDBPath();
	tempPaths.push(p);
	return p;
}

async function write(db: RocksDatabase, timestamp: number, size: number): Promise<void> {
	const log = db.useLog('foo');
	await db.transaction(async (txn) => {
		txn.setTimestamp(timestamp);
		log.addEntry(Buffer.alloc(size, 1), txn.id);
		db.putSync(`k${timestamp}`, 'v', { transaction: txn });
	});
}

/**
 * Segment 1 holds keys 1000, 5000 and 2000; segment 2 holds 3000 and an unflushed 1500, so a purge
 * deletes segment 1 only. 5000 is above every key left in the log.
 */
async function writeOutOfOrderSegments(db: RocksDatabase): Promise<void> {
	await write(db, 1000, 100);
	await write(db, 5000, 100);
	await write(db, 2000, 100);
	await write(db, 3000, 300);
	db.flushSync();
	await write(db, 1500, 50);
}

function age(file: string): void {
	const old = new Date(Date.now() - 2 * 60 * 60 * 1000);
	utimesSync(file, old, old);
}

describe('log.getPurgeWatermark()', () => {
	afterEach(() => {
		for (const p of tempPaths) {
			rmSync(p, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
		}
		tempPaths.length = 0;
	});

	it('is known with nothing purged for a new log', () =>
		dbRunner(async ({ db }) => {
			expect(db.useLog('foo').getPurgeWatermark()).toEqual({ known: true, purgedThrough: 0 });
		}));

	// On releases without the watermark, nothing reports 5000 once segment 1 is gone: the highest
	// readable key is 3000.
	it('reports the true maximum of a purged segment, above every key left in the log', () =>
		dbRunner({ dbOptions: [{ transactionLogMaxSize: 400 }] }, async ({ db, dbPath }) => {
			const log = db.useLog('foo');
			await writeOutOfOrderSegments(db);
			expect(db.purgeLogs({ name: 'foo', before: Date.now() + 1000 })).toEqual([
				join(dbPath, 'transaction_logs', 'foo', '1.txnlog'),
			]);
			expect(Math.max(...Array.from(log.query({ start: 0 })).map((entry) => entry.timestamp))).toBe(
				3000
			);
			expect(log.getPurgeWatermark()).toEqual({ known: true, purgedThrough: 5000 });

			db.close();
			db.open();
			expect(db.useLog('foo').getPurgeWatermark()).toEqual({ known: true, purgedThrough: 5000 });
		}));

	it('is raised by the native retention when a database opens', () =>
		dbRunner(
			{ dbOptions: [{ transactionLogMaxSize: 400 }], skipOpen: true },
			async ({ db, dbPath }) => {
				db.open();
				await writeOutOfOrderSegments(db);
				db.close();
				const segment1 = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				age(segment1);

				const reopened = RocksDatabase.open(dbPath, { transactionLogRetention: 60 * 60 * 1000 });
				try {
					expect(existsSync(segment1)).toBe(false);
					expect(reopened.useLog('foo').getPurgeWatermark()).toEqual({
						known: true,
						purgedThrough: 5000,
					});
				} finally {
					reopened.close();
				}
			}
		));

	it('is unknown for a log created by a release without it', () =>
		dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
			db.open();
			await write(db, 1000, 100);
			db.close();
			rmSync(join(dbPath, 'transaction_logs', 'foo', 'txn.purged'));

			db.open();
			expect(db.useLog('foo').getPurgeWatermark()).toEqual({ known: false });
		}));

	it('survives a backup and restore', () =>
		dbRunner({ dbOptions: [{ transactionLogMaxSize: 400 }] }, async ({ db }) => {
			await writeOutOfOrderSegments(db);
			db.purgeLogs({ name: 'foo', before: Date.now() + 1000 });

			const backupDir = tempPath();
			await db.backup(backupDir, { transactionLogs: true });
			const restoreDir = tempPath();
			await backups.restore(backupDir, restoreDir);

			const restored = RocksDatabase.open(restoreDir);
			try {
				expect(restored.useLog('foo').getPurgeWatermark()).toEqual({
					known: true,
					purgedThrough: 5000,
				});
			} finally {
				restored.close();
			}
		}));

	it('starts a new history after the log is destroyed', () =>
		dbRunner({ dbOptions: [{ transactionLogMaxSize: 400 }] }, async ({ db }) => {
			await writeOutOfOrderSegments(db);
			db.purgeLogs({ name: 'foo', before: Date.now() + 1000 });
			db.purgeLogs({ name: 'foo', destroy: true });
			// no store exists until a write recreates one, and reading never creates one
			expect(db.useLog('foo').getPurgeWatermark()).toEqual({ known: false });
			await write(db, 9000, 10);
			expect(db.useLog('foo').getPurgeWatermark()).toEqual({ known: true, purgedThrough: 0 });
		}));

	describe('setPurgeBaseline()', () => {
		it('makes a log 2.13.0 wrote known at the baseline', () =>
			dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
				const storePath = copyRelease213Store(dbPath);
				db.open();
				const log = db.useLog('legacy');
				expect(log.getPurgeWatermark()).toEqual({ known: false });
				expect(Array.from(log.query({ start: 0 })).map((entry) => entry.timestamp)).toEqual([
					2000,
					AHEAD_2,
					3000,
				]);
				expect(log.setPurgeBaseline(AHEAD_1)).toEqual({ known: true, purgedThrough: AHEAD_1 });
				expect(log.getPurgeWatermark()).toEqual({ known: true, purgedThrough: AHEAD_1 });
				expect((await validateTransactionLogStore(storePath)).warnings).toEqual([]);

				db.close();
				db.open();
				expect(db.useLog('legacy').getPurgeWatermark()).toEqual({
					known: true,
					purgedThrough: AHEAD_1,
				});
			}));

		it('never reports below a key ahead of the clock that a purge deleted', () =>
			dbRunner({ skipOpen: true }, async ({ dbPath }) => {
				const storePath = copyRelease213Store(dbPath);
				age(join(storePath, '2.txnlog'));
				const reopened = RocksDatabase.open(dbPath, { transactionLogRetention: 60 * 60 * 1000 });
				try {
					expect(existsSync(join(storePath, '2.txnlog'))).toBe(false);
					const log = reopened.useLog('legacy');
					expect(log.getPurgeWatermark()).toEqual({ known: false });
					expect(log.setPurgeBaseline(3000)).toEqual({ known: true, purgedThrough: AHEAD_2 });
				} finally {
					reopened.close();
				}
			}));

		it('leaves a known watermark as it is', () =>
			dbRunner(async ({ db }) => {
				await write(db, 1000, 100);
				expect(db.useLog('foo').setPurgeBaseline(9000)).toEqual({
					known: true,
					purgedThrough: 0,
				});
			}));

		it('leaves a log unknown when a deletion after the record was not recorded', () =>
			dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
				const storePath = copyRelease213Store(dbPath);
				db.open();
				db.close();
				rmSync(join(storePath, '2.txnlog'));
				expect((await validateTransactionLogStore(storePath)).warnings).toEqual([
					'Log files 2..2 were deleted without updating txn.purged; the log can no longer be given a purge baseline',
				]);
				db.open();
				expect(db.useLog('legacy').setPurgeBaseline(AHEAD_2)).toEqual({ known: false });
			}));

		it('carries an unadopted log through a backup that verifies and restores adoptable', () =>
			dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
				copyRelease213Store(dbPath);
				db.open();
				const backupDir = tempPath();
				await db.backup(backupDir, { transactionLogs: true });
				await expect(backups.verify(backupDir, 1)).resolves.toBeUndefined();
				const restoreDir = tempPath();
				await backups.restore(backupDir, restoreDir);
				const restored = RocksDatabase.open(restoreDir);
				try {
					const log = restored.useLog('legacy');
					expect(log.getPurgeWatermark()).toEqual({ known: false });
					expect(log.setPurgeBaseline(AHEAD_1)).toEqual({ known: true, purgedThrough: AHEAD_1 });
				} finally {
					restored.close();
				}
			}));

		// The caller's synchronous boot purge: adopt, commit a floor, sync the WAL, purge, then die.
		it('keeps the floor and the watermark across a kill right after a boot purge', async () => {
			const dbPath = tempPath();
			const storePath = copyRelease213Store(dbPath);
			await runFixture(join(__dirname, 'fixtures', 'fork-purge-baseline-boot.mts'), dbPath, {
				killsItself: true,
			});
			expect(existsSync(join(storePath, '2.txnlog'))).toBe(false);
			const db = RocksDatabase.open(dbPath);
			try {
				expect(db.getSync('floor')).toBe(4242);
				expect(db.useLog('legacy').getPurgeWatermark()).toEqual({
					known: true,
					purgedThrough: AHEAD_2,
				});
			} finally {
				db.close();
			}
		}, 20_000);

		it('rejects a baseline that is not a finite number at or above 0', () =>
			dbRunner(async ({ db }) => {
				await write(db, 1000, 100);
				const log = db.useLog('foo');
				for (const baseline of [Number.NaN, -1, Number.POSITIVE_INFINITY, '5', undefined]) {
					expect(() => log.setPurgeBaseline(baseline as number)).toThrow(TypeError);
				}
			}));

		it('is refused by a read-only database, which still sees the writer adopt it', () =>
			dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
				copyRelease213Store(dbPath);
				db.open();
				const reader = RocksDatabase.open(dbPath, { readOnly: true });
				try {
					const readerLog = reader.useLog('legacy');
					expect(() => readerLog.setPurgeBaseline(5000)).toThrow(
						'Unsupported operation in read-only mode'
					);
					expect(readerLog.getPurgeWatermark()).toEqual({ known: false });
					db.useLog('legacy').setPurgeBaseline(5000);
					expect(readerLog.getPurgeWatermark()).toEqual({ known: true, purgedThrough: 5000 });
				} finally {
					reader.close();
				}
			}));
	});

	it('is checked by the validator', () =>
		dbRunner(async ({ db }) => {
			await write(db, 1000, 100);
			const storePath = join(db.path, 'transaction_logs', 'foo');
			expect((await validateTransactionLogStore(storePath)).warnings).toEqual([]);

			writeFileSync(join(storePath, 'txn.purged'), Buffer.alloc(32, 0xff));
			expect((await validateTransactionLogStore(storePath)).warnings).toEqual([
				"txn.purged holds no valid record; the log's purge watermark reads as unknown",
			]);
		}));
});
