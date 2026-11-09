import { backups, RocksDatabase, validateTransactionLogStore } from '../src/index.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { existsSync, rmSync, utimesSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

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
