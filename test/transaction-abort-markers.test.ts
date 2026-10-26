import { backups, RocksDatabase } from '../src/index.ts';
import { constants } from '../src/load-binding.ts';
import { parseTransactionLog } from '../src/parse-transaction-log.ts';
import { ConflictKind, RETRY_NOW, Transaction } from '../src/transaction.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { existsSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { afterEach, describe, expect, it } from 'vitest';

const { TRANSACTION_LOG_ENTRY_ABORTED_FLAG, TRANSACTION_LOG_ENTRY_LAST_FLAG } = constants;

const settled = <T>(promise: Promise<T>) => {
	let done = false;
	promise.then(
		() => (done = true),
		() => (done = true)
	);
	return new Promise<boolean>((resolve) => setImmediate(() => setImmediate(() => resolve(done))));
};

const logFlags = (dbPath: string, name = 'foo') => {
	const path = join(dbPath, 'transaction_logs', name, '1.txnlog');
	return existsSync(path) ? parseTransactionLog(path).entries.map((entry) => entry.flags) : [];
};

const readEntries = (db: RocksDatabase, name = 'foo') =>
	Array.from(db.useLog(name).query({ start: 0 }), (entry) => entry.data.toString());

describe('transaction-log abort markers', () => {
	afterEach(() => {
		RocksDatabase.config({ transactionLogAbortMarkers: false, transactionLogWriteIntents: false });
	});

	it.each([
		['the flag off', false],
		['write intents at their default', true],
	])('leaves contention unchanged with %s: no refusals, intent parks or abort bits', (_, markers) =>
		dbRunner({ dbOptions: [{ verificationTable: true }] }, async ({ db, dbPath }) => {
			RocksDatabase.config({ transactionLogAbortMarkers: markers });
			await db.put('hot', 'v0');
			db.populateVersion('hot', 1.5e12);
			const log = db.useLog('foo');
			const kinds: number[] = [];
			const refused: boolean[] = [];

			await Promise.all(
				Array.from({ length: 8 }, (_, i) =>
					db
						.transaction(
							async (txn, attempt) => {
								if (attempt > 1) {
									kinds.push(txn.lastConflictKind());
								}
								log.addEntry(Buffer.from(`w${i}`), txn.id);
								await txn.put('hot', `v${i}`);
								await delay(5);
							},
							{ maxRetries: 50 }
						)
						.catch((err) => refused.push(err.refusedBeforeLog))
				)
			);

			expect(kinds.length).toBeGreaterThan(0);
			expect(kinds.every((kind) => kind === ConflictKind.atCommit)).toBe(true);
			expect(refused).toEqual([]);
			expect(logFlags(dbPath).some((flags) => flags & TRANSACTION_LOG_ENTRY_ABORTED_FLAG)).toBe(
				false
			);
			expect(log.getStats().totals.abortsMarked).toBe(0);
		})
	);

	it('marks a batch aborted when its transaction ends without committing, and readers skip it', () =>
		dbRunner(async ({ db, dbPath }) => {
			RocksDatabase.config({ transactionLogAbortMarkers: true });
			const log = db.useLog('foo');

			const first = db.transaction(
				async (txn, attempt) => {
					log.addEntry(Buffer.from('lost-1'), txn.id);
					log.addEntry(Buffer.from('lost-2'), txn.id);
					await txn.put('foo', 'first');
					if (attempt === 1) {
						await delay(50);
					}
				},
				{ retryOnBusy: false }
			);
			await db.transaction(async (txn) => {
				log.addEntry(Buffer.from('won'), txn.id);
				await txn.put('foo', 'second');
			});

			// The abort succeeded, so the commit conflict surfaces instead of ERR_TRANSACTION_ABANDONED.
			await expect(first).rejects.toMatchObject({ code: 'ERR_BUSY' });

			expect(logFlags(dbPath)).toEqual([
				TRANSACTION_LOG_ENTRY_LAST_FLAG,
				TRANSACTION_LOG_ENTRY_ABORTED_FLAG,
				TRANSACTION_LOG_ENTRY_LAST_FLAG,
			]);
			expect(readEntries(db)).toEqual(['won']);
			expect(log.getStats().totals.abortsMarked).toBe(1);
		}));

	it('releases the watermark only after marking, so committed readers never see the batch', () =>
		dbRunner(async ({ db }) => {
			RocksDatabase.config({ transactionLogAbortMarkers: true });
			const log = db.useLog('foo');
			let releaseRetry!: () => void;
			const retryGate = new Promise<void>((resolve) => (releaseRetry = resolve));
			let logged!: () => void;
			const loggedAndConflicted = new Promise<void>((resolve) => (logged = resolve));

			const pinned = db.transaction(async (txn, attempt) => {
				if (attempt === 1) {
					log.addEntry(Buffer.from('pinned'), txn.id);
					await txn.put('k', 'pinned');
					await delay(50);
					return;
				}
				// The retry keeps attempt 1's batch; hold the transaction open with it pinned.
				expect(txn.logWritten()).toBe(true);
				logged();
				await retryGate;
				throw new Error('give up');
			});
			await db.transaction(async (txn) => {
				log.addEntry(Buffer.from('before'), txn.id);
				await txn.put('k', 'conflict');
			});
			await loggedAndConflicted;

			await db.transaction(async (txn) => {
				log.addEntry(Buffer.from('after'), txn.id);
				await txn.put('other', 'v');
			});
			expect(readEntries(db)).toEqual(['before']);

			releaseRetry();
			await expect(pinned).rejects.toThrow('give up');
			expect(readEntries(db)).toEqual(['before', 'after']);
		}));

	it('refuses a younger attempt before it logs when an older transaction holds its key', () =>
		dbRunner({ dbOptions: [{ verificationTable: true }] }, async ({ db, dbPath }) => {
			RocksDatabase.config({ transactionLogAbortMarkers: true, transactionLogWriteIntents: true });
			await db.put('hot', 'v0');
			db.populateVersion('hot', 1.5e12);
			const log = db.useLog('foo');

			const older = new Transaction(db.store, { coordinatedRetry: true });
			await older.put('hot', 'older');
			const younger = new Transaction(db.store, { coordinatedRetry: true });
			log.addEntry(Buffer.from('younger'), younger.id);
			await younger.put('hot', 'younger');

			const parked = younger.commit();
			expect(await settled(parked)).toBe(false);

			await older.commit();
			expect(await parked).toBe(RETRY_NOW);
			expect(younger.logWritten()).toBe(false);
			expect(younger.lastConflictKind()).toBe(ConflictKind.intentBeforeLog);
			expect(logFlags(dbPath)).toEqual([]);
			younger.abort();
		}));

	it('reports refusedBeforeLog on a refused attempt and retries it by default', () =>
		dbRunner({ dbOptions: [{ verificationTable: true }] }, async ({ db, dbPath }) => {
			RocksDatabase.config({ transactionLogAbortMarkers: true, transactionLogWriteIntents: true });
			await db.put('hot', 'v0');
			db.populateVersion('hot', 1.5e12);
			const log = db.useLog('foo');

			const older = new Transaction(db.store);
			await older.put('hot', 'older');
			const younger = new Transaction(db.store);
			log.addEntry(Buffer.from('younger'), younger.id);
			await younger.put('hot', 'younger');
			await expect(younger.commit()).rejects.toMatchObject({
				code: 'ERR_BUSY',
				refusedBeforeLog: true,
				hasLog: false,
			});
			younger.abort();
			older.abort();

			const holder = new Transaction(db.store);
			await holder.put('hot', 'held');
			let attempts = 0;
			const retried = db.transaction(async (txn) => {
				attempts++;
				log.addEntry(Buffer.from('retried'), txn.id);
				await txn.put('hot', 'retried');
				if (attempts === 1) {
					holder.abort();
				}
			});
			await retried;
			// attempt 1 joined the holder's intent and was refused before logging; attempt 2 committed
			expect(attempts).toBe(2);

			const blocker = new Transaction(db.store);
			await blocker.put('hot', 'blocking');
			await expect(
				db.transaction(
					async (txn) => {
						log.addEntry(Buffer.from('no-retry'), txn.id);
						await txn.put('hot', 'no-retry');
					},
					{ retryOnBusy: false }
				)
			).rejects.toMatchObject({ refusedBeforeLog: true });
			blocker.abort();
			expect(readEntries(db)).toEqual(['retried']);
			expect(logFlags(dbPath)).toEqual([TRANSACTION_LOG_ENTRY_LAST_FLAG]);
		}));

	it('does not refuse an attempt behind an older write intent unless write intents are on', () =>
		dbRunner({ dbOptions: [{ verificationTable: true }] }, async ({ db, dbPath }) => {
			RocksDatabase.config({ transactionLogAbortMarkers: true });
			await db.put('hot', 'v0');
			db.populateVersion('hot', 1.5e12);
			const log = db.useLog('foo');

			const older = new Transaction(db.store, { coordinatedRetry: true });
			await older.put('hot', 'older');
			const younger = new Transaction(db.store, { coordinatedRetry: true });
			log.addEntry(Buffer.from('younger'), younger.id);
			await younger.put('hot', 'younger');
			await younger.commit();
			expect(younger.lastConflictKind()).toBe(ConflictKind.none);

			expect(await older.commit()).toBe(RETRY_NOW);
			expect(older.lastConflictKind()).toBe(ConflictKind.atCommit);
			older.abort();

			const syncHolder = new Transaction(db.store);
			await syncHolder.put('hot', 'held');
			let attempts = 0;
			db.transactionSync((txn) => {
				attempts++;
				log.addEntry(Buffer.from('sync'), txn.id);
				txn.putSync('hot', 'sync');
			});
			expect(attempts).toBe(1);
			syncHolder.abort();
			expect(readEntries(db)).toEqual(['younger', 'sync']);
			expect(logFlags(dbPath).some((flags) => flags & TRANSACTION_LOG_ENTRY_ABORTED_FLAG)).toBe(
				false
			);
		}));

	it('reads write intents when an attempt begins and again at each retry', () =>
		dbRunner({ dbOptions: [{ verificationTable: true }] }, async ({ db, dbPath }) => {
			RocksDatabase.config({ transactionLogAbortMarkers: true, transactionLogWriteIntents: true });
			await db.put('hot', 'v0');
			db.populateVersion('hot', 1.5e12);
			const log = db.useLog('foo');

			// On to off: attempt 1 began with intents on and is refused; the retry began with them off.
			const holder = new Transaction(db.store);
			await holder.put('hot', 'held');
			const onThenOff = new Transaction(db.store, { coordinatedRetry: true });
			log.addEntry(Buffer.from('on-then-off'), onThenOff.id);
			await onThenOff.put('hot', 'on-then-off');
			RocksDatabase.config({ transactionLogWriteIntents: false });
			const parked = onThenOff.commit();
			expect(await settled(parked)).toBe(false);
			holder.abort();
			expect(await parked).toBe(RETRY_NOW);
			expect(onThenOff.lastConflictKind()).toBe(ConflictKind.intentBeforeLog);
			// Taken with intents off, holder2's intent carries no age, so an aged retry would treat it as
			// possibly older and be refused.
			const holder2 = new Transaction(db.store);
			await holder2.put('hot', 'held');
			log.addEntry(Buffer.from('on-then-off'), onThenOff.id);
			await onThenOff.put('hot', 'on-then-off');
			expect(await onThenOff.commit()).not.toBe(RETRY_NOW);
			holder2.abort();

			// Off to on: attempt 1 began with intents off and logs; its retry began with them on and is
			// refused behind an older holder after the log write.
			const older = new Transaction(db.store);
			const offThenOn = new Transaction(db.store, { coordinatedRetry: true });
			log.addEntry(Buffer.from('off-then-on'), offThenOn.id);
			await offThenOn.put('hot', 'off-then-on');
			await db.put('hot', 'conflict');
			RocksDatabase.config({ transactionLogWriteIntents: true });
			expect(await offThenOn.commit()).toBe(RETRY_NOW);
			expect(offThenOn.logWritten()).toBe(true);
			expect(offThenOn.lastConflictKind()).toBe(ConflictKind.atCommit);
			await older.put('hot', 'older');
			await offThenOn.put('hot', 'off-then-on');
			const parkedAfterLog = offThenOn.commit();
			expect(await settled(parkedAfterLog)).toBe(false);
			older.abort();
			expect(await parkedAfterLog).toBe(RETRY_NOW);
			expect(offThenOn.lastConflictKind()).toBe(ConflictKind.intentAfterLog);
			await offThenOn.put('hot', 'off-then-on');
			await offThenOn.commit();

			expect(readEntries(db)).toEqual(['on-then-off', 'off-then-on']);
			expect(logFlags(dbPath).some((flags) => flags & TRANSACTION_LOG_ENTRY_ABORTED_FLAG)).toBe(
				false
			);
		}));

	it('lets the older of two crossing transactions proceed', () =>
		dbRunner({ dbOptions: [{ verificationTable: true }] }, async ({ db }) => {
			RocksDatabase.config({ transactionLogAbortMarkers: true, transactionLogWriteIntents: true });
			await db.put('hot', 'v0');
			db.populateVersion('hot', 1.5e12);
			const log = db.useLog('foo');

			const older = new Transaction(db.store);
			const younger = new Transaction(db.store);
			log.addEntry(Buffer.from('younger'), younger.id);
			await younger.put('hot', 'younger');
			log.addEntry(Buffer.from('older'), older.id);
			await older.put('hot', 'older');

			await older.commit();
			expect(older.lastConflictKind()).toBe(ConflictKind.none);
			await expect(younger.commit()).rejects.toMatchObject({ code: 'ERR_BUSY' });
			younger.abort();
			expect(readEntries(db)).toEqual(['older']);
		}));

	it('parks a logged retry on an older intent without writing its batch again', () =>
		dbRunner({ dbOptions: [{ verificationTable: true }] }, async ({ db, dbPath }) => {
			RocksDatabase.config({ transactionLogAbortMarkers: true, transactionLogWriteIntents: true });
			await db.put('hot', 'v0');
			db.populateVersion('hot', 1.5e12);
			const log = db.useLog('foo');

			const older = new Transaction(db.store);
			const logged = new Transaction(db.store, { coordinatedRetry: true });
			log.addEntry(Buffer.from('logged'), logged.id);
			await logged.put('hot', 'logged');
			await db.put('hot', 'conflict');
			expect(await logged.commit()).toBe(RETRY_NOW);
			expect(logged.logWritten()).toBe(true);
			expect(logged.lastConflictKind()).toBe(ConflictKind.atCommit);
			const entriesLogged = logFlags(dbPath).length;

			await older.put('hot', 'older');
			await logged.put('hot', 'logged-retry');
			const parked = logged.commit();
			expect(await settled(parked)).toBe(false);

			older.abort();
			expect(await parked).toBe(RETRY_NOW);
			expect(logged.lastConflictKind()).toBe(ConflictKind.intentAfterLog);
			await logged.put('hot', 'logged-final');
			await logged.commit();
			expect(logFlags(dbPath).length).toBe(entriesLogged);
			expect(readEntries(db)).toEqual(['logged']);
			expect(await db.get('hot')).toBe('logged-final');
			// The batch held the watermark from its first conflict until it committed.
			const totals = log.getStats().totals;
			expect(totals.pinnedTransactions).toBe(1);
			expect(totals.watermarkPinnedUs).toBeGreaterThan(0);
		}));

	it('makes a backup wait for a captured unresolved batch, so the restore skips it once aborted', () =>
		dbRunner(async ({ db, dbPath }) => {
			RocksDatabase.config({ transactionLogAbortMarkers: true });
			const log = db.useLog('foo');
			await db.transaction(async (txn) => {
				log.addEntry(Buffer.from('committed'), txn.id);
				await txn.put('k', 'committed');
			});
			const pinned = new Transaction(db.store);
			log.addEntry(Buffer.from('pinned'), pinned.id);
			await pinned.put('k', 'pinned');
			await db.put('k', 'conflict');
			await expect(pinned.commit()).rejects.toMatchObject({ code: 'ERR_BUSY' });

			const backupDir = `${dbPath}-backup`;
			const backup = db.backup(backupDir, { transactionLogs: true });
			await delay(100);
			expect(await settled(backup)).toBe(false);
			pinned.abort();
			await backup;

			const restoreDir = `${dbPath}-restore`;
			await backups.restore(backupDir, restoreDir);
			expect(logFlags(restoreDir)).toEqual([
				TRANSACTION_LOG_ENTRY_LAST_FLAG,
				TRANSACTION_LOG_ENTRY_ABORTED_FLAG | TRANSACTION_LOG_ENTRY_LAST_FLAG,
			]);
			const restored = RocksDatabase.open(restoreDir);
			try {
				expect(readEntries(restored)).toEqual(['committed']);
			} finally {
				restored.close();
			}
			rmSync(backupDir, { recursive: true, force: true });
			rmSync(restoreDir, { recursive: true, force: true });
		}));

	it('stops a backup waiting on an unresolved batch when the database closes', () =>
		dbRunner(async ({ db, dbPath }) => {
			RocksDatabase.config({ transactionLogAbortMarkers: true });
			const log = db.useLog('foo');
			const pinned = new Transaction(db.store);
			log.addEntry(Buffer.from('pinned'), pinned.id);
			await pinned.put('k', 'pinned');
			await db.put('k', 'conflict');
			await expect(pinned.commit()).rejects.toMatchObject({ code: 'ERR_BUSY' });

			const backup = db.backup(`${dbPath}-backup`, {
				transactionLogs: true,
				checkDiskSpace: false,
			});
			const outcome = backup.then(
				() => 'completed',
				(err) => String(err?.message)
			);
			await delay(100);
			const started = performance.now();
			db.close();
			expect(await outcome).toMatch(/closed/);
			expect(performance.now() - started).toBeLessThan(5000);
			rmSync(`${dbPath}-backup`, { recursive: true, force: true });
		}));

	it('refuses a backup whose captured batch stays unresolved past the wait bound', async () => {
		const result = await runFixture('fork-abort-backup-timeout.mts', {
			ROCKSDB_JS_BACKUP_UNRESOLVED_WAIT_MS: '200',
		});
		expect(result).toEqual({ code: 0, signal: null, stderr: '' });
	});

	it('retries a refused transactionSync() attempt and gives up after maxRetries', () =>
		dbRunner({ dbOptions: [{ verificationTable: true }] }, async ({ db }) => {
			RocksDatabase.config({ transactionLogAbortMarkers: true, transactionLogWriteIntents: true });
			await db.put('hot', 'v0');
			db.populateVersion('hot', 1.5e12);
			const log = db.useLog('foo');

			const holder = new Transaction(db.store);
			await holder.put('hot', 'held');
			let attempts = 0;
			db.transactionSync((txn) => {
				attempts++;
				log.addEntry(Buffer.from('sync'), txn.id);
				txn.putSync('hot', 'sync');
				if (attempts === 1) {
					holder.abort();
				}
			});
			expect(attempts).toBe(2);

			const blocker = new Transaction(db.store);
			await blocker.put('hot', 'blocking');
			attempts = 0;
			expect(() =>
				db.transactionSync(
					(txn) => {
						attempts++;
						log.addEntry(Buffer.from('never'), txn.id);
						txn.putSync('hot', 'never');
					},
					{ maxRetries: 2 }
				)
			).toThrow(expect.objectContaining({ refusedBeforeLog: true }));
			expect(attempts).toBe(3);
			blocker.abort();
			expect(readEntries(db)).toEqual(['sync']);
		}));

	it('abandons a coordinated retry with retryOnBusy: false instead of retrying', () =>
		dbRunner({ dbOptions: [{ verificationTable: true }] }, async ({ db }) => {
			RocksDatabase.config({ transactionLogAbortMarkers: true, transactionLogWriteIntents: true });
			await db.put('hot', 'v0');
			db.populateVersion('hot', 1.5e12);
			const log = db.useLog('foo');

			const holder = new Transaction(db.store);
			await holder.put('hot', 'held');
			let attempts = 0;
			const result = db.transaction(
				async (txn) => {
					attempts++;
					log.addEntry(Buffer.from('once'), txn.id);
					await txn.put('hot', 'once');
				},
				{ coordinatedRetry: true, retryOnBusy: false }
			);
			await expect(result).rejects.toMatchObject({ code: 'ERR_BUSY', refusedBeforeLog: true });
			expect(attempts).toBe(1);
			holder.abort();
		}));

	it('resolves a park at the transaction park deadline', () =>
		dbRunner({ dbOptions: [{ verificationTable: true }] }, async ({ db }) => {
			RocksDatabase.config({ transactionLogAbortMarkers: true, transactionLogWriteIntents: true });
			await db.put('hot', 'v0');
			db.populateVersion('hot', 1.5e12);
			const log = db.useLog('foo');

			const holder = new Transaction(db.store);
			await holder.put('hot', 'held');
			const parked = new Transaction(db.store, { coordinatedRetry: true });
			log.addEntry(Buffer.from('parked'), parked.id);
			await parked.put('hot', 'parked');
			parked.setParkDeadline(Date.now() + 100);
			const started = performance.now();
			expect(await parked.commit()).toBe(RETRY_NOW);
			const elapsed = performance.now() - started;
			expect(elapsed).toBeGreaterThanOrEqual(80);
			expect(elapsed).toBeLessThan(2000);

			parked.setParkDeadline(Date.now() - 1);
			log.addEntry(Buffer.from('parked'), parked.id);
			await parked.put('hot', 'parked');
			expect(await parked.commit()).toBe(RETRY_NOW);
			parked.abort();
			holder.abort();
		}));

	it.skipIf(!constants.PRELOG_CONFLICT_CHECK)(
		"refuses a stale attempt before it logs with RocksDB's conflict check",
		() =>
			dbRunner(async ({ db, dbPath }) => {
				RocksDatabase.config({ transactionLogAbortMarkers: true });
				await db.put('k', 'v0');
				const log = db.useLog('foo');
				const stale = new Transaction(db.store);
				expect(await stale.get('k')).toBe('v0');
				await db.put('k', 'v1'); // committed after `stale` read the key; no intent is left behind
				log.addEntry(Buffer.from('stale'), stale.id);
				await stale.put('k', 'stale');
				await expect(stale.commit()).rejects.toMatchObject({
					code: 'ERR_BUSY',
					refusedBeforeLog: true,
					hasLog: false,
				});
				expect(stale.lastConflictKind()).toBe(ConflictKind.checkBeforeLog);
				expect(logFlags(dbPath)).toEqual([]);
				stale.abort();
			})
	);

	it('fails closed when a marker cannot be written, and resolves on a later retry', async () => {
		const result = await runFixture('fork-abort-mark-failure.mts', {
			ROCKSDB_JS_ABORT_MARK_FAILURE: '2',
		});
		expect(result).toEqual({ code: 0, signal: null, stderr: '' });
	});

	it.skipIf(!!process.versions.bun || !!process.versions.deno)(
		'treats a worker torn down with a parked logged retry as a terminal abort',
		async () => {
			const result = await runFixture('fork-abort-worker-teardown.mts');
			expect(result).toEqual({ code: 0, signal: null, stderr: '' });
		}
	);
});

function runFixture(
	fixture: string,
	env: Record<string, string> = {}
): Promise<{ code: number | null; signal: NodeJS.Signals | null; stderr: string }> {
	return new Promise((resolve, reject) => {
		const child = spawn(
			process.execPath,
			[join(__dirname, 'fixtures', fixture), generateDBPath()],
			{
				env: { ...process.env, ...env },
			}
		);
		// Hang backstop only; under vitest's timeout so the kill still reports stderr.
		const timer = setTimeout(() => child.kill(), 20_000);
		let stderr = '';
		child.stderr?.on('data', (chunk) => {
			stderr += chunk.toString();
		});
		child.on('close', (code, signal) => {
			clearTimeout(timer);
			resolve({ code, signal, stderr });
		});
		child.on('error', reject);
	});
}
