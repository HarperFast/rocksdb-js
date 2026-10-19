// The commit protocol of `src/binding/transaction/DESIGN.md` (HarperFast/harper#3253):
// an optimistic conflict is detected before the transaction-log batch is written, so an
// ordinary conflict retry leaves no durable entry; `hasDurableLog` reports the authoritative
// native state; a handle whose batch is durable can neither be aborted nor silently dropped.

import { RocksDatabase, registryStatus } from '../src/index.ts';
import {
	constants,
	forceTryAgainForTesting,
	isCommitValidatedDelayedForTesting,
	setCommitValidatedDelayForTesting,
} from '../src/load-binding.ts';
import { Transaction, TransactionLoggedError } from '../src/transaction.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { createWorkerBootstrapScript } from './lib/worker-bootstrap.ts';
import { readdir, stat } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';
import { afterEach, describe, expect, it, vi } from 'vitest';

const { TRANSACTION_LOG_FILE_HEADER_SIZE, TRANSACTION_LOG_ENTRY_HEADER_SIZE } = constants;

const forceGC: (() => void) | undefined =
	typeof globalThis.gc === 'function'
		? globalThis.gc
		: typeof (globalThis as { Bun?: { gc?: (sync: boolean) => void } }).Bun?.gc === 'function'
			? () => (globalThis as unknown as { Bun: { gc: (sync: boolean) => void } }).Bun.gc(true)
			: undefined;

async function logBytes(dbPath: string, name: string): Promise<number> {
	let size = 0;
	for (const file of await readdir(join(dbPath, 'transaction_logs', name)).catch(() => [])) {
		const info = await stat(join(dbPath, 'transaction_logs', name, file)).catch(() => undefined);
		if (info?.isFile()) size += info.size;
	}
	return size;
}

function oneEntryBytes(payloadLength: number): number {
	return TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + payloadLength;
}

type Position = { sequence: number; offset: number };
const positionAtMost = (a: Position, b: Position) =>
	a.sequence < b.sequence || (a.sequence === b.sequence && a.offset <= b.offset);

function collectWarnings(filter: string): { warnings: string[]; stop: () => void } {
	const warnings: string[] = [];
	const onWarning = (message: string) => {
		if (message.includes(filter)) warnings.push(message);
	};
	RocksDatabase.on('log.warn', onWarning);
	return { warnings, stop: () => RocksDatabase.off('log.warn', onWarning) };
}

/** A transaction whose log batch is durable but whose commit has not landed. */
async function loggedPending(db: RocksDatabase, logName: string, key: string) {
	const log = db.useLog(logName);
	const txn = new Transaction(db.store);
	txn.putSync(key, 'logged');
	log.addEntry(Buffer.from(key), txn.id);
	forceTryAgainForTesting(1);
	// Inspected without keeping the error object, which references the handle, so a caller
	// that drops the handle really drops it.
	let outcome: { code?: string; hasLog?: boolean } | undefined;
	try {
		await txn.commit();
	} catch (error) {
		outcome = {
			code: (error as { code?: string }).code,
			hasLog: (error as { hasLog?: boolean }).hasLog,
		};
	}
	expect(outcome).toEqual({ code: 'ERR_TRY_AGAIN', hasLog: true });
	expect(txn.hasDurableLog).toBe(true);
	return { txn, log };
}

describe('conflict before the transaction-log write', () => {
	afterEach(() => {
		forceTryAgainForTesting(0);
		setCommitValidatedDelayForTesting(0, 0);
	});

	for (const mode of ['async', 'sync'] as const) {
		it(`${mode}: a conflicting first attempt writes no log entry and hasDurableLog stays false until the commit that lands`, () =>
			dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
				const log = db.useLog('audit');
				const payload = Buffer.alloc(64, 'a');
				await db.put('k', 'initial');
				const durableAtStart: boolean[] = [];
				let handle: Transaction | undefined;
				const body = async (txn: Transaction, attempt: number) => {
					handle = txn;
					durableAtStart.push(txn.hasDurableLog);
					await txn.get('k');
					if (attempt === 1) await db.put('k', 'conflict');
					txn.putSync('k', 'committed');
					log.addEntry(payload, txn.id);
				};
				if (mode === 'async') {
					await db.transaction(body, { retryOnBusy: true, maxRetries: 5 });
				} else {
					await db.transactionSync(body, { retryOnBusy: true, maxRetries: 5 });
				}
				expect(durableAtStart).toEqual([false, false]);
				expect(handle!.hasDurableLog).toBe(true);
				expect(await db.get('k')).toBe('committed');
				expect(await logBytes(dbPath, 'audit')).toBe(oneEntryBytes(payload.length));
				expect([...log.query({ start: 0 })]).toHaveLength(1);
			}));
	}

	it('a rejected manual commit leaves the handle unlogged and abortable', () =>
		dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
			const log = db.useLog('audit');
			await db.put('k', 'initial');
			const txn = new Transaction(db.store);
			expect(await txn.get('k')).toBe('initial');
			await db.put('k', 'conflict');
			txn.putSync('k', 'mine');
			log.addEntry(Buffer.from('entry'), txn.id);
			// `hasLog` says the transaction carried log entries (the default-retry signal);
			// `hasDurableLog` says none of them reached the log.
			await expect(txn.commit()).rejects.toMatchObject({ code: 'ERR_BUSY', hasLog: true });
			expect(txn.hasDurableLog).toBe(false);
			expect(await logBytes(dbPath, 'audit')).toBe(0);
			expect(() => txn.abort()).not.toThrow();
			expect(await db.get('k')).toBe('conflict');
			expect([...log.query({ start: 0, readUncommitted: true })]).toHaveLength(0);
		}));

	it('a rejected commitSync leaves the handle unlogged and abortable', () =>
		dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
			const log = db.useLog('audit');
			await db.put('k', 'initial');
			const txn = new Transaction(db.store);
			expect(txn.getSync('k')).toBe('initial');
			db.putSync('k', 'conflict');
			txn.putSync('k', 'mine');
			log.addEntry(Buffer.from('entry'), txn.id);
			expect(() => txn.commitSync()).toThrow(
				expect.objectContaining({ code: 'ERR_BUSY', hasLog: true })
			);
			expect(txn.hasDurableLog).toBe(false);
			expect(await logBytes(dbPath, 'audit')).toBe(0);
			txn.abort();
			expect(db.getSync('k')).toBe('conflict');
		}));

	it('coordinated retry resolves RETRY_NOW before the log write and logs once', () =>
		dbRunner(
			{ dbOptions: [{ path: generateDBPath(), verificationTable: true }] },
			async ({ db, dbPath }) => {
				const log = db.useLog('audit');
				const payload = Buffer.alloc(32, 'c');
				await db.put('k', 'initial');
				const durableAtStart: boolean[] = [];
				await db.transaction(
					async (txn, attempt) => {
						durableAtStart.push(txn.hasDurableLog);
						await txn.get('k');
						if (attempt === 1) await db.put('k', 'conflict');
						txn.putSync('k', 'committed');
						log.addEntry(payload, txn.id);
					},
					{ coordinatedRetry: true, maxRetries: 5 }
				);
				expect(durableAtStart).toEqual([false, false]);
				expect(await db.get('k')).toBe('committed');
				expect(await logBytes(dbPath, 'audit')).toBe(oneEntryBytes(payload.length));
			}
		));

	it('pessimistic databases keep logging once and report hasDurableLog', () =>
		dbRunner(
			{ dbOptions: [{ path: generateDBPath(), pessimistic: true }] },
			async ({ db, dbPath }) => {
				const log = db.useLog('audit');
				const payload = Buffer.alloc(16, 'p');
				let handle: Transaction | undefined;
				await db.transaction(async (txn) => {
					handle = txn;
					expect(txn.hasDurableLog).toBe(false);
					txn.putSync('k', 'v');
					log.addEntry(payload, txn.id);
				});
				expect(handle!.hasDurableLog).toBe(true);
				expect(await logBytes(dbPath, 'audit')).toBe(oneEntryBytes(payload.length));
				expect(await db.get('k')).toBe('v');
			}
		));

	it('an unlogged transaction aborts as before', () =>
		dbRunner(async ({ db }) => {
			const txn = new Transaction(db.store);
			txn.putSync('k', 'v');
			expect(txn.hasDurableLog).toBe(false);
			txn.abort();
			expect(await db.get('k')).toBeUndefined();
			await expect(txn.commit()).rejects.toMatchObject({ code: 'ERR_ALREADY_ABORTED' });
		}));

	describe('a handle whose log batch is durable', () => {
		it('refuses abort() and abandonWrites() before touching anything, then commits once', () =>
			dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
				const { txn, log } = await loggedPending(db, 'audit', 'k');
				expect(() => txn.abort()).toThrow(TransactionLoggedError);
				expect(() => txn.abandonWrites()).toThrow(TransactionLoggedError);
				let caught: unknown;
				try {
					txn.abort();
				} catch (error) {
					caught = error;
				}
				expect(caught).toMatchObject({ code: 'ERR_TRANSACTION_LOGGED', txn });
				expect(txn.hasDurableLog).toBe(true);

				// Still pending and usable: the retry re-stages its writes (the log batch is write-once).
				txn.putSync('k', 'logged');
				log.addEntry(Buffer.from('k'), txn.id);
				await txn.commit();
				expect(txn.hasDurableLog).toBe(true);
				expect(await db.get('k')).toBe('logged');
				expect(await logBytes(dbPath, 'audit')).toBe(oneEntryBytes(1));
				expect([...log.query({ start: 0 })]).toHaveLength(1);
			}));

		it('refuses abort() after a rejected commitSync too', () =>
			dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
				const log = db.useLog('audit');
				const txn = new Transaction(db.store);
				txn.putSync('k', 'logged');
				log.addEntry(Buffer.from('k'), txn.id);
				forceTryAgainForTesting(1);
				expect(() => txn.commitSync()).toThrow(
					expect.objectContaining({ code: 'ERR_TRY_AGAIN', hasLog: true })
				);
				expect(txn.hasDurableLog).toBe(true);
				expect(() => txn.abort()).toThrow(TransactionLoggedError);
				txn.putSync('k', 'logged');
				txn.commitSync();
				expect(db.getSync('k')).toBe('logged');
				expect(await logBytes(dbPath, 'audit')).toBe(oneEntryBytes(1));
			}));

		for (const mode of ['async', 'sync'] as const) {
			it(`${mode}: db.transaction keeps retrying past maxRetries once the batch is durable`, () =>
				dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
					const log = db.useLog('audit');
					forceTryAgainForTesting(3);
					let attempts = 0;
					let handle: Transaction | undefined;
					const body = (txn: Transaction, attempt: number) => {
						handle = txn;
						attempts = attempt;
						txn.putSync('k', attempt);
						log.addEntry(Buffer.from('k'), txn.id);
					};
					if (mode === 'async') {
						await db.transaction(async (txn, attempt) => body(txn, attempt), { maxRetries: 2 });
					} else {
						db.transactionSync(body, { maxRetries: 2 });
					}
					expect(attempts).toBe(4);
					expect(handle!.hasDurableLog).toBe(true);
					expect(await db.get('k')).toBe(4);
					expect(await logBytes(dbPath, 'audit')).toBe(oneEntryBytes(1));
				}));
		}

		it('db.transaction hands a logged handle back when its retry body throws', () =>
			dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db }) => {
				const log = db.useLog('audit');
				forceTryAgainForTesting(1);
				const failure = new Error('body failed on retry');
				let caught: unknown;
				try {
					await db.transaction(async (txn, attempt) => {
						txn.putSync('k', attempt);
						log.addEntry(Buffer.from('k'), txn.id);
						if (attempt === 2) throw failure;
					});
				} catch (error) {
					caught = error;
				}
				expect(caught).toBeInstanceOf(TransactionLoggedError);
				const refused = caught as TransactionLoggedError;
				expect(refused.cause).toBe(failure);
				expect(refused.txn.hasDurableLog).toBe(true);
				refused.txn.putSync('k', 'recovered');
				await refused.txn.commit();
				expect(await db.get('k')).toBe('recovered');
				expect([...log.query({ start: 0 })]).toHaveLength(1);
			}));

		it.skipIf(!forceGC)(
			'dropped without a caller keeps the watermark and txn.state behind it and warns',
			() =>
				dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
					const { warnings, stop } = collectWarnings('durable transaction-log batch');
					try {
						const log = db.useLog('audit');
						await db.transaction(async (txn) => {
							txn.putSync('seed', 1);
							log.addEntry(Buffer.from('seed'), txn.id);
						});
						db.flushSync();
						const pinned = log.getStats().nextLogPosition;
						await (async () => {
							await loggedPending(db, 'audit', 'orphan');
						})();
						const deadline = Date.now() + 10_000;
						while (registryStatus().find((entry) => entry.path === dbPath)!.transactions > 0) {
							expect(Date.now()).toBeLessThan(deadline);
							forceGC!();
							await delay(20);
						}
						expect(warnings).toHaveLength(1);
						expect(await db.get('orphan')).toBeUndefined();

						// A later commit publishes nothing past the orphan's position ...
						await db.transaction(async (txn) => {
							txn.putSync('later', 1);
							log.addEntry(Buffer.from('later'), txn.id);
						});
						const stats = log.getStats();
						expect(stats.lastCommittedPosition).toEqual(pinned);
						expect([...log.query({ start: 0 })].map((entry) => entry.data.toString())).toEqual([
							'seed',
						]);
						expect([...log.query({ start: 0, readUncommitted: true })]).toHaveLength(3);

						// ... and a flush cannot move the replay boundary past it.
						db.flushSync();
						expect(positionAtMost(log.getStats().lastFlushedPosition, pinned)).toBe(true);
					} finally {
						stop();
					}
				})
		);

		it('left pending by an exiting worker keeps the watermark behind it and warns', () =>
			dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
				const { warnings, stop } = collectWarnings('durable transaction-log batch');
				try {
					const log = db.useLog('audit');
					await db.transaction(async (txn) => {
						txn.putSync('seed', 1);
						log.addEntry(Buffer.from('seed'), txn.id);
					});
					const pinned = log.getStats().nextLogPosition;
					const worker = new Worker(
						createWorkerBootstrapScript(
							resolve(__dirname, 'workers/logged-pending-exit-worker.mts')
						),
						{ eval: true, workerData: { path: dbPath, log: 'audit', key: 'orphan' } }
					);
					const message = await new Promise<{
						code?: string;
						hasDurableLog?: boolean;
						error?: string;
					}>((resolveMessage, reject) => {
						worker.once('message', resolveMessage);
						worker.once('error', reject);
					});
					expect(message).toEqual({ code: 'ERR_TRY_AGAIN', hasDurableLog: true });
					await new Promise<void>((resolveExit) => worker.once('exit', () => resolveExit()));
					await vi.waitFor(() => expect(warnings).toHaveLength(1));
					expect(registryStatus().find((entry) => entry.path === dbPath)!.transactions).toBe(0);

					await db.transaction(async (txn) => {
						txn.putSync('later', 1);
						log.addEntry(Buffer.from('later'), txn.id);
					});
					expect(log.getStats().lastCommittedPosition).toEqual(pinned);
					expect([...log.query({ start: 0 })].map((entry) => entry.data.toString())).toEqual([
						'seed',
					]);
					db.flushSync();
					expect(positionAtMost(log.getStats().lastFlushedPosition, pinned)).toBe(true);
				} finally {
					stop();
				}
			}));
	});

	describe('competing writers inside the validated window', () => {
		it('wait for the logged commit, which lands without a conflict after its log write', () =>
			dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
				const log = db.useLog('audit');
				await db.put('k', 'initial');
				const txn = new Transaction(db.store);
				expect(await txn.get('k')).toBe('initial');
				txn.putSync('k', 'logged');
				log.addEntry(Buffer.from('k'), txn.id);

				setCommitValidatedDelayForTesting(1, 1500);
				const committing = txn.commit();
				await vi.waitFor(() => expect(isCommitValidatedDelayedForTesting()).toBe(true));
				const windowStart = Date.now();

				const worker = new Worker(
					createWorkerBootstrapScript(resolve(__dirname, 'workers/conflict-window-worker.mts')),
					{ eval: true, workerData: { path: dbPath, key: 'k', value: 'direct', delayMs: 0 } }
				);
				const direct = new Promise<{ started: number; done: number; error?: string }>(
					(resolveMessage, reject) => {
						worker.once('message', resolveMessage);
						worker.once('error', reject);
					}
				);
				let competitorDone = 0;
				// A blind write tracked before the logged commit landed conflicts with it and is
				// refused before its own (absent) log write; retryOnBusy lets it land afterwards.
				const competitor = db
					.transaction(
						async (other) => {
							other.putSync('k', 'competitor');
						},
						{ retryOnBusy: true, maxRetries: 5 }
					)
					.then(() => {
						competitorDone = Date.now();
					});

				await expect(committing).resolves.toBeUndefined();
				const loggedDone = Date.now();
				expect(txn.hasDurableLog).toBe(true);

				const directResult = await direct;
				await competitor;
				// Both competitors started inside the window and could only finish once the logged
				// commit released its reservation (a free putSync returns in well under a millisecond).
				expect(directResult.error).toBeUndefined();
				expect(directResult.started).toBeLessThan(loggedDone);
				expect(directResult.done - windowStart).toBeGreaterThanOrEqual(1400);
				expect(competitorDone - windowStart).toBeGreaterThanOrEqual(1400);
				expect(['direct', 'competitor']).toContain(await db.get('k'));
				expect(await logBytes(dbPath, 'audit')).toBe(oneEntryBytes(1));
				expect([...log.query({ start: 0 })]).toHaveLength(1);
				await new Promise<void>((resolveExit) => worker.once('exit', () => resolveExit()));
			}));

		it('a direct write from another thread waits out a synchronous logged commit', () =>
			dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
				const log = db.useLog('audit');
				const txn = new Transaction(db.store);
				txn.putSync('k', 'logged');
				log.addEntry(Buffer.from('k'), txn.id);

				const worker = new Worker(
					createWorkerBootstrapScript(resolve(__dirname, 'workers/conflict-window-worker.mts')),
					{ eval: true, workerData: { path: dbPath, key: 'k', value: 'direct', delayMs: 300 } }
				);
				const direct = new Promise<{ started: number; done: number; error?: string }>(
					(resolveMessage, reject) => {
						worker.once('message', resolveMessage);
						worker.once('error', reject);
					}
				);
				// Give the worker time to open the database before the JS thread blocks.
				await delay(100);
				setCommitValidatedDelayForTesting(1, 1500);
				txn.commitSync();
				const loggedDone = Date.now();
				expect(txn.hasDurableLog).toBe(true);

				const directResult = await direct;
				expect(directResult.error).toBeUndefined();
				expect(directResult.started).toBeLessThan(loggedDone);
				expect(directResult.done - directResult.started).toBeGreaterThanOrEqual(900);
				expect(db.getSync('k')).toBe('direct');
				expect(await logBytes(dbPath, 'audit')).toBe(oneEntryBytes(1));
				await new Promise<void>((resolveExit) => worker.once('exit', () => resolveExit()));
			}));
	});
});
