// The commit protocol of `src/binding/transaction/DESIGN.md` (HarperFast/harper#3253):
// an optimistic conflict is detected before the transaction-log batch is written, so an
// ordinary conflict retry leaves no durable entry; `hasDurableLog` reports the authoritative
// native state; a handle whose batch is durable can neither be aborted nor silently dropped.

import { RocksDatabase, registryStatus, type TransactionLog } from '../src/index.ts';
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
import { join } from 'node:path';
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

/**
 * Physical size of a store's segments, valid only before any `query()` maps the active segment
 * (Windows pre-extends a mapped segment to its capacity).
 */
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

function expectLoggedOnce(log: TransactionLog, entries = 1, bytes?: number) {
	const totals = log.getStats().totals;
	expect(totals.transactionsWritten).toBe(1);
	expect(totals.entriesWritten).toBe(entries);
	if (bytes !== undefined) expect(totals.bytesWritten).toBe(bytes);
}

type Position = { sequence: number; offset: number };
const positionAtMost = (a: Position, b: Position) =>
	a.sequence < b.sequence || (a.sequence === b.sequence && a.offset <= b.offset);

function withinDeadline<T>(
	promise: Promise<T>,
	worker: Worker,
	label: string,
	ms = 20_000
): Promise<T> {
	let timer: NodeJS.Timeout;
	return Promise.race([
		promise,
		new Promise<T>((_, reject) => {
			timer = setTimeout(() => {
				void worker.terminate();
				reject(new Error(`${label} did not settle within ${ms}ms`));
			}, ms);
		}),
	]).finally(() => clearTimeout(timer));
}

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
	// The rejection is read into plain values so no error object outlives this helper.
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
				expectLoggedOnce(log, 1);
				expect([...log.query({ start: 0 })]).toHaveLength(1);
			}));
	}

	it('carries a log batch staged only on the first attempt across a pre-log refusal', () =>
		dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db }) => {
			const log = db.useLog('audit');
			await db.put('k', 'initial');
			let attempts = 0;
			await db.transaction(
				async (txn, attempt) => {
					attempts = attempt;
					await txn.get('k');
					if (attempt === 1) await db.put('k', 'conflict');
					txn.putSync('k', 'committed');
					// The old write-then-check contract: stage the entry once.
					if (attempt === 1) log.addEntry(Buffer.from('once'), txn.id);
				},
				{ retryOnBusy: true, maxRetries: 5 }
			);
			expect(attempts).toBe(2);
			expect(await db.get('k')).toBe('committed');
			expect([...log.query({ start: 0 })].map((entry) => entry.data.toString())).toEqual(['once']);
			expectLoggedOnce(log, 1);
		}));

	it('drops a carried log batch when the retry decides to write nothing', () =>
		dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db }) => {
			const log = db.useLog('audit');
			await db.put('k', 'initial');
			let attempts = 0;
			await db.transaction(
				async (txn, attempt) => {
					attempts = attempt;
					const current = await txn.get('k');
					if (attempt === 1) {
						await db.put('k', 'conflict');
						txn.putSync('k', 'mine');
						log.addEntry(Buffer.from('mine'), txn.id);
					} else {
						expect(current).toBe('conflict');
					}
				},
				{ retryOnBusy: true, maxRetries: 5 }
			);
			expect(attempts).toBe(2);
			expect(await db.get('k')).toBe('conflict');
			expect([...log.query({ start: 0, readUncommitted: true })]).toHaveLength(0);
			expect(log.getStats().totals.transactionsWritten).toBe(0);
		}));

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
			dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db }) => {
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

				expect(() => txn.putSync('k', 'other')).toThrow(TransactionLoggedError);
				expect(() => txn.removeSync('k')).toThrow(TransactionLoggedError);
				await expect(txn.put('k', 'other')).rejects.toBeInstanceOf(TransactionLoggedError);
				let routed: unknown;
				try {
					db.putSync('k', 'other', { transaction: txn });
				} catch (error) {
					routed = error;
				}
				expect(routed).toBeInstanceOf(TransactionLoggedError);
				expect((routed as TransactionLoggedError).txn).toBe(txn);
				log.addEntry(Buffer.from('k'), txn.id);
				await txn.commit();
				expect(txn.hasDurableLog).toBe(true);
				expect(await db.get('k')).toBe('logged');
				expectLoggedOnce(log, 1);
				expect([...log.query({ start: 0 })]).toHaveLength(1);
			}));

		it('refuses abort() after a rejected commitSync too', () =>
			dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db }) => {
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
				txn.commitSync();
				expect(db.getSync('k')).toBe('logged');
				expectLoggedOnce(log, 1);
			}));

		for (const mode of ['async', 'sync'] as const) {
			it(`${mode}: db.transaction re-commits a durable batch past maxRetries without re-running the callback`, () =>
				dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
					const log = db.useLog('audit');
					forceTryAgainForTesting(3);
					let calls = 0;
					let handle: Transaction | undefined;
					const body = (txn: Transaction, attempt: number) => {
						handle = txn;
						calls++;
						txn.putSync('k', attempt);
						log.addEntry(Buffer.from('k'), txn.id);
					};
					if (mode === 'async') {
						await db.transaction(async (txn, attempt) => body(txn, attempt), {
							maxRetries: 2,
							retryOnBusy: false,
						});
					} else {
						db.transactionSync(body, { maxRetries: 2, retryOnBusy: false });
					}
					expect(calls).toBe(1);
					expect(handle!.hasDurableLog).toBe(true);
					expect(await db.get('k')).toBe(1);
					expect(await logBytes(dbPath, 'audit')).toBe(oneEntryBytes(1));
				}));
		}

		for (const mode of ['async', 'sync', 'sync-promise'] as const) {
			it(`${mode}: keeps the callback's value across a durable re-commit`, () =>
				dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db }) => {
					const log = db.useLog('audit');
					forceTryAgainForTesting(1);
					const body = (txn: Transaction) => {
						txn.putSync('k', 'v');
						log.addEntry(Buffer.from('k'), txn.id);
						return 42;
					};
					let value: unknown;
					if (mode === 'async') {
						value = await db.transaction(async (txn) => body(txn));
					} else if (mode === 'sync') {
						value = db.transactionSync(body);
					} else {
						value = await db.transactionSync(async (txn) => body(txn));
					}
					expect(value).toBe(42);
					expect(await db.get('k')).toBe('v');
				}));
		}

		it('keeps its keys reserved: other writers are refused with ERR_BUSY until it lands', () =>
			dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db }) => {
				const { txn, log } = await loggedPending(db, 'audit', 'k');
				// A direct write and an unrelated transaction's commit on the held key are refused
				// at once, not blocked; a free key is unaffected.
				expect(() => db.putSync('k', 'direct')).toThrow(/Resource busy|ERR_BUSY/);
				db.putSync('other', 'free');
				await expect(
					db.transaction(async (other) => {
						other.putSync('k', 'competitor');
					})
				).rejects.toMatchObject({ code: 'ERR_BUSY' });
				expect(await db.get('k')).toBeUndefined();

				await txn.commit();
				expect(await db.get('k')).toBe('logged');
				db.putSync('k', 'direct');
				expect(db.getSync('k')).toBe('direct');
				expect([...log.query({ start: 0 })]).toHaveLength(1);
				expectLoggedOnce(log, 1);
			}));

		it('releases its keys when it is closed without committing', () =>
			dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
				const { warnings, stop } = collectWarnings('durable transaction-log batch');
				try {
					const worker = new Worker(
						createWorkerBootstrapScript('./test/workers/logged-pending-exit-worker.mts'),
						{
							eval: true,
							workerData: { path: dbPath, log: 'audit', key: 'k', holdUntilMessage: true },
						}
					);
					const exited = new Promise<void>((resolveExit) =>
						worker.once('exit', () => resolveExit())
					);
					const message = await withinDeadline(
						new Promise<unknown>((resolveMessage, reject) => {
							worker.once('message', resolveMessage);
							worker.once('error', reject);
							worker.once('exit', () => reject(new Error('worker exited without a result')));
						}),
						worker,
						'logged-pending worker'
					);
					expect(message).toEqual({ code: 'ERR_TRY_AGAIN', hasDurableLog: true });
					expect(() => db.putSync('k', 'direct')).toThrow(/Resource busy|ERR_BUSY/);
					worker.postMessage('release');
					await withinDeadline(exited, worker, 'logged-pending worker exit');
					await vi.waitFor(() => expect(warnings).toHaveLength(1));
					db.putSync('k', 'direct');
					expect(db.getSync('k')).toBe('direct');
				} finally {
					stop();
				}
			}));

		it('may be aborted once its column family is dropped, and releases its log position', () =>
			dbRunner(
				{ dbOptions: [{ name: 'table' }, { name: 'table' }] },
				async ({ db, dbPath }, { db: dropper }) => {
					const { warnings, stop } = collectWarnings('dropped column family');
					try {
						const { txn, log } = await loggedPending(db, 'audit', 'k');
						dropper.dropSync();
						await expect(txn.commit()).rejects.toMatchObject({ code: 'ERR_COLUMN_FAMILY_DROPPED' });
						expect(() => txn.abort()).not.toThrow();
						await vi.waitFor(() => expect(warnings).toHaveLength(1));
						// Nothing pins the store: a later logged transaction publishes past the entry.
						const other = RocksDatabase.open(dbPath, { name: 'other' });
						try {
							await other.transaction(async (t) => {
								t.putSync('later', 1);
								log.addEntry(Buffer.from('later'), t.id);
							});
							expect([...log.query({ start: 0 })].map((entry) => entry.data.toString())).toEqual([
								'k',
								'later',
							]);
						} finally {
							other.close();
						}
					} finally {
						stop();
					}
				}
			));

		it('keeps the obligation when only some of its families were dropped', () =>
			dbRunner(
				{ dbOptions: [{ name: 'table' }, { name: 'table' }, { name: 'other' }] },
				async ({ db }, { db: dropper }, { db: other }) => {
					const log = db.useLog('audit');
					const txn = new Transaction(db.store);
					txn.putSync('k', 'in-table');
					other.putSync('j', 'in-other', { transaction: txn });
					log.addEntry(Buffer.from('kj'), txn.id);
					forceTryAgainForTesting(1);
					await expect(txn.commit()).rejects.toMatchObject({ code: 'ERR_TRY_AGAIN' });
					dropper.dropSync();
					await expect(txn.commit()).rejects.toMatchObject({ code: 'ERR_COLUMN_FAMILY_DROPPED' });
					// The write to the surviving family never landed, so the entry stays an obligation.
					expect(() => txn.abort()).toThrow(TransactionLoggedError);
					expect([...log.query({ start: 0 })]).toHaveLength(0);
				}
			));

		it('commits the write set its log batch describes even when nothing is staged again', () =>
			dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db }) => {
				const log = db.useLog('audit');
				const txn = new Transaction(db.store);
				txn.putSync('a', 1);
				txn.putSync('b', 2);
				log.addEntry(Buffer.from('ab'), txn.id);
				forceTryAgainForTesting(1);
				await expect(txn.commit()).rejects.toMatchObject({ code: 'ERR_TRY_AGAIN' });
				expect(txn.hasDurableLog).toBe(true);
				expect(await txn.get('a')).toBe(1);
				// The write set is what the log describes: nothing can change it, and all of it lands.
				expect(() => txn.putSync('b', 3)).toThrow(
					expect.objectContaining({ code: 'ERR_TRANSACTION_LOGGED' })
				);
				await txn.commit();
				expect(await db.get('a')).toBe(1);
				expect(await db.get('b')).toBe(2);
				expect([...log.query({ start: 0 })]).toHaveLength(1);
				expectLoggedOnce(log, 1, TRANSACTION_LOG_ENTRY_HEADER_SIZE + 2);
			}));

		it('transactionSync aborts the handle and rethrows when an async callback rejects', () =>
			dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
				const failure = new Error('callback rejected');
				let handle: Transaction | undefined;
				await expect(
					db.transactionSync(async (txn) => {
						handle = txn;
						txn.putSync('k', 'v');
						throw failure;
					})
				).rejects.toBe(failure);
				expect(handle!.hasDurableLog).toBe(false);
				await expect(handle!.commit()).rejects.toMatchObject({ code: 'ERR_ALREADY_ABORTED' });
				expect(registryStatus().find((entry) => entry.path === dbPath)!.transactions).toBe(0);
				expect(await db.get('k')).toBeUndefined();
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
						createWorkerBootstrapScript('./test/workers/logged-pending-exit-worker.mts'),
						{ eval: true, workerData: { path: dbPath, log: 'audit', key: 'orphan' } }
					);
					const exited = new Promise<void>((resolveExit) =>
						worker.once('exit', () => resolveExit())
					);
					const message = await withinDeadline(
						new Promise<{ code?: string; hasDurableLog?: boolean; error?: string }>(
							(resolveMessage, reject) => {
								worker.once('message', resolveMessage);
								worker.once('error', reject);
								worker.once('exit', () => reject(new Error('worker exited without a result')));
							}
						),
						worker,
						'logged-pending worker'
					);
					expect(message).toEqual({ code: 'ERR_TRY_AGAIN', hasDurableLog: true });
					await withinDeadline(exited, worker, 'logged-pending worker exit');
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
					createWorkerBootstrapScript('./test/workers/conflict-window-worker.mts'),
					{ eval: true, workerData: { path: dbPath, key: 'k', value: 'direct', delayMs: 0 } }
				);
				const exited = new Promise<void>((resolveExit) => worker.once('exit', () => resolveExit()));
				const direct = new Promise<{ started: number; done: number; error?: string }>(
					(resolveMessage, reject) => {
						worker.once('message', resolveMessage);
						worker.once('error', reject);
						worker.once('exit', () => reject(new Error('worker exited without a result')));
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

				const directResult = await withinDeadline(direct, worker, 'direct write');
				await competitor;
				// Both competitors started inside the window and could only finish once the logged
				// commit released its reservation (a free putSync returns in well under a millisecond).
				expect(directResult.error).toBeUndefined();
				expect(directResult.started).toBeLessThan(loggedDone);
				expect(directResult.done - windowStart).toBeGreaterThanOrEqual(1400);
				expect(competitorDone - windowStart).toBeGreaterThanOrEqual(1400);
				expect(['direct', 'competitor']).toContain(await db.get('k'));
				expectLoggedOnce(log, 1);
				expect([...log.query({ start: 0 })]).toHaveLength(1);
				await withinDeadline(exited, worker, 'worker exit');
			}));

		it('a direct write from another thread waits out a synchronous logged commit', () =>
			dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
				const log = db.useLog('audit');
				const txn = new Transaction(db.store);
				txn.putSync('k', 'logged');
				log.addEntry(Buffer.from('k'), txn.id);

				const worker = new Worker(
					createWorkerBootstrapScript('./test/workers/conflict-window-worker.mts'),
					{ eval: true, workerData: { path: dbPath, key: 'k', value: 'direct', delayMs: 300 } }
				);
				const exited = new Promise<void>((resolveExit) => worker.once('exit', () => resolveExit()));
				const direct = new Promise<{ started: number; done: number; error?: string }>(
					(resolveMessage, reject) => {
						worker.once('message', resolveMessage);
						worker.once('error', reject);
						worker.once('exit', () => reject(new Error('worker exited without a result')));
					}
				);
				// Give the worker time to open the database before the JS thread blocks.
				await delay(100);
				setCommitValidatedDelayForTesting(1, 1500);
				txn.commitSync();
				const loggedDone = Date.now();
				expect(txn.hasDurableLog).toBe(true);

				const directResult = await withinDeadline(direct, worker, 'direct write');
				expect(directResult.error).toBeUndefined();
				expect(directResult.started).toBeLessThan(loggedDone);
				expect(directResult.done - directResult.started).toBeGreaterThanOrEqual(900);
				expect(db.getSync('k')).toBe('direct');
				expect(await logBytes(dbPath, 'audit')).toBe(oneEntryBytes(1));
				await withinDeadline(exited, worker, 'worker exit');
			}));
	});
});
