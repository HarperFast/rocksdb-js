import { RocksDatabase } from '../src/index.ts';
import {
	forceDropFailureForTesting,
	forceTryAgainForTesting,
	getCommitGateCountersForTesting,
	setCommitHoldForTesting,
} from '../src/load-binding.ts';
import { type Transaction, TransactionAbandonedError } from '../src/transaction.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { createWorkerBootstrapScript } from './lib/worker-bootstrap.ts';
import { spawn } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';
import { afterEach, describe, expect, it } from 'vitest';

/**
 * Column-family commit gate (#806, #726; AGENTS.md invariant 20): the
 * observable contract on both sides of a commit/drop race, in both modes.
 */

const modes = [
	{ mode: 'optimistic', pessimistic: false },
	{ mode: 'pessimistic', pessimistic: true },
];

type CodedError = Error & { code?: string; hasLog?: boolean; cause?: unknown };

const DROPPED_MESSAGE = /column family (".*"|\d+) (is being|was) dropped/;
// The contract Harper's cross-worker reproducer matches on (harper#2456).
const HARPER_CONTRACT = /column family .*dropp/;

function expectDroppedError(err: unknown, family?: string): CodedError {
	expect(err).toBeInstanceOf(Error);
	const coded = err as CodedError;
	expect(coded.code).toBe('ERR_COLUMN_FAMILY_DROPPED');
	expect(coded.message).toMatch(DROPPED_MESSAGE);
	expect(coded.message).toMatch(HARPER_CONTRACT);
	if (family) {
		expect(coded.message).toContain(`"${family}"`);
	}
	return coded;
}

async function rejectsWithDropped(promise: Promise<unknown>, family?: string): Promise<CodedError> {
	try {
		await promise;
	} catch (err) {
		return expectDroppedError(err, family);
	}
	throw new Error('expected the commit to be refused');
}

function throwsWithDropped(fn: () => unknown, family?: string): CodedError {
	try {
		fn();
	} catch (err) {
		return expectDroppedError(err, family);
	}
	throw new Error('expected the commit to be refused');
}

/**
 * The database must be fully writable after the race: an unrelated family takes
 * sync and transactional writes, no background error was latched, and the
 * dropped name can be recreated as a fresh, empty, writable family.
 */
async function expectHealthy(
	victim: RocksDatabase,
	dbPath: string,
	pessimistic: boolean
): Promise<void> {
	victim.putSync('probe', 'sync');
	expect(victim.getSync('probe')).toBe('sync');
	await victim.transaction(async (txn: Transaction) => {
		await victim.put('probe-txn', 'txn', { transaction: txn });
	});
	expect(victim.getSync('probe-txn')).toBe('txn');
	expect(victim.getLastError()).toBeNull();
	expect(victim.columns).not.toContain('doomed');
	const fresh = RocksDatabase.open(dbPath, { name: 'doomed', pessimistic });
	try {
		expect(fresh.getSync('a')).toBeUndefined();
		expect(fresh.getSync('dead')).toBeUndefined();
		fresh.putSync('fresh', 'ok');
		expect(fresh.getSync('fresh')).toBe('ok');
	} finally {
		fresh.close();
	}
}

async function waitForCounter(field: 'commitsAdmitted' | 'dropsBegun', min: number): Promise<void> {
	const deadline = Date.now() + 20_000;
	while (getCommitGateCountersForTesting()[field] < min) {
		if (Date.now() > deadline) {
			throw new Error(`${field} never reached ${min}`);
		}
		await delay(1);
	}
}

type GateWorker = {
	worker: Worker;
	send: (msg: Record<string, unknown>) => void;
	waitFor: (type: string, id?: number) => Promise<Record<string, any>>;
	stop: () => Promise<void>;
};

async function startWorker(path: string, pessimistic: boolean): Promise<GateWorker> {
	const worker = new Worker(
		createWorkerBootstrapScript('./test/workers/drop-commit-gate-worker.mts'),
		{
			eval: true,
			workerData: { path, pessimistic },
		}
	);
	const queue: Record<string, any>[] = [];
	const waiters: {
		match: (m: Record<string, any>) => boolean;
		resolve: (m: Record<string, any>) => void;
	}[] = [];
	worker.on('message', (msg: Record<string, any>) => {
		const index = waiters.findIndex((w) => w.match(msg));
		if (index === -1) {
			queue.push(msg);
		} else {
			waiters.splice(index, 1)[0].resolve(msg);
		}
	});
	const waitFor = (type: string, id?: number) => {
		const match = (m: Record<string, any>) => m.type === type && (id === undefined || m.id === id);
		const queued = queue.findIndex(match);
		if (queued !== -1) {
			return Promise.resolve(queue.splice(queued, 1)[0]);
		}
		return new Promise<Record<string, any>>((resolve) => waiters.push({ match, resolve }));
	};
	const errored = new Promise<never>((_, reject) => worker.once('error', reject));
	await Promise.race([waitFor('ready'), errored]);
	return {
		worker,
		send: (msg) => worker.postMessage(msg),
		waitFor: (type, id) => Promise.race([waitFor(type, id), errored]),
		stop: async () => {
			worker.postMessage({ type: 'close', id: -1 });
			await Promise.race([waitFor('closed', -1), delay(5000)]);
			await worker.terminate();
		},
	};
}

describe.each(modes)('drop before commit ($mode)', ({ pessimistic }) => {
	const dbOptions = [
		{ name: 'victim', pessimistic },
		{ name: 'doomed', pessimistic },
		{ name: 'doomed', pessimistic },
	];

	it('refuses an async commit staged before dropSync() with a contained error', () =>
		dbRunner({ dbOptions }, async ({ db: victim, dbPath }, { db: doomed }, { db: stale }) => {
			await rejectsWithDropped(
				stale.transaction(async (txn: Transaction) => {
					await stale.put('a', '1', { transaction: txn });
					doomed.dropSync();
				}),
				'doomed'
			);
			await expectHealthy(victim, dbPath, pessimistic);
		}));

	it('refuses a sync commit staged before dropSync() with a contained error', () =>
		dbRunner({ dbOptions }, async ({ db: victim, dbPath }, { db: doomed }, { db: stale }) => {
			throwsWithDropped(
				() =>
					stale.transactionSync((txn: Transaction) => {
						stale.putSync('a', '1', { transaction: txn });
						doomed.dropSync();
					}),
				'doomed'
			);
			await expectHealthy(victim, dbPath, pessimistic);
		}));

	it('refuses a commit staged before an async drop() with a contained error', () =>
		dbRunner({ dbOptions }, async ({ db: victim, dbPath }, { db: doomed }, { db: stale }) => {
			await rejectsWithDropped(
				stale.transaction(async (txn: Transaction) => {
					await stale.remove('a', { transaction: txn });
					await doomed.drop();
				}),
				'doomed'
			);
			await expectHealthy(victim, dbPath, pessimistic);
		}));

	it('refuses a batch spanning a live and a dropped family whole', () =>
		dbRunner({ dbOptions }, async ({ db: victim, dbPath }, { db: doomed }, { db: stale }) => {
			await rejectsWithDropped(
				stale.transaction(async (txn: Transaction) => {
					await victim.put('live', 'A', { transaction: txn });
					await stale.put('dead', 'B', { transaction: txn });
					doomed.dropSync();
				}),
				'doomed'
			);
			expect(victim.getSync('live')).toBeUndefined();
			await expectHealthy(victim, dbPath, pessimistic);
		}));

	it('refuses a commit through a stale handle long after the drop', () =>
		dbRunner({ dbOptions }, async ({ db: victim, dbPath }, { db: doomed }, { db: stale }) => {
			doomed.dropSync();
			const staged = stale.transaction(async (txn: Transaction) => {
				await stale.put('a', '1', { transaction: txn });
			});
			if (pessimistic) {
				// A pessimistic write locks its key at staging time, and RocksDB's lock
				// manager already forgot the dropped family — the write itself is refused
				// before there is anything to commit. Contained either way.
				await expect(staged).rejects.toThrow(/Column family id not found/);
			} else {
				const error = await rejectsWithDropped(staged, 'doomed');
				expect(error.message).toMatch(/was dropped/);
			}
			await expectHealthy(victim, dbPath, pessimistic);
		}));

	it('admits by the families actually written, not the handle the transaction was created on', () =>
		dbRunner(
			{
				dbOptions: [
					{ name: 'home', pessimistic },
					{ name: 'other', pessimistic },
				],
			},
			async ({ db: home }, { db: other }) => {
				await home.transaction(async (txn: Transaction) => {
					await other.put('k', 'v', { transaction: txn });
					home.dropSync();
				});
				expect(other.getSync('k')).toBe('v');
				expect(other.columns).not.toContain('home');
				await home.transaction(async () => {});
			}
		));

	it('does not wait on a transaction that is staged but idle on the dropping thread', () =>
		dbRunner({ dbOptions }, async ({ db: victim, dbPath }, { db: doomed }, { db: stale }) => {
			// Harper's dropSync() runs inside a synchronous exclusive schema section
			// while transactions staged on that thread are still open.
			const staged = new Promise<Transaction>((resolve) => {
				void stale
					.transaction(async (txn: Transaction) => {
						await stale.put('a', '1', { transaction: txn });
						resolve(txn);
						await delay(50);
					})
					.catch(() => {});
			});
			const txn = await staged;
			doomed.dropSync();
			expect(victim.columns).not.toContain('doomed');
			await rejectsWithDropped(txn.commit(), 'doomed');
			await expectHealthy(victim, dbPath, pessimistic);
		}));

	it('tracks a transaction spanning more families than the inline set', async () => {
		const dbPath = generateDBPath();
		const families = Array.from({ length: 10 }, (_, i) =>
			RocksDatabase.open(dbPath, { name: `f${i}`, pessimistic })
		);
		const doomed = RocksDatabase.open(dbPath, { name: 'doomed', pessimistic });
		try {
			await families[0].transaction(async (txn: Transaction) => {
				for (const [i, family] of families.entries()) {
					await family.put(`k${i}`, i, { transaction: txn });
				}
			});
			for (const [i, family] of families.entries()) {
				expect(family.getSync(`k${i}`)).toBe(i);
			}
			await rejectsWithDropped(
				families[0].transaction(async (txn: Transaction) => {
					for (const [i, family] of families.entries()) {
						await family.put(`m${i}`, i, { transaction: txn });
					}
					await doomed.put('dead', 1, { transaction: txn });
					doomed.dropSync();
				})
			);
			for (const [i, family] of families.entries()) {
				expect(family.getSync(`m${i}`)).toBeUndefined();
			}
			const fresh = RocksDatabase.open(dbPath, { name: 'doomed', pessimistic });
			try {
				expect(fresh.getSync('dead')).toBeUndefined();
				fresh.putSync('fresh', 'ok');
				expect(fresh.getSync('fresh')).toBe('ok');
			} finally {
				fresh.close();
			}
			expect(families[0].getLastError()).toBeNull();
		} finally {
			doomed.close();
			for (const family of families) {
				family.close();
			}
			rmSync(dbPath, { recursive: true, force: true });
		}
	});
});

describe.each(modes)('commit admitted before the drop begins ($mode)', ({ pessimistic }) => {
	afterEach(() => setCommitHoldForTesting(false));

	it('the drop waits for the admitted commit, refuses a later one, and leaves unrelated writes alone', async () => {
		const dbPath = generateDBPath();
		const victim = RocksDatabase.open(dbPath, { name: 'victim', pessimistic });
		const doomed = RocksDatabase.open(dbPath, { name: 'doomed', pessimistic });
		const committer = await startWorker(dbPath, pessimistic);
		const late = await startWorker(dbPath, pessimistic);
		try {
			const start = getCommitGateCountersForTesting();
			setCommitHoldForTesting(true);
			committer.send({ type: 'commit', id: 1, family: 'doomed', key: 'won' });
			// the commit is admitted (observed, not assumed) and parked inside its window
			await waitForCounter('commitsAdmitted', start.commitsAdmitted + 1);

			// once the drop below has closed the gate, `late` writes an unrelated
			// family, tries a sync commit on the dropping one, then releases the hold
			late.send({
				type: 'late-commit-when-drop-begins',
				id: 2,
				family: 'doomed',
				minDropsBegun: start.dropsBegun + 1,
				unrelatedFamily: 'victim',
				releaseHold: true,
			});

			// blocks this thread until the held commit has landed
			doomed.dropSync();

			const settled = await committer.waitFor('commit-settled', 1);
			expect(settled.error).toBeUndefined();

			const lateDone = await late.waitFor('late-commit-done', 2);
			const lateError = expectDroppedError(
				Object.assign(new Error(lateDone.error?.message), lateDone.error),
				'doomed'
			);
			expect(lateError.message).toMatch(/is being dropped/);
			// refused before admission, while the earlier commit was still held
			expect(lateDone.after.commitsAdmitted).toBe(lateDone.before.commitsAdmitted);
			expect(lateDone.during.unrelatedPut).toBe('ok');

			expect(getCommitGateCountersForTesting().dropsBegun).toBe(start.dropsBegun + 1);
			expect(victim.getSync('during')).toBe('ok');
			await expectHealthy(victim, dbPath, pessimistic);
		} finally {
			setCommitHoldForTesting(false);
			await committer.stop();
			await late.stop();
			doomed.close();
			victim.close();
			rmSync(dbPath, { recursive: true, force: true });
		}
	}, 60_000);

	it('a worker terminated with its commit admitted neither wedges the drop nor poisons the database', async () => {
		const dbPath = generateDBPath();
		const victim = RocksDatabase.open(dbPath, { name: 'victim', pessimistic });
		const doomed = RocksDatabase.open(dbPath, { name: 'doomed', pessimistic });
		const committer = await startWorker(dbPath, pessimistic);
		const releaser = await startWorker(dbPath, pessimistic);
		try {
			const start = getCommitGateCountersForTesting();
			setCommitHoldForTesting(true);
			committer.send({ type: 'commit', id: 1, family: 'doomed', key: 'orphan' });
			await waitForCounter('commitsAdmitted', start.commitsAdmitted + 1);
			// the committing env dies while its commit is parked inside the gate
			await committer.worker.terminate();

			releaser.send({
				type: 'late-commit-when-drop-begins',
				id: 2,
				family: 'doomed',
				minDropsBegun: start.dropsBegun + 1,
				releaseHold: true,
			});
			// waits for the orphaned commit to land, then drops
			doomed.dropSync();

			const released = await releaser.waitFor('late-commit-done', 2);
			expectDroppedError(
				Object.assign(new Error(released.error?.message), released.error),
				'doomed'
			);
			await expectHealthy(victim, dbPath, pessimistic);
		} finally {
			setCommitHoldForTesting(false);
			await releaser.stop();
			doomed.close();
			victim.close();
			rmSync(dbPath, { recursive: true, force: true });
		}
	}, 60_000);

	it('lets two handles drop the same family concurrently from different threads', async () => {
		const dbPath = generateDBPath();
		const victim = RocksDatabase.open(dbPath, { name: 'victim', pessimistic });
		const doomed = RocksDatabase.open(dbPath, { name: 'doomed', pessimistic });
		const a = await startWorker(dbPath, pessimistic);
		const b = await startWorker(dbPath, pessimistic);
		try {
			doomed.putSync('k', 'v');
			// both handles must hold the same generation before either drops
			a.send({ type: 'open', id: 0, family: 'doomed' });
			b.send({ type: 'open', id: 0, family: 'doomed' });
			await Promise.all([a.waitFor('opened', 0), b.waitFor('opened', 0)]);
			a.send({ type: 'drop', id: 1, family: 'doomed' });
			b.send({ type: 'drop', id: 2, family: 'doomed' });
			const [droppedA, droppedB] = await Promise.all([
				a.waitFor('dropped', 1),
				b.waitFor('dropped', 2),
			]);
			expect(droppedA.error).toBeUndefined();
			expect(droppedB.error).toBeUndefined();
			await expectHealthy(victim, dbPath, pessimistic);
		} finally {
			await a.stop();
			await b.stop();
			doomed.close();
			victim.close();
			rmSync(dbPath, { recursive: true, force: true });
		}
	}, 60_000);
});

describe('release paths', () => {
	afterEach(() => forceDropFailureForTesting(0));

	it('does not wait on aborted transactions or closed handles', () =>
		dbRunner(
			{ dbOptions: [{ name: 'victim' }, { name: 'doomed' }, { name: 'doomed' }] },
			async ({ db: victim, dbPath }, { db: doomed }, { db: stale }) => {
				// an aborted transaction released whatever it staged
				stale.transactionSync((txn: Transaction) => {
					stale.putSync('a', '1', { transaction: txn });
					txn.abort();
				});
				// a refused commit, then abandoned, released as well
				await rejectsWithDropped(
					stale.transaction(async (txn: Transaction) => {
						await stale.put('b', '2', { transaction: txn });
						doomed.dropSync();
					}),
					'doomed'
				);
				// the handle whose transactions were refused can close and the drop of a
				// second family (from another handle) returns immediately afterwards
				stale.close();
				const second = RocksDatabase.open(dbPath, { name: 'second' });
				const other = RocksDatabase.open(dbPath, { name: 'second' });
				try {
					await other.transaction(async (txn: Transaction) => {
						await other.put('c', '3', { transaction: txn });
					});
					second.dropSync();
					expect(victim.columns).not.toContain('second');
				} finally {
					other.close();
					second.close();
				}
				await expectHealthy(victim, dbPath, false);
			}
		));

	it('releases the verification-table intents of a refused commit', () =>
		dbRunner(
			{
				dbOptions: [
					{ name: 'victim', verificationTable: true },
					{ name: 'doomed', verificationTable: true },
					{ name: 'doomed', verificationTable: true },
				],
			},
			async ({ db: victim, dbPath }, { db: doomed }, { db: stale }) => {
				await rejectsWithDropped(
					stale.transaction(async (txn: Transaction) => {
						await victim.put('live', 'A', { transaction: txn });
						await stale.put('dead', 'B', { transaction: txn });
						doomed.dropSync();
					}),
					'doomed'
				);
				expect(victim.getSync('live')).toBeUndefined();
				// a coordinated-retry writer on the same key parks on a leaked write
				// intent until the park timeout; a released one lets it commit at once
				const started = Date.now();
				await victim.transaction(
					async (txn: Transaction) => {
						await victim.put('live', 'C', { transaction: txn });
					},
					{ coordinatedRetry: true }
				);
				expect(Date.now() - started).toBeLessThan(2000);
				expect(victim.getSync('live')).toBe('C');
				await expectHealthy(victim, dbPath, false);
			}
		));

	it('refuses a logged transaction before its log batch is written when the drop already began', () =>
		dbRunner(
			{ dbOptions: [{ name: 'victim' }, { name: 'doomed' }, { name: 'doomed' }] },
			async ({ db: victim, dbPath }, { db: doomed }, { db: stale }) => {
				const error = await rejectsWithDropped(
					stale.transaction(async (txn: Transaction) => {
						stale.useLog('audit').addEntry(Buffer.from('entry'), txn.id);
						await stale.put('a', '1', { transaction: txn });
						doomed.dropSync();
					}),
					'doomed'
				);
				// the pre-check kept the doomed batch out of the log, so the caller sees
				// the refusal itself rather than an abandonment
				expect(error.hasLog).toBe(false);
				await expectHealthy(victim, dbPath, false);
			}
		));

	it('keeps the commit failure reachable as the cause of an abandoned logged transaction', () =>
		dbRunner({ dbOptions: [{ name: 'victim' }] }, async ({ db: victim }) => {
			// a forced TryAgain after the log write, with no retry budget, is the
			// deterministic way to abandon a logged transaction
			forceTryAgainForTesting(1);
			let caught: unknown;
			try {
				await victim.transaction(
					async (txn: Transaction) => {
						victim.useLog('audit').addEntry(Buffer.from('entry'), txn.id);
						await victim.put('a', '1', { transaction: txn });
					},
					{ maxRetries: 1 }
				);
			} catch (err) {
				caught = err;
			} finally {
				forceTryAgainForTesting(0);
			}
			expect(caught).toBeInstanceOf(TransactionAbandonedError);
			expect((caught as CodedError).cause).toMatchObject({ code: 'ERR_TRY_AGAIN', hasLog: true });
		}));

	it('frees the name on a retry after RocksDB dropped the family but reported a failure', () =>
		dbRunner(
			{ dbOptions: [{ name: 'victim' }, { name: 'doomed' }, { name: 'doomed' }] },
			async ({ db: victim, dbPath }, { db: doomed }, { db: stale }) => {
				// the OPTIONS-persistence shape: RocksDB removed the family, then errored
				forceDropFailureForTesting(1);
				expect(() => doomed.dropSync()).toThrow(/forced post-drop failure/);
				forceDropFailureForTesting(0);
				// admission stays closed: commits are refused, never poisoned
				await rejectsWithDropped(
					stale.transaction(async (txn: Transaction) => {
						await stale.put('a', '1', { transaction: txn });
					}),
					'doomed'
				);
				victim.putSync('ok', 1);
				// the name still points at the dropped generation until a drop completes
				expect(victim.columns).toContain('doomed');
				// the retry lands on RocksDB's already-dropped path and retires the entry
				doomed.dropSync();
				expect(victim.columns).not.toContain('doomed');
				const fresh = RocksDatabase.open(dbPath, { name: 'doomed' });
				try {
					fresh.putSync('f', 1);
					expect(fresh.getSync('f')).toBe(1);
					// a stale handle's late re-drop leaves the fresh generation alone
					stale.dropSync();
					expect(fresh.columns).toContain('doomed');
					fresh.putSync('g', 2);
					expect(fresh.getSync('g')).toBe(2);
					fresh.dropSync();
				} finally {
					fresh.close();
				}
				await expectHealthy(victim, dbPath, false);
			}
		));
});

/**
 * Same-thread ordering under every commit execution mode, in a child process so
 * ROCKSDB_JS_COMMIT_THREAD and the admitted-window seam are set in the
 * environment the process starts with (both are read once per process).
 */
describe('same-thread dropSync() across commit execution modes', () => {
	const fixturePath = join(__dirname, 'fixtures', 'fork-drop-commit-race.mts');
	const commitModes: { label: string; env: Record<string, string> }[] = [
		{ label: 'legacy libuv (0)', env: { ROCKSDB_JS_COMMIT_THREAD: '0' } },
		{ label: 'single lane (default)', env: {} },
		{ label: 'two-lane (2)', env: { ROCKSDB_JS_COMMIT_THREAD: '2' } },
	];

	function runFixture(mode: string, env: Record<string, string>): Promise<Record<string, any>> {
		return new Promise((resolve, reject) => {
			const dbPath = generateDBPath();
			const childEnv: NodeJS.ProcessEnv = { ...process.env, ...env };
			if (!('ROCKSDB_JS_COMMIT_THREAD' in env)) {
				delete childEnv.ROCKSDB_JS_COMMIT_THREAD;
			}
			const child = spawn(process.execPath, [fixturePath, dbPath, mode], { env: childEnv });
			let stdout = '';
			let stderr = '';
			let settled = false;
			// A gate regression wedges the child inside dropSync(), where no in-child
			// timer can run; reap it from here rather than leaving it orphaned.
			const watchdog = setTimeout(() => {
				if (settled) return;
				settled = true;
				child.kill('SIGKILL');
				reject(new Error(`fixture timed out (${mode})\n${stderr}`));
			}, 40_000);
			child.stdout.on('data', (chunk) => (stdout += chunk));
			child.stderr.on('data', (chunk) => (stderr += chunk));
			child.on('error', (err) => {
				clearTimeout(watchdog);
				if (!settled) {
					settled = true;
					reject(err);
				}
			});
			child.on('close', (code, signal) => {
				clearTimeout(watchdog);
				rmSync(dbPath, { recursive: true, force: true });
				if (settled) return;
				settled = true;
				if (code !== 0 || signal) {
					reject(new Error(`fixture exited code=${code} signal=${signal}\n${stderr}`));
					return;
				}
				resolve(JSON.parse(stdout.trim().split('\n').pop() ?? '{}'));
			});
		});
	}

	for (const { label, env } of commitModes) {
		it.each(modes)(
			`${label}: the drop waits for the thread's own admitted commit ($mode)`,
			async ({ mode }) => {
				const result = await runFixture(mode, env);
				expect(result.commitWins).toBe('fulfilled');
				expect(result.dropBeganAfterAdmission).toBe(true);
				// the helper's sync commit was refused while the drop was still waiting
				expect(result.lateCommit?.code).toBe('ERR_COLUMN_FAMILY_DROPPED');
				expect(result.lateCommit?.message).toMatch(/is being dropped/);
				expect(result.lateAdmittedDelta).toBe(0);
				expect(result.unrelatedDuring).toBe('ok');
				if (env.ROCKSDB_JS_COMMIT_THREAD === '2') {
					// the authoritative refusal after the log batch was already persisted
					expect(result.afterLog?.error?.code).toBe('ERR_TRANSACTION_ABANDONED');
					expect(result.afterLog?.cause?.code).toBe('ERR_COLUMN_FAMILY_DROPPED');
					expect(result.afterLog?.cause?.message).toMatch(DROPPED_MESSAGE);
					expect(result.afterLog?.liveApplied).toBeUndefined();
					expect(result.afterLog?.heldApplied).toBe(1);
					expect(result.afterLog?.watermarkReleased).toBe(true);
				}
				expect(result.dropWins?.code).toBe('ERR_COLUMN_FAMILY_DROPPED');
				expect(result.dropWins?.message).toMatch(DROPPED_MESSAGE);
				expect(result.victimProbe).toBe(1);
				expect(result.victimTxnProbe).toBe(2);
				expect(result.lastError).toBeNull();
				expect(result.freshProbe).toBe(3);
				expect(result.freshSeesOld).toBeNull();
			},
			60_000
		);
	}
});
