import { RocksDatabase, Transaction } from '../src/index.ts';
import { forceDropFailureForTesting, forceTryAgainForTesting } from '../src/load-binding.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';

/**
 * Deferred, reference-counted column-family reclamation (AGENTS.md invariant
 * 23, HarperFast/rocksdb-js#806 / #726): a drop retires the family logically
 * and returns; the physical drop runs when no admitted commit names it.
 */

const fixturePath = join(__dirname, 'fixtures', 'fork-drop-deferred-commit.mts');
// The child matrix depends on Node's N-API worker teardown and process-signal
// behavior. Bun and Deno still run the in-process contract cases below; Deno's
// related worker-env teardown failures are tracked in #746.
const isNode = !process.versions.bun && !process.versions.deno;

function removeDBPath(dbPath: string): void {
	if (!process.env.KEEP_FILES) {
		rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
	}
}

type Scenario =
	| 'admitted-commit'
	| 'handle-closed'
	| 'handle-closed-retry'
	| 'worker-terminated'
	| 'open-waits'
	| 'crash-reopen'
	| 'retry-race'
	| 'staging-put'
	| 'staging-delete'
	| 'staging-timeout';
type TxnMode = 'optimistic' | 'pessimistic';
type DropKind = 'sync' | 'async';
type CommitThread = '0' | '1' | '2';

function runFixture(
	scenario: Scenario,
	txnMode: TxnMode,
	dropKind: DropKind,
	commitThread: CommitThread,
	dbPath = generateDBPath()
): Promise<{ code: number | null; signal: NodeJS.Signals | null; stdout: string; stderr: string }> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [fixturePath, dbPath, scenario, txnMode, dropKind], {
			env: {
				...process.env,
				ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS: '1000',
				ROCKSDB_JS_COMMIT_THREAD: commitThread,
			},
		});
		let stdout = '';
		let stderr = '';
		child.stdout?.on('data', (chunk) => (stdout += chunk.toString()));
		child.stderr?.on('data', (chunk) => (stderr += chunk.toString()));
		child.on('close', (code, signal) => resolve({ code, signal, stdout, stderr }));
		child.on('error', reject);
	});
}

async function expectFixture(
	scenario: Scenario,
	txnMode: TxnMode,
	dropKind: DropKind,
	commitThread: CommitThread
): Promise<void> {
	const result = await runFixture(scenario, txnMode, dropKind, commitThread);
	const label = `${scenario} ${txnMode} ${dropKind} ROCKSDB_JS_COMMIT_THREAD=${commitThread}`;
	expect(result.signal, `${label}\n${result.stderr}`).toBeNull();
	expect(result.code, `${label}\n${result.stderr}`).toBe(0);
	expect(JSON.parse(result.stdout.trim().split('\n').pop()!)).toMatchObject({ ok: true, scenario });
}

describe('Deferred column-family reclamation', () => {
	afterEach(() => {
		forceDropFailureForTesting(0);
		forceTryAgainForTesting(0);
	});

	describe.skipIf(!isNode)(
		'drop racing an admitted commit (child process, commit parked after admission)',
		() => {
			const commitThreads: CommitThread[] = ['0', '1', '2'];
			for (const commitThread of commitThreads) {
				for (const txnMode of ['optimistic', 'pessimistic'] as TxnMode[]) {
					for (const dropKind of ['sync', 'async'] as DropKind[]) {
						it(
							`returns at once, the commit lands, nothing is poisoned (${txnMode}, ${dropKind}, commit thread ${commitThread})`,
							{ timeout: 60_000 },
							() => expectFixture('admitted-commit', txnMode, dropKind, commitThread)
						);
					}
				}
				it(
					`reclaims after the committing worker is terminated (commit thread ${commitThread})`,
					{ timeout: 60_000 },
					() => expectFixture('worker-terminated', 'optimistic', 'sync', commitThread)
				);
				it(
					`reclaims after the committing handle closes (commit thread ${commitThread})`,
					{ timeout: 60_000 },
					() => expectFixture('handle-closed', 'optimistic', 'sync', commitThread)
				);
				it(
					`aborts a retry after the committing handle closes (commit thread ${commitThread})`,
					{ timeout: 60_000 },
					() => expectFixture('handle-closed-retry', 'optimistic', 'sync', commitThread)
				);
			}

			it(
				'open() of the dropped name waits for the admitted commit and creates a fresh family',
				{ timeout: 60_000 },
				() => expectFixture('open-waits', 'optimistic', 'sync', '1')
			);

			it(
				'open() racing a drop that retries a failed physical drop never reports a failure that did not happen',
				{ timeout: 60_000 },
				() => expectFixture('retry-race', 'optimistic', 'sync', '1')
			);

			// The documented cross-restart gap (AGENTS.md invariant 24): a process
			// killed while a physical drop is deferred leaves the old family live on disk.
			it(
				'a process killed inside the deferral window leaves the family on disk',
				{ timeout: 60_000 },
				async () => {
					const dbPath = generateDBPath();
					try {
						const result = await runFixture('crash-reopen', 'optimistic', 'sync', '1', dbPath);
						const diedAbruptly =
							result.signal === 'SIGKILL' ||
							result.code === 137 ||
							(process.platform === 'win32' && result.code === 1);
						expect(diedAbruptly, result.stderr).toBe(true);
						expect(result.stdout).toContain('ready');
						const reopened = RocksDatabase.open(dbPath);
						try {
							expect(reopened.columns).toContain('table');
						} finally {
							reopened.close();
						}
						const table = RocksDatabase.open(dbPath, { name: 'table' });
						try {
							expect(table.getSync('seed')).toBe('old-generation');
						} finally {
							table.close();
						}
					} finally {
						removeDBPath(dbPath);
					}
				}
			);
		}
	);

	describe.skipIf(!isNode)('drop between a pessimistic staging precheck and RocksDB', () => {
		for (const operation of ['put', 'delete'] as const) {
			it(
				`abandons the whole transaction when ${operation} reaches a physically dropped family`,
				{ timeout: 60_000 },
				() => expectFixture(`staging-${operation}`, 'pessimistic', 'sync', '1')
			);
		}

		it('abandons when an ordinary lock timeout overlaps retirement', { timeout: 60_000 }, () =>
			expectFixture('staging-timeout', 'pessimistic', 'sync', '1')
		);
	});

	it('exposes columnFamily.pendingReclaims in getStat() and getStats()', () =>
		dbRunner({ dbOptions: [{ name: 'table' }] }, ({ db }) => {
			expect(db.getStat('columnFamily.pendingReclaims')).toBe(0);
			expect(db.getStats()['columnFamily.pendingReclaims']).toBe(0);
		}));

	it('refuses the commit of a transaction staged before the drop on the dropping thread, without waiting', () =>
		dbRunner(
			{ dbOptions: [{ name: 'table' }, { name: 'table' }, { name: 'other' }] },
			async ({ db: staged, dbPath }, { db: dropper }, { db: other }) => {
				staged.putSync('seed', 'old');
				await expect(
					staged.transaction(async (txn) => {
						txn.putSync('k', 'v');
						// Harper drops from a synchronous schema section while this
						// transaction is staged on the same thread: the drop must not
						// wait for it.
						dropper.dropSync();
						expect(staged.columns).not.toContain('table');
						// Nothing is admitted, so the physical drop already ran.
						expect(staged.getStat('columnFamily.pendingReclaims')).toBe(0);
					})
				).rejects.toMatchObject({ code: 'ERR_COLUMN_FAMILY_DROPPED' });

				other.putSync('probe', 1);
				expect(other.getSync('probe')).toBe(1);
				expect(other.getLastError()).toBeNull();

				const fresh = RocksDatabase.open(dbPath, { name: 'table' });
				try {
					expect(fresh.getSync('seed')).toBeUndefined();
					expect(fresh.getSync('k')).toBeUndefined();
				} finally {
					fresh.close();
				}
			}
		));

	it('refuses staging a write to a retired family and commits a transaction that never touched it', () =>
		dbRunner(
			{ dbOptions: [{ name: 'home' }, { name: 'home' }, { name: 'other' }] },
			async ({ db: home }, { db: homeDropper }, { db: other }) => {
				// override-only batch: the transaction belongs to `home` but only
				// names `other`; dropping `home` must not refuse it
				await home.transaction(async (txn) => {
					await other.put('x', 1, { transaction: txn });
					homeDropper.dropSync();
				});
				expect(other.getSync('x')).toBe(1);

				// a later stage on the retired family is refused at staging
				await expect(
					home.transaction(async (txn) => {
						await home.put('late', 1, { transaction: txn });
					})
				).rejects.toMatchObject({ code: 'ERR_COLUMN_FAMILY_DROPPED' });
				await expect(
					other.transaction(async (txn) => {
						txn.putSync('fine', 1);
						await home.put('late', 1, { transaction: txn });
					})
				).rejects.toMatchObject({ code: 'ERR_COLUMN_FAMILY_DROPPED' });
				expect(other.getSync('fine')).toBeUndefined();
			}
		));

	it('abandons the transaction and releases its intents when a staging refusal is caught', () =>
		dbRunner(
			{
				dbOptions: [
					{ name: 'live', verificationTable: true },
					{ name: 'retired', verificationTable: true },
					{ name: 'retired', verificationTable: true },
				],
			},
			async ({ db: live }, { db: retired }, { db: dropper }) => {
				live.putSync('key', 'old');
				live.populateVersion('key', 1.5e12);
				const txn = new Transaction(live.store);
				txn.putSync('key', 'new');
				dropper.dropSync();

				expect(() => retired.putSync('other', 'value', { transaction: txn })).toThrowError(
					expect.objectContaining({ code: 'ERR_COLUMN_FAMILY_DROPPED' })
				);

				live.populateVersion('key', 1.6e12);
				expect(live.verifyVersion('key', 1.6e12)).toBe(true);

				// abandoned bars writes and commits, not reads, so the transaction
				// still serves the staged value no commit will ever produce
				expect(live.getSync('key', { transaction: txn })).toBe('new');

				expect(() => txn.commitSync()).toThrowError(
					expect.objectContaining({ code: 'ERR_WRITES_ABANDONED' })
				);
				expect(live.getSync('key')).toBe('old');
				txn.abort();
			}
		));

	it('refuses a commit whose override family was dropped after staging, whole', () =>
		dbRunner(
			{ dbOptions: [{ name: 'home' }, { name: 'other' }, { name: 'other' }] },
			async ({ db: home }, { db: other }, { db: otherDropper }) => {
				await expect(
					home.transaction(async (txn) => {
						txn.putSync('live', 'A');
						await other.put('dead', 'B', { transaction: txn });
						otherDropper.dropSync();
					})
				).rejects.toMatchObject({ code: 'ERR_COLUMN_FAMILY_DROPPED' });
				expect(home.getSync('live')).toBeUndefined();
				expect(home.getLastError()).toBeNull();
			}
		));

	for (const commitKind of ['async', 'sync'] as const) {
		it(`releases verification-table intents after ${commitKind} admission refusal`, () =>
			dbRunner(
				{
					dbOptions: [
						{ name: 'live', verificationTable: true },
						{ name: 'retired', verificationTable: true },
						{ name: 'retired', verificationTable: true },
					],
				},
				async ({ db: live }, { db: retired }, { db: dropper }) => {
					live.putSync('key', 'old');
					live.populateVersion('key', 1.5e12);
					const txn = new Transaction(live.store);
					txn.putSync('key', 'new');
					retired.putSync('other', 'value', { transaction: txn });
					dropper.dropSync();

					if (commitKind === 'async') {
						await expect(txn.commit()).rejects.toMatchObject({ code: 'ERR_COLUMN_FAMILY_DROPPED' });
					} else {
						expect(() => txn.commitSync()).toThrowError(
							expect.objectContaining({ code: 'ERR_COLUMN_FAMILY_DROPPED' })
						);
					}

					live.populateVersion('key', 1.6e12);
					expect(live.verifyVersion('key', 1.6e12)).toBe(true);
					// Admission refusal restores Pending for both commit APIs: reads
					// remain available even though writes and another commit are barred.
					expect(txn.getSync('key')).toBe('new');
					expect(() => txn.putSync('later', 'value')).toThrow(/abandoned/);
					txn.abort();
				}
			));
	}

	it('forgets a family when a pessimistic write fails before entering the batch', () =>
		dbRunner(
			{
				dbOptions: [
					{ name: 'live', pessimistic: true },
					{ name: 'contended', pessimistic: true },
					{ name: 'contended', pessimistic: true },
				],
			},
			async ({ db: live }, { db: contended }, { db: dropper }) => {
				const holder = new Transaction(contended.store);
				holder.putSync('locked', 'holder');

				const txn = new Transaction(live.store);
				expect(() => contended.putSync('locked', 'candidate', { transaction: txn })).toThrow(
					/timed out|busy/i
				);
				txn.putSync('landed', 'value');
				dropper.dropSync();
				await txn.commit();

				expect(live.getSync('landed')).toBe('value');
				holder.abort();
			}
		));

	it('keeps an open iterator on the dropped family serving and does not defer the drop for it', () =>
		dbRunner(
			{ dbOptions: [{ name: 'table' }, { name: 'table' }] },
			({ db: reader, dbPath }, { db: dropper }) => {
				for (let i = 0; i < 100; i++) {
					reader.putSync(`k${String(i).padStart(3, '0')}`, i);
				}
				const iterator = reader.getRange({})[Symbol.iterator]();
				let seen = 0;
				for (let i = 0; i < 10; i++) {
					expect(iterator.next().done).toBe(false);
					seen++;
				}

				dropper.dropSync();
				expect(reader.getStat('columnFamily.pendingReclaims')).toBe(0);

				for (;;) {
					const { done } = iterator.next();
					if (done) break;
					seen++;
				}
				expect(seen).toBe(100);
				expect(reader.getSync('k050')).toBe(50);

				const fresh = RocksDatabase.open(dbPath, { name: 'table' });
				try {
					expect(fresh.getKeysCount()).toBe(0);
				} finally {
					fresh.close();
				}
			}
		));

	it('rebuilds the touched set on a coordinated retry so a family dropped between attempts does not refuse a commit that no longer names it', () =>
		dbRunner(
			{ dbOptions: [{ name: 'a' }, { name: 'b' }, { name: 'b' }] },
			async ({ db: a }, { db: b }, { db: bDropper }) => {
				forceTryAgainForTesting(1);
				const attempts: number[] = [];
				await a.transaction(
					async (txn, attempt) => {
						attempts.push(attempt);
						txn.putSync('k', attempt);
						if (attempt === 1) {
							await b.put('k', 1, { transaction: txn });
						} else {
							bDropper.dropSync();
						}
					},
					{ retryOnBusy: true }
				);
				expect(attempts).toEqual([1, 2]);
				expect(a.getSync('k')).toBe(2);
			}
		));

	it('refuses a logged transaction at admission before any log bytes are written', () =>
		dbRunner(
			{ dbOptions: [{ name: 'table' }, { name: 'table' }] },
			async ({ db }, { db: dropper }) => {
				const log = db.useLog('audit');
				await expect(
					db.transaction(async (txn) => {
						txn.putSync('k', 'v');
						log.addEntry(Buffer.from('entry'), txn.id);
						dropper.dropSync();
					})
				).rejects.toMatchObject({ code: 'ERR_COLUMN_FAMILY_DROPPED', hasLog: false });
				expect(Array.from(log.query({ start: 0 })).length).toBe(0);
			}
		));

	it('drops through a second handle to the same generation idempotently and lists the name once recreated', () =>
		dbRunner(
			{ dbOptions: [{ name: 'table' }, { name: 'table' }] },
			({ db: db1, dbPath }, { db: db2 }) => {
				db1.putSync('k', 'v');
				db1.dropSync();
				expect(() => db2.dropSync()).not.toThrow();
				expect(db1.getStat('columnFamily.pendingReclaims')).toBe(0);
				const fresh = RocksDatabase.open(dbPath, { name: 'table' });
				try {
					fresh.putSync('k2', 'v2');
					// a stale re-drop of the old generation never touches the fresh one
					db2.dropSync();
					expect(fresh.columns).toContain('table');
					expect(fresh.getSync('k2')).toBe('v2');
				} finally {
					fresh.close();
				}
			}
		));

	describe('a physical drop that fails', () => {
		it('is reported by the drop that ran it, keeps the name retired, warns, and is retried by the next drop', () =>
			dbRunner(
				{ dbOptions: [{ name: 'table' }, { name: 'table' }, { name: 'other' }] },
				async ({ db: db1 }, { db: db2 }, { db: other }) => {
					const warnings: string[] = [];
					const onWarning = (message: string) => {
						if (message.includes('column family "table"')) warnings.push(message);
					};
					RocksDatabase.on('log.warn', onWarning);
					try {
						db1.putSync('k', 'v');
						forceDropFailureForTesting(1);
						expect(() => db1.dropSync()).toThrow(/forced drop failure/);
						expect(db1.columns).not.toContain('table');
						expect(db1.getStat('columnFamily.pendingReclaims')).toBe(1);
						await vi.waitFor(() => expect(warnings).toHaveLength(1));
						// the retired generation stays readable through its handles
						expect(db2.getSync('k')).toBe('v');

						forceDropFailureForTesting(0);
						// a second handle to the same generation retries the failed drop
						expect(() => db2.dropSync()).not.toThrow();
						expect(db1.getStat('columnFamily.pendingReclaims')).toBe(0);
						other.putSync('probe', 1);
						expect(other.getLastError()).toBeNull();
					} finally {
						RocksDatabase.off('log.warn', onWarning);
					}
				}
			));

		it('is retried by a drop on another family', () =>
			dbRunner(
				{ dbOptions: [{ name: 'table' }, { name: 'other' }] },
				({ db: table, dbPath }, { db: other }) => {
					forceDropFailureForTesting(1);
					expect(() => table.dropSync()).toThrow(/forced drop failure/);
					forceDropFailureForTesting(0);
					other.dropSync();
					expect(table.getStat('columnFamily.pendingReclaims')).toBe(0);
					const fresh = RocksDatabase.open(dbPath, { name: 'table' });
					try {
						expect(fresh.getKeysCount()).toBe(0);
					} finally {
						fresh.close();
					}
				}
			));

		it('is retried by open() of the same name, which then creates a fresh family', () =>
			dbRunner({ dbOptions: [{ name: 'table' }] }, async ({ db, dbPath }) => {
				db.putSync('k', 'v');
				forceDropFailureForTesting(1);
				await expect(db.drop()).rejects.toMatchObject({
					code: 'ERR_IO_ERROR',
					message: expect.stringMatching(/forced drop failure/),
				});
				expect(db.getStat('columnFamily.pendingReclaims')).toBe(1);

				// still failing: the open reports it and creates nothing
				expect(() => RocksDatabase.open(dbPath, { name: 'table' })).toThrow(
					/still being reclaimed/
				);
				expect(db.columns).not.toContain('table');

				forceDropFailureForTesting(0);
				const fresh = RocksDatabase.open(dbPath, { name: 'table' });
				try {
					expect(db.getStat('columnFamily.pendingReclaims')).toBe(0);
					expect(fresh.getSync('k')).toBeUndefined();
					fresh.putSync('k', 'new');
					expect(fresh.getSync('k')).toBe('new');
				} finally {
					fresh.close();
				}
			}));

		it('is retried on close, which performs the real drop', () => {
			const dbPath = generateDBPath();
			try {
				const db = RocksDatabase.open(dbPath, { name: 'table' });
				db.putSync('k', 'v');
				forceDropFailureForTesting(1);
				expect(() => db.dropSync()).toThrow(/forced drop failure/);
				expect(db.getStat('columnFamily.pendingReclaims')).toBe(1);
				forceDropFailureForTesting(0);
				db.close();

				const reopened = RocksDatabase.open(dbPath);
				try {
					expect(reopened.columns).toEqual(['default']);
				} finally {
					reopened.close();
				}
			} finally {
				forceDropFailureForTesting(0);
				removeDBPath(dbPath);
			}
		});

		it('resolves a drop that failed after RocksDB removed the family as already dropped on retry', () =>
			dbRunner(
				{ dbOptions: [{ name: 'table' }, { name: 'other' }] },
				({ db: table, dbPath }, { db: other }) => {
					forceDropFailureForTesting(2);
					expect(() => table.dropSync()).toThrow(/forced post-drop failure/);
					expect(table.getStat('columnFamily.pendingReclaims')).toBe(1);
					forceDropFailureForTesting(0);
					// the retry gets RocksDB's "already dropped", which is success
					other.dropSync();
					expect(table.getStat('columnFamily.pendingReclaims')).toBe(0);
					const fresh = RocksDatabase.open(dbPath, { name: 'table' });
					try {
						expect(fresh.getKeysCount()).toBe(0);
					} finally {
						fresh.close();
					}
				}
			));
	});
});
