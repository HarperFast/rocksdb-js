import { RocksDatabase } from '../src/index.ts';
import { Transaction, TransactionAlreadyAbortedError } from '../src/transaction.ts';
import { dbRunner, generateDBPath, runFixture } from './lib/util.ts';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const methods = ['commit', 'commitSync'] as const;

async function commitError(txn: Transaction, method: (typeof methods)[number]): Promise<unknown> {
	try {
		await (method === 'commit' ? txn.commit() : txn.commitSync());
	} catch (error) {
		return error;
	}
	throw new Error(`${method}() unexpectedly succeeded`);
}

function expectNothingCommitted(dbPath: string): void {
	const reopened = RocksDatabase.open(dbPath);
	try {
		expect(reopened.getSync('k')).toBeUndefined();
	} finally {
		reopened.close();
	}
}

describe('Transaction commit error precedence', () => {
	for (const method of methods) {
		describe(`${method}()`, () => {
			// Closing the last handle aborts its pending transactions.
			for (const state of ['pending', 'aborted', 'abandoned'] as const) {
				it(`reports a closed database over a transaction that was ${state}`, () =>
					dbRunner(async ({ db }) => {
						const txn = new Transaction(db.store);
						txn.putSync('k', 'v');
						if (state === 'aborted') txn.abort();
						if (state === 'abandoned') txn.abandonWrites();
						db.close();
						expect(await commitError(txn, method)).toMatchObject({
							message: 'Database not open',
						});
					}));
			}

			it('reports the abandonment over a family dropped after it', () =>
				dbRunner(
					{ dbOptions: [{ name: 'home' }, { name: 'doomed' }, { name: 'doomed' }] },
					async ({ db: home }, { db: doomed }, { db: dropper }) => {
						const txn = new Transaction(home.store);
						txn.putSync('live', 'A');
						doomed.putSync('dead', 'B', { transaction: txn });
						txn.abandonWrites();
						dropper.dropSync();
						expect(await commitError(txn, method)).toMatchObject({
							code: 'ERR_WRITES_ABANDONED',
						});
						expect(home.getSync('live')).toBeUndefined();
					}
				));

			it('treats a second commit as done even after a family it wrote was dropped', () =>
				dbRunner(
					{ dbOptions: [{ name: 'home' }, { name: 'doomed' }, { name: 'doomed' }] },
					async ({ db: home }, { db: doomed }, { db: dropper }) => {
						const txn = new Transaction(home.store);
						txn.putSync('live', 'A');
						doomed.putSync('dead', 'B', { transaction: txn });
						await (method === 'commit' ? txn.commit() : txn.commitSync());
						dropper.dropSync();
						await (method === 'commit' ? txn.commit() : txn.commitSync());
						expect(home.getSync('live')).toBe('A');
					}
				));
		});
	}

	it('db.transaction() rejects a commit on a database closed in its callback', () =>
		dbRunner(async ({ db, dbPath }) => {
			await expect(
				db.transaction((txn) => {
					txn.putSync('k', 'v');
					db.close();
				})
			).rejects.toThrow('Database not open');
			expectNothingCommitted(dbPath);
		}));

	it('db.transactionSync() throws for a commit on a database closed in its callback', () =>
		dbRunner(async ({ db, dbPath }) => {
			expect(() =>
				db.transactionSync((txn) => {
					txn.putSync('k', 'v');
					db.close();
				})
			).toThrow('Database not open');
			expectNothingCommitted(dbPath);
		}));

	// The database state that decides the error is the one at the native commit, not after it.
	it('commit() keeps a user abort when the database closes in the same tick', () =>
		dbRunner(async ({ db }) => {
			const txn = new Transaction(db.store);
			txn.putSync('k', 'v');
			txn.abort();
			const pending = txn.commit();
			db.close();
			await expect(pending).rejects.toBeInstanceOf(TransactionAlreadyAbortedError);
		}));

	it('db.transaction() keeps a user abort when the database closes in the same tick', () =>
		dbRunner(async ({ db }) => {
			const pending = db.transaction((txn) => {
				txn.abort();
			});
			await null;
			db.close();
			await expect(pending).resolves.toBeUndefined();
		}));

	it.each(['transaction', 'transactionSync'])(
		'db.%s() reports a foreign shutdown() that aborted the transaction after beforecommit',
		{ timeout: 20_000 },
		async (method) => {
			const path = generateDBPath();
			try {
				await runFixture(join(__dirname, 'fixtures', 'fork-commit-foreign-abort.mts'), path, {
					args: [method],
				});
			} finally {
				rmSync(path, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
			}
		}
	);
});
