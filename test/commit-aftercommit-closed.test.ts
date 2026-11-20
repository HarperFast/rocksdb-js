import { RocksDatabase, Transaction } from '../src/index.ts';
import { dbRunner } from './lib/util.ts';
import { describe, expect, it } from 'vitest';

function closeBeforeAfterCommit(txn: Transaction, db: RocksDatabase): void {
	const notify = txn.notify.bind(txn);
	txn.notify = (event: string, ...args: any[]) => {
		if (event === 'aftercommit') {
			db.close();
		}
		return notify(event, ...args);
	};
}

describe('aftercommit after a durable commit', () => {
	for (const method of ['commit', 'commitSync'] as const) {
		it(`${method}() succeeds and warns when the database closed after the commit`, () =>
			dbRunner(async ({ db, dbPath }) => {
				const warnings: string[] = [];
				let warned!: () => void;
				const warning = new Promise<void>((resolve) => (warned = resolve));
				const onWarn = (msg: string) => {
					warnings.push(msg);
					warned();
				};
				RocksDatabase.on('log.warn', onWarn);
				try {
					const txn = new Transaction(db.store);
					txn.putSync('k', 'v');
					closeBeforeAfterCommit(txn, db);
					await (method === 'commit' ? txn.commit() : txn.commitSync());
					await warning;
					expect(warnings.filter((w) => w.includes('aftercommit'))).toHaveLength(1);
				} finally {
					RocksDatabase.off('log.warn', onWarn);
				}
				const reopened = RocksDatabase.open(dbPath);
				try {
					expect(reopened.getSync('k')).toBe('v');
				} finally {
					reopened.close();
				}
			}));
	}

	for (const method of ['commit', 'commitSync'] as const) {
		it(`${method}() still propagates an aftercommit failure while the database is open`, () =>
			dbRunner(async ({ db }) => {
				const txn = new Transaction(db.store);
				txn.putSync('k', 'v');
				const notify = txn.notify.bind(txn);
				txn.notify = (event: string, ...args: any[]) => {
					if (event === 'aftercommit') {
						throw new Error('notify failed');
					}
					return notify(event, ...args);
				};
				await expect(async () =>
					method === 'commit' ? txn.commit() : txn.commitSync()
				).rejects.toThrow('notify failed');
				expect(db.getSync('k')).toBe('v');
			}));
	}
});
