// Regression test for HarperFast/rocksdb-js#964: transaction() and transactionSync() must run
// the body at most `maxRetries` times.

import { dbRunner } from './lib/util.ts';
import { describe, expect, it } from 'vitest';

describe('maxRetries bounds total attempts', () => {
	it('transaction() runs the body maxRetries times', () =>
		dbRunner(async ({ db }) => {
			await db.put('k', 'initial');
			let attempts = 0;
			await expect(
				db.transaction(
					async (txn, attempt) => {
						attempts = attempt;
						await txn.get('k');
						await db.put('k', `conflict-${attempt}`);
						await txn.put('k', 'x');
					},
					{ retryOnBusy: true, maxRetries: 3 }
				)
			).rejects.toThrow();
			expect(attempts).toBe(3);
		}));

	it('transactionSync() runs the body maxRetries times', () =>
		dbRunner(async ({ db }) => {
			db.putSync('k', 'initial');
			let attempts = 0;
			expect(() =>
				db.transactionSync(
					(txn, attempt) => {
						attempts = attempt;
						txn.getSync('k');
						db.putSync('k', `conflict-${attempt}`);
						txn.putSync('k', 'x');
					},
					{ retryOnBusy: true, maxRetries: 3 }
				)
			).toThrow();
			expect(attempts).toBe(3);
		}));
});
