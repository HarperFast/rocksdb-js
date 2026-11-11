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

	it('treats maxRetries: 0 as a single attempt in both helpers', () =>
		dbRunner(async ({ db }) => {
			let runs = 0;
			await db.transaction(
				async (txn) => {
					runs++;
					await txn.put('a', '1');
				},
				{ maxRetries: 0 }
			);
			db.transactionSync(
				(txn) => {
					runs++;
					txn.putSync('b', '2');
				},
				{ maxRetries: 0 }
			);
			expect(runs).toBe(2);
			expect(await db.get('a')).toBe('1');
			expect(db.getSync('b')).toBe('2');
		}));
});
