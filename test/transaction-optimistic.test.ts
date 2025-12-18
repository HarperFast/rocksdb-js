import { describe, expect, it } from 'vitest';
import { RocksDatabase, Transaction } from '../src/index.js';
import { dbRunner } from './lib/util.js';
describe('Optimistic Transaction Specific Behavior', () => {
	it(`async should be able to resume a conflicted transaction`, () => dbRunner({
	}, async ({ db }) => {
		await db.put('foo', 'bar1');

		setTimeout(() => db.put('foo', 'bar2'));

		const transaction = new Transaction(db.store);
		let initialValue = await db.get('foo', { transaction });
		await new Promise((resolve) => setTimeout(resolve, 10));
		try {
			await db.put('foo', 'bar3', { transaction });
			await transaction.commit();
		} catch(error: unknown | Error & { code: string }) {
			initialValue = await db.get('foo', { transaction });
			expect(initialValue).toBe('bar2');
		}
		await db.put('foo', 'bar3', { transaction });
		await transaction.commit();
	}));
});
