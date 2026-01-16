import { setTimeout as delay } from 'node:timers/promises';
import { describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';

describe('getOldestSnapshotTimestamp()', () => {
	it('should get oldest snapshot timestamp in optimistic mode', () =>
		dbRunner(async ({ db }) => {
			expect(db.getOldestSnapshotTimestamp()).toBe(0);

			await db.transaction(async (txn) => {
				expect(db?.getOldestSnapshotTimestamp()).toBe(0);
				await txn.put('foo', 'bar');
				expect(db?.getOldestSnapshotTimestamp()).toBe(0);
				await txn.get('foo');
				expect(db?.getOldestSnapshotTimestamp()).toBeGreaterThan(0);
			});

			expect(db.getOldestSnapshotTimestamp()).toBe(0);
		}));

	it('should get oldest snapshot timestamp in optimistic mode', () =>
		dbRunner(async ({ db }) => {
			expect(db.getOldestSnapshotTimestamp()).toBe(0);

			const promise = db.transaction(async (txn) => {
				await txn.get('foo');
				await delay(100);
			});

			expect(db?.getOldestSnapshotTimestamp()).toBeGreaterThan(0);
			await promise;
			expect(db.getOldestSnapshotTimestamp()).toBe(0);
		}));

	it('should get oldest snapshot timestamp in pessimistic mode', () =>
		dbRunner({ dbOptions: [{ pessimistic: true }] }, async ({ db }) => {
			expect(db.getOldestSnapshotTimestamp()).toBe(0);

			await db.transaction(async (txn) => {
				expect(db?.getOldestSnapshotTimestamp()).toBe(0);
				await txn.put('foo', 'bar');
				expect(db?.getOldestSnapshotTimestamp()).toBeGreaterThan(0);
				await txn.get('foo');
				expect(db?.getOldestSnapshotTimestamp()).toBeGreaterThan(0);
			});

			expect(db.getOldestSnapshotTimestamp()).toBe(0);
		}));
});
