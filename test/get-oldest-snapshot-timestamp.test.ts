import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';

describe('getOldestSnapshotTimestamp()', () => {
	it('should get oldest snapshot timestamp in optimistic mode', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = new RocksDatabase(dbPath).open();
			expect(db.getOldestSnapshotTimestamp()).toBe(0);

			await db.transaction(async (tx) => {
				expect(db?.getOldestSnapshotTimestamp()).toBe(0);
				await tx.put('foo', 'bar');
				expect(db?.getOldestSnapshotTimestamp()).toBe(0);
				await tx.get('foo');
				expect(db?.getOldestSnapshotTimestamp()).toBeGreaterThan(0);
			});

			expect(db.getOldestSnapshotTimestamp()).toBe(0);
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should get oldest snapshot timestamp in pessimistic mode', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = new RocksDatabase(dbPath, { pessimistic: true }).open();
			expect(db.getOldestSnapshotTimestamp()).toBe(0);

			await db.transaction(async (tx) => {
				expect(db?.getOldestSnapshotTimestamp()).toBe(0);
				await tx.put('foo', 'bar');
				expect(db?.getOldestSnapshotTimestamp()).toBeGreaterThan(0);
				await tx.get('foo');
				expect(db?.getOldestSnapshotTimestamp()).toBeGreaterThan(0);
			});

			expect(db.getOldestSnapshotTimestamp()).toBe(0);
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});
});
