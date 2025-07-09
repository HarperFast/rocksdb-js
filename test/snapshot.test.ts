import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import { setTimeout as delay } from 'node:timers/promises';

describe('getOldestSnapshotTimestamp()', () => {
	it('should get oldest snapshot timestamp in optimistic mode', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = new RocksDatabase(dbPath).open();
			expect(db.getOldestSnapshotTimestamp()).toBe(0);

			await db.transaction(async (txn) => {
				expect(db?.getOldestSnapshotTimestamp()).toBe(0);
				await txn.put('foo', 'bar');
				expect(db?.getOldestSnapshotTimestamp()).toBe(0);
				await txn.get('foo');
				expect(db?.getOldestSnapshotTimestamp()).toBeGreaterThan(0);
			});

			expect(db.getOldestSnapshotTimestamp()).toBe(0);
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should get oldest snapshot timestamp in optimistic mode', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = new RocksDatabase(dbPath).open();
			expect(db.getOldestSnapshotTimestamp()).toBe(0);

			const promise = db.transaction(async (txn) => {
				await txn.get('foo');
				await delay(100);
			});

			expect(db?.getOldestSnapshotTimestamp()).toBeGreaterThan(0);
			await promise;
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

			await db.transaction(async (txn) => {
				expect(db?.getOldestSnapshotTimestamp()).toBe(0);
				await txn.put('foo', 'bar');
				expect(db?.getOldestSnapshotTimestamp()).toBeGreaterThan(0);
				await txn.get('foo');
				expect(db?.getOldestSnapshotTimestamp()).toBeGreaterThan(0);
			});

			expect(db.getOldestSnapshotTimestamp()).toBe(0);
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});
});
