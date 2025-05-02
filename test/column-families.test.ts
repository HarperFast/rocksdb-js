import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';

describe('Column Families', () => {
	it('should open multiple column families', async () => {
		let db: RocksDatabase | null = null;
		let db2: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath);
			await db.put('foo', 'bar');

			db2 = await RocksDatabase.open(dbPath, { name: 'foo' });
			await db2.put('foo', 'bar2');

			expect(db.get('foo')).toBe('bar');
			expect(db2.get('foo')).toBe('bar2');
		} finally {
			db?.close();
			db2?.close();
			await rimraf(dbPath);
		}
	});

	it('should reuse same instance for same column family', async () => {
		let db: RocksDatabase | null = null;
		let db2: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, { name: 'foo' });
			await db.put('foo', 'bar');
			expect(db.get('foo')).toBe('bar');

			db2 = await RocksDatabase.open(dbPath, { name: 'foo' });
			expect(db2.get('foo')).toBe('bar');
		} finally {
			db?.close();
			db2?.close();
			await rimraf(dbPath);
		}
	});
});
