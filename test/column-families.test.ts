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

			await expect(db.get('foo')).resolves.toBe('bar');
			await expect(db2.get('foo')).resolves.toBe('bar2');
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
			await expect(db.get('foo')).resolves.toBe('bar');

			db2 = await RocksDatabase.open(dbPath, { name: 'foo' });
			await expect(db2.get('foo')).resolves.toBe('bar');
		} finally {
			db?.close();
			db2?.close();
			await rimraf(dbPath);
		}
	});
});
