import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { join } from 'node:path';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { tmpdir } from 'node:os';

describe('Column Families', () => {
	let db: RocksDatabase | null = null;
	const dbPath = join(tmpdir(), 'testdb');

	beforeEach(() => rimraf(dbPath));

	afterEach(() => {
		if (db) {
			db.close();
			db = null;
		}
		return rimraf(dbPath);
	});

	it('should open multiple column families', async () => {
		db = await RocksDatabase.open(dbPath);
		db.put('foo', 'bar');

		let db2: RocksDatabase | null = null;
		try {
			db2 = await RocksDatabase.open(dbPath, { name: 'foo' });
			db2.put('foo', 'bar2');

			await expect(db.get('foo')).resolves.toBe('bar');
			await expect(db2.get('foo')).resolves.toBe('bar2');
		} finally {
			db2?.close();
		}
	});

	it('should reuse same instance for same column family', async () => {
		db = await RocksDatabase.open(dbPath, { name: 'foo' });
		db.put('foo', 'bar');

		let db2: RocksDatabase | null = null;
		try {
			db2 = await RocksDatabase.open(dbPath, { name: 'foo' });
			await expect(db2.get('foo')).resolves.toBe('bar');
		} finally {
			db2?.close();
		}
	});
});
