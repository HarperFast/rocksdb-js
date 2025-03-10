import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { RocksDatabase } from '../src/index.js';
import { rimraf } from 'rimraf';

describe('Column Families', () => {
	let db: RocksDatabase | null = null;

	beforeEach(() => rimraf('/tmp/testdb'));

	afterEach(() => {
		if (db) {
			db.close();
			db = null;
		}
		return rimraf('/tmp/testdb');
	});

	it('should open multiple column families', async () => {
		db = await RocksDatabase.open('/tmp/testdb');
		db.put('foo', 'bar');

		let db2: RocksDatabase | null = null;
		try {
			db2 = await RocksDatabase.open('/tmp/testdb', { name: 'foo' });
			db2.put('foo', 'bar2');

			await expect(db.get('foo')).resolves.toBe('bar');
			await expect(db2.get('foo')).resolves.toBe('bar2');
		} finally {
			db2?.close();
		}
	});
	
	it('should reuse same instance for same column family', async () => {
		db = await RocksDatabase.open('/tmp/testdb', { name: 'foo'});
		db.put('foo', 'bar');

		let db2: RocksDatabase | null = null;
		try {
			db2 = await RocksDatabase.open('/tmp/testdb', { name: 'foo' });
			await expect(db2.get('foo')).resolves.toBe('bar');
		} finally {
			db2?.close();
		}
	});
});
