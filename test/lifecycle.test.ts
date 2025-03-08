import { afterEach, describe, expect, it } from 'vitest';
import { RocksDatabase } from '../src/index.js';
import { rimraf } from 'rimraf';

describe('Lifecycle', () => {
	let db: RocksDatabase | null = null;

	afterEach(() => {
		if (db) {
			db.close();
			db = null;
		}
		return rimraf('/tmp/testdb');
	});

	it('should open and close database', async () => {
		db = new RocksDatabase('/tmp/testdb');

		db.close(); // noop

		await db.open();
		await db.open(); // noop

		await expect(db.get('foo')).resolves.toBeUndefined();

		await db.close();

		await expect(db.get('foo')).rejects.toThrow('Database not open');
	});

	it('should open multiple column families', async () => {
		let db2: RocksDatabase | null = null;
		try {
			db = await RocksDatabase.open('/tmp/testdb', {
				parallelism: 2
			});
			db.put('foo', 'bar');

			db2 = await RocksDatabase.open('/tmp/testdb', {
				name: 'foo',
				parallelism: 2
			});
			db2.put('foo', 'bar2');

			await expect(db.get('foo')).resolves.toBe('bar');
			await expect(db2.get('foo')).resolves.toBe('bar2');
		} finally {
			db2?.close();
		}
	});
});
