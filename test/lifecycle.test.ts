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

	it.skip('should open and close database', async () => {
		db = new RocksDatabase('/tmp/testdb');

		db.close(); // noop

		await db.open();
		await db.open(); // noop

		await expect(db.get('foo')).resolves.toBeUndefined();

		await db.close();

		await expect(db.get('foo')).rejects.toThrow('Database not open');
	});
});
