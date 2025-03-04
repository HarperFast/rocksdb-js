import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { RocksDatabase } from '../src/index.js';
import { rimraf } from 'rimraf';

describe('basic functions', () => {
	let db: RocksDatabase | null = null;

	beforeEach(() => rimraf('/tmp/testdb'));

	afterEach(() => {
		if (db) {
			db.close();
			db = null;
		}
	});

	it('should set and get a value using default column family', async () => {
		db = await RocksDatabase.open('/tmp/testdb', { name: 'foo', parallelism: 2 });
		await db.put('test', 'test');
		const value = await db.get('test');
		expect(value).toBe('test');
	});

	it('should error if database not open', async () => {
		db = new RocksDatabase('/tmp/testdb');
		await expect(db.get('test')).rejects.toThrow('Database not open');
	});
});
