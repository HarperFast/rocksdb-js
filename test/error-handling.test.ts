import { afterEach, describe, expect, it } from 'vitest';
import { RocksDatabase } from '../src/index.js';
import { rimraf } from 'rimraf';

describe('Error Handling', () => {
	let db: RocksDatabase | null = null;

	afterEach(() => {
		if (db) {
			db.close();
			db = null;
		}
		return rimraf('/tmp/testdb');
	});

	it('should error if database not open', async () => {
		db = new RocksDatabase('/tmp/testdb');
		await expect(db.get('test')).rejects.toThrow('Database not open');
	});
});
