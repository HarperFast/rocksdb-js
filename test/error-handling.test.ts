import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { join } from 'node:path';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { tmpdir } from 'node:os';

describe('Error Handling', () => {
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

	it('should error if database not open', async () => {
		db = new RocksDatabase(dbPath);
		await expect(db.get('test')).rejects.toThrow('Database not open');
	});
});
