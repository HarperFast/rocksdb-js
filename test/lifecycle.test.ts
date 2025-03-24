import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { join } from 'node:path';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { tmpdir } from 'node:os';

describe('Lifecycle', () => {
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

	it('should open and close database', async () => {
		db = new RocksDatabase(dbPath);

		db.close(); // noop

		await db.open();
		await db.open(); // noop

		await expect(db.get('foo')).resolves.toBeUndefined();

		await db.close();

		await expect(db.get('foo')).rejects.toThrow('Database not open');
	});
});
