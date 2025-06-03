import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';

describe('Lifecycle', () => {
	it('should open and close database', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = new RocksDatabase(dbPath);

			expect(db.isOpen()).toBe(false);

			db.close(); // noop

			await db.open();
			await db.open(); // noop

			expect(db.isOpen()).toBe(true);
			expect(db.get('foo')).toBeUndefined();

			db.close();

			expect(db.isOpen()).toBe(false);

			await expect(db.get('foo')).rejects.toThrow('Database not open');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});
});
