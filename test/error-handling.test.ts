import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import { DBI } from '../src/dbi.js';

describe('Error Handling', () => {
	it('should error if database not open', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = new RocksDatabase(dbPath);
			await expect(db.get('test')).rejects.toThrow('Database not open');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should error creating an abstract DBI instance', async () => {
		expect(() => new DBI(null as any)).toThrow('DBI is an abstract class and cannot be instantiated directly');
	});
});
