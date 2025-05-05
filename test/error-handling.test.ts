import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import { DBI } from '../src/dbi.js';

describe('Error Handling', () => {
	it('should error if database path is invalid', async () => {
		expect(() => new RocksDatabase(undefined as any)).toThrow('Invalid database path');
		expect(() => new RocksDatabase(null as any)).toThrow('Invalid database path');
		expect(() => new RocksDatabase('')).toThrow('Invalid database path');
		expect(() => new RocksDatabase(1 as any)).toThrow('Invalid database path');
	});

	it('should error if database options is not an object', async () => {
		expect(() => new RocksDatabase(generateDBPath(), 'foo' as any)).toThrow('Database options must be an object');
		expect(() => new RocksDatabase(generateDBPath(), 1 as any)).toThrow('Database options must be an object');
	});

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
