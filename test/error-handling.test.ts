import { describe, expect, it } from 'vitest';
import { DBI } from '../src/dbi.js';
import { RocksDatabase, Store } from '../src/index.js';
import { dbRunner, generateDBPath } from './lib/util.js';

describe('Error Handling', () => {
	it('should error if database path is invalid', () => {
		expect(() => new RocksDatabase(undefined as any)).toThrow('Invalid database path or store');
		expect(() => new RocksDatabase(null as any)).toThrow('Invalid database path or store');
		expect(() => new RocksDatabase(1 as any)).toThrow('Invalid database path or store');

		// if it's a string, it thinks it's a path
		expect(() => new RocksDatabase('')).toThrow('Invalid database path');
	});

	it('should error if database options is not an object', () => {
		expect(() => new RocksDatabase(generateDBPath(), 'foo' as any)).toThrow(
			'Database options must be an object'
		);
		expect(() => new RocksDatabase(generateDBPath(), 1 as any)).toThrow(
			'Database options must be an object'
		);
	});

	it('should error if database not open', () =>
		dbRunner({ skipOpen: true }, async ({ db }) => {
			await expect(db.get('test')).rejects.toThrow('Database not open');
		}));

	it('should error creating an abstract DBI instance', () => {
		expect(() => new DBI(null as any)).toThrow(
			'DBI is an abstract class and cannot be instantiated directly'
		);
	});

	it('should error if store is invalid', () => {
		expect(() => new Store(null as any)).toThrow('Invalid database path');
	});
});
