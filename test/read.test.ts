import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';

describe('Read Operations', () => {
	describe('get()', () => {
		it('should error if database is not open', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = new RocksDatabase(dbPath);
				await expect(db.get('foo')).rejects.toThrow('Database not open');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should return undefined if key does not exist', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = await RocksDatabase.open(dbPath);
				const value = await db.get('baz');
				expect(value).toBeUndefined();
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should throw an error if key is not specified', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = await RocksDatabase.open(dbPath);
				await expect((db.get as any)()).rejects.toThrow('Key is required');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	describe('getBinary()', () => {
		it('should error if database is not open', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = new RocksDatabase(dbPath);
				await expect(db.getBinary('foo')).rejects.toThrow('Database not open');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	describe('getBinaryFast()', () => {
		it('should error if database is not open', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = new RocksDatabase(dbPath);
				await expect(db.getBinaryFast('foo')).rejects.toThrow('Database not open');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	// getEntry()
	// getKeys()
	// getRange()
	// getValues()
	// getValuesCount()
});
