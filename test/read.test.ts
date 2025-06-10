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

	describe('getSync()', () => {
		it('should error if database is not open', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = new RocksDatabase(dbPath);
				expect(() => db!.getSync('foo')).toThrow('Database not open');
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
				const value = db.getSync('baz');
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
				expect(() => (db!.getSync as any)()).toThrow('Key is required');
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
				await expect(db!.getBinary('foo')).rejects.toThrow('Database not open');
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
				const value = await db.getBinary('baz');
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
				expect(() => (db!.getBinary as any)()).toThrow('Key is required');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	describe('getBinarySync()', () => {
		it('should error if database is not open', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = new RocksDatabase(dbPath);
				expect(() => db!.getBinarySync('foo')).toThrow('Database not open');
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
				const value = db.getBinarySync('baz');
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
				expect(() => (db!.getBinarySync as any)()).toThrow('Key is required');
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
				await expect(db!.getBinaryFast('foo')).rejects.toThrow('Database not open');
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
				const value = await db.getBinaryFast('baz');
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
				expect(() => (db!.getBinaryFast as any)()).toThrow('Key is required');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	describe('getBinaryFastSync()', () => {
		it('should error if database is not open', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = new RocksDatabase(dbPath);
				expect(() => db!.getBinaryFastSync('foo')).toThrow('Database not open');
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
				const value = db.getBinaryFastSync('baz');
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
				expect(() => (db!.getBinaryFastSync as any)()).toThrow('Key is required');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	describe('getKeys()', () => {
		it('should error if database is not open', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = new RocksDatabase(dbPath);
				await expect(db.getKeys()).rejects.toThrow('Database not open');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	// getKeys()
	// getRange()
	// getValues()
	// getValuesCount()
});
