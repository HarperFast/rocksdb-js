import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';

describe('Write operations', () => {
	describe('put()', () => {
		it('should set and get a value using default column family', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = RocksDatabase.open(dbPath);
				await db.put('foo', 'bar1');
				const value = await db.get('foo');
				expect(value).toBe('bar1');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should set and get a value using custom column family', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = RocksDatabase.open(dbPath, {
					name: 'foo'
				});
				await db.put('foo', 'bar2');
				const value = await db.get('foo');
				expect(value).toBe('bar2');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should throw an error if key is not specified', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = RocksDatabase.open(dbPath);
				await expect((db.put as any)()).rejects.toThrow('Key is required');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should throw an error if database is closed', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = RocksDatabase.open(dbPath);
				await db.close();
				await expect((db.put as any)()).rejects.toThrow('Database not open');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	describe('putSync()', () => {
		it('should set and get a value using default column family', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = RocksDatabase.open(dbPath);
				db.putSync('foo', 'bar1');
				const value = await db.get('foo');
				expect(value).toBe('bar1');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should set and get a value using custom column family', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = RocksDatabase.open(dbPath, {
					name: 'foo'
				});
				db.putSync('foo', 'bar2');
				const value = await db.get('foo');
				expect(value).toBe('bar2');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should throw an error if key is not specified', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = RocksDatabase.open(dbPath);
				expect(() => (db!.putSync as any)()).toThrow('Key is required');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should throw an error if database is closed', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = RocksDatabase.open(dbPath);
				await db.close();
				expect(() => (db!.putSync as any)()).toThrow('Database not open');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	describe('remove()', () => {
		it('should not error if removing a non-existent key', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = RocksDatabase.open(dbPath);
				await db.remove('baz');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should remove a key', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = RocksDatabase.open(dbPath);
				let value = await db.get('foo');
				expect(value).toBeUndefined();
				await db.put('foo', 'bar3');
				value = await db.get('foo');
				expect(value).toBe('bar3');
				await db.remove('foo');
				value = await db.get('foo');
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
				db = RocksDatabase.open(dbPath);
				await expect((db.remove as any)()).rejects.toThrow('Key is required');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should throw an error if database is closed', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = RocksDatabase.open(dbPath);
				await db.close();
				await expect((db.remove as any)()).rejects.toThrow('Database not open');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});
});
