import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';

describe('CRUD Operations', () => {
	describe('get()', () => {
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

	describe('put()', () => {
		it('should set and get a value using default column family', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = await RocksDatabase.open(dbPath, {
					parallelismThreads: 2
				});
				db.put('foo', 'bar1');
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
				db = await RocksDatabase.open(dbPath, {
					name: 'foo',
					parallelismThreads: 2
				});
				db.put('foo', 'bar2');
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
				db = await RocksDatabase.open(dbPath);
				// @ts-expect-error - Calling remove without any args
				expect(() => (db.put as any)()).toThrow('Key is required');
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
				db = await RocksDatabase.open(dbPath);
				db.remove('baz');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should remove a key', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = await RocksDatabase.open(dbPath);
				let value = await db.get('foo');
				expect(value).toBeUndefined();
				db.put('foo', 'bar3');
				value = await db.get('foo');
				expect(value).toBe('bar3');
				db.remove('foo');
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
				db = await RocksDatabase.open(dbPath);
				// @ts-expect-error - Calling remove without any args
				expect(() => (db.remove as any)()).toThrow('Key is required');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});
});
