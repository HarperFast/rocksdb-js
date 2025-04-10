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
	});

	describe('set()', () => {
		it('should set and get a value using default column family', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, {
					parallelismThreads: 2
				});
				await db.put('foo', 'bar1');
				const value = await db.get('foo');
				expect(value).toBe('bar1');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should set and get a value using custom column family', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, {
					name: 'foo',
					parallelismThreads: 2
				});
				await db.put('foo', 'bar2');
				const value = await db.get('foo');
				expect(value).toBe('bar2');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	describe('remove()', () => {
		it('should not error if removing a non-existent key', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath);
				await db.remove('baz');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should remove a key', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath);
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
	});
});
