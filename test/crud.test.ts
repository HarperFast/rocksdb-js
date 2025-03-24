import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { join } from 'node:path';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { tmpdir } from 'node:os';

describe('CRUD Operations', () => {
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

	describe('get()', () => {
		it('should return undefined if key does not exist', async () => {
			db = await RocksDatabase.open(dbPath);
			const value = await db.get('baz');
			expect(value).toBeUndefined();
		});
	});

	describe('set()', () => {
		it('should set and get a value using default column family', async () => {
			db = await RocksDatabase.open(dbPath, {
				parallelism: 2
			});
			await db.put('foo', 'bar1');
			const value = await db.get('foo');
			expect(value).toBe('bar1');
		});

		it('should set and get a value using custom column family', async () => {
			db = await RocksDatabase.open(dbPath, {
				name: 'foo',
				parallelism: 2
			});
			await db.put('foo', 'bar2');
			const value = await db.get('foo');
			expect(value).toBe('bar2');
		});
	});

	describe('remove()', () => {
		it('should not error if removing a non-existent key', async () => {
			db = await RocksDatabase.open(dbPath);
			await db.remove('baz');
		});

		it('should remove a key', async () => {
			db = await RocksDatabase.open(dbPath);
			let value = await db.get('foo');
			expect(value).toBeUndefined();
			await db.put('foo', 'bar3');
			value = await db.get('foo');
			expect(value).toBe('bar3');
			await db.remove('foo');
			value = await db.get('foo');
			expect(value).toBeUndefined();
		});
	});
});
