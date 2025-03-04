import { afterEach, describe, expect, it } from 'vitest';
import { RocksDatabase } from '../src/index.js';
import { rimraf } from 'rimraf';
import fs from 'fs';

describe('CRUD Operations', () => {
	let db: RocksDatabase | null = null;

	afterEach(() => {
		if (db) {
			db.close();
			db = null;
		}
		return rimraf('/tmp/testdb');
	});

	describe('get()', () => {

		it('should return undefined if key does not exist', async () => {
			db = await RocksDatabase.open('/tmp/testdb');
			const value = await db.get('baz');
			expect(value).toBeUndefined();
		});
	});

	describe('set()', () => {
		it('should set and get a value using default column family', async () => {
			db = await RocksDatabase.open('/tmp/testdb', {
				parallelism: 2
			});
			await db.put('foo', 'bar1');
			const value = await db.get('foo');
			expect(value).toBe('bar1');
			console.log('done bar1');
		});

		it('should set and get a value using custom column family', async () => {
			db = await RocksDatabase.open('/tmp/testdb', {
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
			db = await RocksDatabase.open('/tmp/testdb');
			await db.remove('baz');
		});

		it('should remove a key', async () => {
			db = new RocksDatabase('/tmp/testdb');
			await db.open();
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
