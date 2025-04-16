import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';

describe('Block Cache', () => {
	it('should disable block cache', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, { noBlockCache: true });
			db.put('foo', 'bar');
			await expect(db.get('foo')).resolves.toBe('bar');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should enable block cache and override default size', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			RocksDatabase.config({ blockCacheSize: 1024 * 1024 });
			db = await RocksDatabase.open(dbPath);
			db.put('foo', 'bar');
			await expect(db.get('foo')).resolves.toBe('bar');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should change the block cache size', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			RocksDatabase.config({ blockCacheSize: 1024 * 1024 });

			db = await RocksDatabase.open(dbPath);
			db.put('foo', 'bar');
			await expect(db.get('foo')).resolves.toBe('bar');

			RocksDatabase.config({ blockCacheSize: 2048 * 1024 });
			await expect(db.get('foo')).resolves.toBe('bar');

			RocksDatabase.config({ blockCacheSize: 0 });
			await expect(db.get('foo')).resolves.toBe('bar');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should throw error when block cache size is negative', async () => {
		const dbPath = generateDBPath();

		try {
			expect(() => RocksDatabase.config({ blockCacheSize: -1 }))
				.toThrowError('Block cache size must be a positive integer or 0 to disable caching');
		} finally {
			await rimraf(dbPath);
		}
	});
});
