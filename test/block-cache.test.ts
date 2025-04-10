import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';

describe('Block Cache', () => {
	it('should disable block cache', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, { blockCacheSize: 0 });
			db.put('foo', 'bar');
			await expect(db.get('foo')).resolves.toBe('bar');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should enable block cache', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, { blockCacheSize: 1024 * 1024 });
			db.put('foo', 'bar');
			await expect(db.get('foo')).resolves.toBe('bar');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should throw error when block cache size is negative', async () => {
		const dbPath = generateDBPath();

		try {
			await expect(RocksDatabase.open(dbPath, { blockCacheSize: -1 }))
				.rejects.toThrow('napi_invalid_arg');
		} finally {
			await rimraf(dbPath);
		}
	});
});
