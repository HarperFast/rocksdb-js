import { RocksDatabase } from '../src/index.js';
import { dbRunner } from './lib/util.js';
import { describe, expect, it } from 'vitest';

describe('Block Cache', () => {
	it('should disable block cache', () =>
		dbRunner({ dbOptions: [{ noBlockCache: true }] }, async ({ db }) => {
			await db.put('foo', 'bar');
			// note get() will find 'foo' in the memtable and return it synchronously
			expect(db.get('foo')).toBe('bar');
		}));

	it('should enable block cache and override default size', () =>
		dbRunner({ skipOpen: true }, async ({ db }) => {
			RocksDatabase.config({ blockCacheSize: 1024 * 1024 });
			db.open();
			await db.put('foo', 'bar');
			expect(db.get('foo')).toBe('bar');
		}));

	it('should change the block cache size', () =>
		dbRunner({ dbOptions: [{ noBlockCache: true }], skipOpen: true }, async ({ db }) => {
			RocksDatabase.config({ blockCacheSize: 1024 * 1024 });

			db.open();
			await db.put('foo', 'bar');
			expect(db.get('foo')).toBe('bar');

			RocksDatabase.config({ blockCacheSize: 2048 * 1024 });
			expect(db.get('foo')).toBe('bar');

			RocksDatabase.config({ blockCacheSize: 0 });
			expect(db.get('foo')).toBe('bar');
		}));

	it('should throw error when block cache size is negative', () => {
		expect(() => RocksDatabase.config({ blockCacheSize: -1 })).toThrowError(
			'Block cache size must be a positive integer or 0 to disable caching'
		);
	});
});
