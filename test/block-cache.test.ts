import { RocksDatabase } from '../src/index.ts';
import { dbRunner } from './lib/util.ts';
import { assert, describe, expect, it } from 'vitest';

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
			assert.equal(db.getStats()['rocksdb.block-cache-capacity'], 1024 * 1024);
			await db.put('foo', 'bar');
			expect(db.get('foo')).toBe('bar');
		}));

	it('should enable block cache and override default size for non-default CF', () =>
		dbRunner({ skipOpen: true, dbOptions: [{ name: 'nonDefault' }] }, async ({ db }) => {
			RocksDatabase.config({ blockCacheSize: 1024 * 1024 });
			db.open();
			assert.equal(db.getStats()['rocksdb.block-cache-capacity'], 1024 * 1024);
			await db.put('foo', 'bar');
			expect(db.get('foo')).toBe('bar');
		}));

	it('should apply noBlockCache to a late-created column family', () =>
		dbRunner({ dbOptions: [{}, { name: 'late', noBlockCache: true }] }, async (_, { db }) => {
			await db.put('foo', 'bar');
			await db.flush();

			const firstRead = db.get('foo');
			expect(firstRead).toBeInstanceOf(Promise);
			expect(await firstRead).toBe('bar');

			// Await it: an unawaited read still in flight when dbRunner closes the
			// database is aborted, and the rejection fails the run.
			const secondRead = db.get('foo');
			expect(secondRead).toBeInstanceOf(Promise);
			expect(await secondRead).toBe('bar');
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
		expect(() => RocksDatabase.config({ blockCacheSize: -1 })).toThrow(
			new RangeError('Block cache size must be a positive integer or 0 to disable caching')
		);
	});
});
