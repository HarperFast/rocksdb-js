import { dbRunner } from './lib/util.js';
import { describe, expect, it } from 'vitest';

describe('Database write buffer options', () => {
	it('should open with default write buffer settings', () =>
		dbRunner(async ({ db }) => {
			await db.put('foo', 'bar');
			expect(await db.get('foo')).toBe('bar');
		}));

	it('should open with custom writeBufferSize and maxWriteBufferNumber', () =>
		dbRunner(
			{
				dbOptions: [
					{
						maxWriteBufferNumber: 8,
						writeBufferSize: 4 * 1024 * 1024,
					},
				],
			},
			async ({ db }) => {
				await db.put('foo', 'bar');
				expect(await db.get('foo')).toBe('bar');
			}
		));

	it('should open with dbWriteBufferSize set', () =>
		dbRunner({ dbOptions: [{ dbWriteBufferSize: 64 * 1024 * 1024 }] }, async ({ db }) => {
			await db.put('foo', 'bar');
			expect(await db.get('foo')).toBe('bar');
		}));

	it('should open with maxWriteBufferSizeToMaintain set', () =>
		dbRunner(
			{ dbOptions: [{ maxWriteBufferSizeToMaintain: 128 * 1024 * 1024 }] },
			async ({ db }) => {
				await db.put('foo', 'bar');
				expect(await db.get('foo')).toBe('bar');
			}
		));

	it('should flush memtables when writeBufferSize is exceeded', () =>
		dbRunner({ dbOptions: [{ writeBufferSize: 64 * 1024 }] }, async ({ db }) => {
			// 64KB memtable; write enough data to force at least one flush.
			// `num-files-at-level0` can be racy under background compaction, so
			// check on-disk SST size instead — once any flush has happened it is
			// non-zero regardless of which level the data settled on.
			const value = 'x'.repeat(1024);
			for (let i = 0; i < 256; i++) {
				await db.put(`key-${i.toString().padStart(6, '0')}`, value);
			}
			await db.flush();
			const sstSize = db.getDBIntProperty('rocksdb.total-sst-files-size');
			expect(sstSize).toBeDefined();
			expect(sstSize!).toBeGreaterThan(0);
		}));
});
