import { RocksDatabase } from '../src/index.ts';
import { Store, type StoreContext, type StorePutOptions } from '../src/store.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { rm } from 'node:fs/promises';
import type { Key } from 'ordered-binary';
import { describe, expect, it } from 'vitest';

describe('Custom Store', () => {
	it('should use a custom store', async () => {
		class CustomStore extends Store {
			putCalled = false;

			put(context: StoreContext, key: Key, value: any, options?: StorePutOptions) {
				this.putCalled = true;
				return super.putSync(context, key, value, options);
			}
		}

		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			const store = new CustomStore(dbPath);
			db = RocksDatabase.open(store);
			await db.put('foo', 'bar');
			expect(await db.get('foo')).toBe('bar');
		} finally {
			db?.close();
			await rm(dbPath, { force: true, recursive: true, maxRetries: 3 });
		}
	});

	it('should not allow a store to be used by another RocksDatabase instance', () =>
		dbRunner(async ({ db }) => {
			await db.put('foo', 'bar');
			expect(() => new RocksDatabase(db.store)).toThrow(
				'Store is already in use by another RocksDatabase instance'
			);
		}));

	it('should reject a reused store even after its owner temporarily closed', () =>
		dbRunner(async ({ db }) => {
			// The claim is durable: closing the owner does not release the store.
			db.close();
			expect(() => new RocksDatabase(db.store)).toThrow(
				'Store is already in use by another RocksDatabase instance'
			);
			db.open();
		}));
});
