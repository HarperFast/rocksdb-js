import { RocksDatabase } from '../src/index.js';
import { Store, StoreContext, type StorePutOptions } from '../src/store.js';
import { dbRunner, generateDBPath } from './lib/util.js';
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

	it("should use one instance's store for another database", () =>
		dbRunner(async ({ db }) => {
			await db.put('foo', 'bar');

			const db2 = new RocksDatabase(db.store);
			expect(db2.isOpen()).toBe(true);
			expect(await db2.get('foo')).toBe('bar');
		}));
});
