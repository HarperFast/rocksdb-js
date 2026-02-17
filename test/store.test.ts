import { RocksDatabase } from '../src/index.js';
import { Store, StoreContext, type StorePutOptions } from '../src/store.js';
import { generateDBPath } from './lib/util.js';
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
});
