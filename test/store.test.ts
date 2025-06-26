import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { Store, type PutOptions } from '../src/store.js';
import { generateDBPath } from './lib/util.js';
import type { NativeDatabase, NativeTransaction } from '../src/load-binding.js';
import type { Key } from 'ordered-binary';
import type { DBITransactional } from '../src/dbi.js';

describe('Custom Store', () => {
	it('should use a custom store', async () => {
		class CustomStore extends Store {
			putCalled = false;

			put(context: NativeDatabase | NativeTransaction, key: Key, value: any, options?: PutOptions & DBITransactional) {
				this.putCalled = true;
				return super.putSync(context, key, value, options);
			}
		}

		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			const store = new CustomStore(dbPath);
			db = await RocksDatabase.open(dbPath, { store });
			await db.put('foo', 'bar');
			expect(await db.get('foo')).toBe('bar');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});
});
