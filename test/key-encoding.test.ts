import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';

describe('Key Encoding', () => {
	describe('uint32', () => {
		it('should encode key with string using uint32', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, {
					keyEncoding: 'uint32'
				});

				for (let i = 0; i < 10; i++) {
					await db.put(i, `value-${i}`);
				}

				for (let i = 0; i < 10; i++) {
					const value = await db.get(i);
					expect(value).toBe(`value-${i}`);
				}
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it.skip('should read and write a range', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, {
					keyEncoding: 'uint32'
				});

				let i;
				for (i = 0; i < 10; i++) {
					await db.put(i, `value-${i}`);
				}

				// i = 0;
				// for (let { key, value } of db.getRange()) {
				// 	expect(key).toBe(i);
				// 	expect(value).toBe(`value-${i}`);
				// 	i++;
				// }

				// i = 0;
				// for (let { key, value } of db.getRange({ start: 0 })) {
				// 	expect(key).toBe(i);
				// 	expect(value).toBe(`value-${i}`);
				// 	i++;
				// }
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error if key is not a number', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, {
					keyEncoding: 'uint32'
				});
				await expect(db.put('foo', 'bar')).rejects.toThrow('Key is not a number');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	describe('binary', () => {
		it('should encode key with string using binary', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, {
					keyEncoding: 'binary'
				});
				await db.put('foo', 'bar');

				const value = await db.get('foo');
				expect(value).toBe('bar');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	describe('ordered-binary', () => {
		it('should encode key with string using ordered-binary', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, {
					keyEncoding: 'ordered-binary'
				});
				await db.put('foo', 'bar');

				const value = await db.get('foo');
				expect(value).toBe('bar');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	// mixed keys

	it('should error if key encoding is not supported', async () => {
		await expect(RocksDatabase.open(generateDBPath(), {
			keyEncoding: 'foo'
		} as any)).rejects.toThrow('Invalid key encoding: foo');
	});
});
