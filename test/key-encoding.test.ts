import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import type { Key } from '../src/encoding.js';

describe('Key Encoding', () => {
	describe('uint32', () => {
		it('should encode key using uint32', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

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
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

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
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

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
		it('should encode key using binary', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

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
		it('should encode key using ordered-binary', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

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

	describe('Custom key encoder', () => {
		it('should encode key with string using custom key encoder', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = await RocksDatabase.open(dbPath, {
					keyEncoder: {
						readKey(key: Buffer) {
							return JSON.parse(key.toString('utf-8'));
						},
						writeKey(key: Key, target: Buffer, start: number) {
							return target.write(JSON.stringify(
								key instanceof Buffer ? key : String(key)
							), start);
						}
					}
				});
				await db.put({ foo: 'bar' } as any, 'baz');
				const value = await db.get({ foo: 'bar' } as any);
				expect(value).toBe('baz');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should throw an error if key has zero length', async () => {
			const dbPath = generateDBPath();
			let db: RocksDatabase | null = null;

			try {
				db = await RocksDatabase.open(dbPath, {
					keyEncoder: {
						readKey: (_key: Buffer) => Buffer.from('foo'),
						writeKey: (_key: Key, _target: Buffer, _start: number) => 0
					}
				});
				await expect(db.get('foo')).rejects.toThrow('Zero length key is not allowed');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error if key encoder is missing readKey or writeKey', async () => {
			const dbPath = generateDBPath();

			try {
				await expect(RocksDatabase.open(dbPath, {
					keyEncoder: {}
				})).rejects.toThrow('Custom key encoder must provide both readKey and writeKey');
			} finally {
				await rimraf(dbPath);
			}
		});
	});

	// mixed keys

	describe('Error handling', () => {
		it('should error if key encoding is not supported', async () => {
			await expect(RocksDatabase.open(generateDBPath(), {
				keyEncoding: 'foo'
			} as any)).rejects.toThrow('Invalid key encoding: foo');
		});
	});
});
