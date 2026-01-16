import { describe, expect, it } from 'vitest';
import type { Key } from '../src/encoding.js';
import { RocksDatabase } from '../src/index.js';
import { dbRunner, generateDBPath } from './lib/util.js';

describe('Key Encoding', () => {
	describe('uint32', () => {
		it('should encode key using uint32', () =>
			dbRunner({ dbOptions: [{ keyEncoding: 'uint32' }] }, async ({ db }) => {
				for (let i = 0; i < 10; i++) {
					await db.put(i, `value-${i}`);
				}

				for (let i = 0; i < 10; i++) {
					const value = await db.get(i);
					expect(value).toBe(`value-${i}`);
				}
			}));

		it.skip('should read and write a range', () =>
			dbRunner({ dbOptions: [{ keyEncoding: 'uint32' }] }, async ({ db }) => {
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
			}));

		it('should error if key is not a number', () =>
			dbRunner({ dbOptions: [{ keyEncoding: 'uint32' }] }, async ({ db }) => {
				await expect(db.put('foo', 'bar')).rejects.toThrow('Key is not a number');
			}));
	});

	describe('binary', () => {
		it('should encode key using binary', () =>
			dbRunner({ dbOptions: [{ keyEncoding: 'binary' }] }, async ({ db }) => {
				await db.put('foo', 'bar');

				const value = await db.get('foo');
				expect(value).toBe('bar');
			}));
	});

	describe('ordered-binary', () => {
		it('should encode key using ordered-binary', () =>
			dbRunner({ dbOptions: [{ keyEncoding: 'ordered-binary' }] }, async ({ db }) => {
				await db.put('foo', 'bar');

				const value = await db.get('foo');
				expect(value).toBe('bar');
			}));
	});

	describe('Custom key encoder', () => {
		it('should encode key with string using custom key encoder', () =>
			dbRunner({
				dbOptions: [{
					keyEncoder: {
						readKey(key: Buffer) {
							return JSON.parse(key.toString('utf-8'));
						},
						writeKey(key: Key, target: Buffer, start: number) {
							return target.write(JSON.stringify(key instanceof Buffer ? key : String(key)), start);
						},
					},
				}],
			}, async ({ db }) => {
				await db.put({ foo: 'bar' } as any, 'baz');
				const value = await db.get({ foo: 'bar' } as any);
				expect(value).toBe('baz');
			}));

		it('should throw an error if key has zero length', () =>
			dbRunner({
				dbOptions: [{
					keyEncoder: {
						readKey: (_key: Buffer) => Buffer.from('foo'),
						writeKey: (_key: Key, _target: Buffer, _start: number) => 0,
					},
				}],
			}, async ({ db }) => {
				await expect(db.get('foo')).rejects.toThrow('Zero length key is not allowed');
			}));

		it('should error if key encoder is missing readKey or writeKey', async () => {
			const dbPath = generateDBPath();

			expect(() => new RocksDatabase(dbPath, { keyEncoder: {} })).toThrow(
				'Custom key encoder must provide both readKey and writeKey'
			);
		});
	});

	// TODO: mixed keys

	describe('Error handling', () => {
		it('should error if key encoding is not supported', () => {
			expect(() => RocksDatabase.open(generateDBPath(), { keyEncoding: 'foo' } as any)).toThrow(
				'Invalid key encoding: foo'
			);
		});
	});
});
