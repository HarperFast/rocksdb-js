import { RocksDatabase } from '../src/database.ts';
import { VALUE_BUFFER } from '../src/store.ts';
import type { Transaction } from '../src/transaction.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { spawnSync } from 'node:child_process';
import { rmSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

/**
 * Writes `count` records, flushes them into SSTs and reopens the database, so the block cache is
 * cold and a batch that reads them has to take the async path.
 */
async function seedColdDatabase(
	count: number,
	options?: { pessimistic?: boolean }
): Promise<{ db: RocksDatabase; other: RocksDatabase; path: string }> {
	const path = generateDBPath();
	let db = RocksDatabase.open(path, options);
	let other = RocksDatabase.open(path, { ...options, name: 'other' });
	for (let i = 0; i < count; i++) {
		await db.put(`key-${i}`, { i, label: `value-${i}` });
		await other.put(`other-${i}`, `other-value-${i}`);
	}
	await db.flush();
	await other.flush();
	other.close();
	db.close();
	db = RocksDatabase.open(path, options);
	other = RocksDatabase.open(path, { ...options, name: 'other' });
	return { db, other, path };
}

function closeAndRemove({
	db,
	other,
	path,
}: {
	db: RocksDatabase;
	other: RocksDatabase;
	path: string;
}) {
	other.close();
	db.close();
	if (!process.env.KEEP_FILES) {
		rmSync(path, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
	}
}

describe('getMany()', () => {
	for (const mode of [
		'optimistic',
		'pessimistic',
		'no-snapshot',
		'reset',
		'commit',
		'commit-sync',
	]) {
		for (const route of ['direct', 'routed']) {
			it(`preserves a pending ${route} batch across transaction mutation (${mode})`, () => {
				const path = generateDBPath();
				try {
					const result = spawnSync(
						process.execPath,
						[
							fileURLToPath(
								new URL('./fixtures/transaction-get-many-mutation.mts', import.meta.url)
							),
							path,
							mode,
							route,
						],
						{
							env: { ...process.env, ROCKSDB_JS_TXN_GET_DELAY_MS: '200' },
							encoding: 'utf8',
							timeout: 15_000,
						}
					);
					expect(result.error).toBeUndefined();
					expect(result.status, result.stderr).toBe(0);
				} finally {
					rmSync(path, { recursive: true, force: true });
				}
			});
		}
	}

	it('rejects decoding errors for cached and cold batches', () =>
		dbRunner(async ({ db }) => {
			await db.put('a', 'value');
			await db.put('b', 'value');
			const decoder = db.store.decoder;
			db.store.decoder = {
				decode() {
					throw new Error('decode failed');
				},
			};
			try {
				await expect(db.getMany(['a', 'b'])).rejects.toThrow('decode failed');
				expect(() => db.getManySync(['a', 'b'])).toThrow('decode failed');
				await db.flush();
				db.close();
				db.open();
				db.store.decoder = {
					decode() {
						throw new Error('decode failed');
					},
				};
				await expect(db.getMany(['a', 'b'])).rejects.toThrow('decode failed');
			} finally {
				db.store.decoder = decoder;
			}
		}));

	it('isolates values retained by a decoder that needs stable buffers', () =>
		dbRunner(
			{
				dbOptions: [
					{
						encoder: { encode: (value: Buffer) => value, copyBuffers: true },
						decoder: {
							needsStableBuffer: true,
							decode: (value: Buffer) => value.subarray(0, 1),
						},
					},
				],
			},
			async ({ db }) => {
				await db.put('small', Buffer.alloc(16, 1));
				await db.put('other', Buffer.alloc(16, 3));
				await db.put('large', Buffer.alloc(100_000, 2));
				await db.flush();
				db.close();
				db.open();
				const cold = db.getMany(['small', 'large']);
				expect(cold).toBeInstanceOf(Promise);
				for (const values of [
					await cold,
					db.getManySync(['small', 'large']),
					await db.getMany(['small', 'large']),
				]) {
					expect(values[0]).toEqual(Buffer.from([1]));
					expect(values[0].buffer).not.toBe(values[1].buffer);
					expect(values[0].buffer.byteLength).toBeLessThan(100_000);
				}
				// This batch fits the shared value buffer, which the next read overwrites.
				const shared = db.getManySync(['small', 'other']);
				db.getSync('other');
				expect(shared).toEqual([Buffer.from([1]), Buffer.from([3])]);
			}
		));

	it('decodes a batch that fits from the shared value buffer, as get() does', () =>
		dbRunner(async ({ db }) => {
			await db.put('a', 'first');
			await db.put('b', 'second');
			await db.put('large', 'v'.repeat(70_000));
			const decoder = db.store.decoder!;
			const sources: ArrayBufferLike[] = [];
			db.store.decoder = {
				decode(buffer, options) {
					sources.push(buffer.buffer);
					return decoder.decode!(buffer, options);
				},
			};
			try {
				expect(db.getManySync(['a', 'b'])).toEqual(['first', 'second']);
				expect(sources).toEqual([VALUE_BUFFER.buffer, VALUE_BUFFER.buffer]);
				sources.length = 0;
				expect(db.getManySync(['a', 'large'])).toEqual(['first', 'v'.repeat(70_000)]);
				expect(sources[0]).not.toBe(VALUE_BUFFER.buffer);
			} finally {
				db.store.decoder = decoder;
			}
		}));

	it('rejects a batch whose decoder reads the database before the batch is decoded', () =>
		dbRunner(async ({ db }) => {
			await db.put('a', 'first');
			await db.put('b', 'second');
			const decoder = db.store.decoder!;
			let reading = false;
			db.store.decoder = {
				decode(buffer, options) {
					const value = decoder.decode!(buffer, options);
					if (!reading) {
						reading = true;
						db.getSync('b');
						reading = false;
					}
					return value;
				},
			};
			try {
				expect(() => db.getManySync(['a', 'b'])).toThrow('decoder read the database');
				await expect(db.getMany(['a', 'b'])).rejects.toThrow('decoder read the database');
			} finally {
				db.store.decoder = decoder;
			}
		}));

	it('returns decoded values in key order, with undefined for missing and duplicate keys', () =>
		dbRunner(async ({ db }) => {
			await db.put('a', { x: 1 });
			await db.put('b', 'hello');
			await db.put(5, [1, 2, 3]);
			const keys = ['a', 'missing', 'b', 5, 'a'];
			const expected = [{ x: 1 }, undefined, 'hello', [1, 2, 3], { x: 1 }];
			expect(db.getManySync(keys)).toEqual(expected);
			const result = db.getMany(keys);
			expect(result).not.toBeInstanceOf(Promise);
			expect(result).toEqual(expected);
		}));

	it('handles an empty batch and a batch of one', () =>
		dbRunner(async ({ db }) => {
			await db.put('only', 'value');
			expect(db.getManySync([])).toEqual([]);
			expect(await db.getMany([])).toEqual([]);
			expect(db.getManySync(['only'])).toEqual(['value']);
			expect(db.getManySync(['nope'])).toEqual([undefined]);
			expect(await db.getMany(['only'])).toEqual(['value']);
		}));

	it('reads the batch with one promise when the block cache is cold, then synchronously', async () => {
		const seeded = await seedColdDatabase(50);
		const { db } = seeded;
		try {
			const keys = Array.from({ length: 50 }, (_, i) => `key-${i}`);
			const expected = keys.map((_, i) => ({ i, label: `value-${i}` }));
			const result = db.getMany([...keys, 'missing']);
			expect(result).toBeInstanceOf(Promise);
			expect(await result).toEqual([...expected, undefined]);
			const cached = db.getMany(keys);
			expect(cached).not.toBeInstanceOf(Promise);
			expect(cached).toEqual(expected);
		} finally {
			closeAndRemove(seeded);
		}
	});

	it('matches getSync() for a cold batch read synchronously', async () => {
		const seeded = await seedColdDatabase(20);
		const { db } = seeded;
		try {
			const keys = ['key-3', 'key-19', 'nope', 'key-3', 'key-0'];
			expect(db.getManySync(keys)).toEqual(keys.map((key) => db.getSync(key)));
		} finally {
			closeAndRemove(seeded);
		}
	});

	it('returns binary values that stay valid after later reads', () =>
		dbRunner(async ({ db }) => {
			await db.put('a', 'first');
			await db.put('b', 'second');
			const sync = db.getManyBinarySync(['a', 'missing', 'b']);
			const async = await db.getManyBinary(['b', 'a']);
			db.getSync('a');
			db.getManySync(['b', 'a']);
			expect(sync[0]).toBeInstanceOf(Buffer);
			expect(sync[1]).toBeUndefined();
			expect(db.store.decodeValue(sync[0] as any)).toBe('first');
			expect(db.store.decodeValue(sync[2] as any)).toBe('second');
			expect(db.store.decodeValue(async[0] as any)).toBe('second');
			expect(db.store.decodeValue(async[1] as any)).toBe('first');
		}));

	it('returns values of a binary-encoded database that stay valid after later reads', () =>
		dbRunner({ dbOptions: [{ encoding: 'binary' }] }, async ({ db }) => {
			await db.put('a', Buffer.from('first'));
			await db.put('b', Buffer.from('second'));
			const sync = db.getManySync(['a', 'b']);
			const cached = await db.getMany(['b', 'a']);
			db.getSync('b');
			db.getManySync(['b', 'a']);
			expect(sync).toEqual([Buffer.from('first'), Buffer.from('second')]);
			expect(cached).toEqual([Buffer.from('second'), Buffer.from('first')]);
		}));

	it('reads large batches, keys and values', () =>
		dbRunner(async ({ db }) => {
			// ~300 KB of keys outgrows the initial batch buffer; values cross the 2 KB blob threshold
			const keys = Array.from({ length: 1000 }, (_, i) => `${'k'.repeat(300)}-${i}`);
			const value = 'v'.repeat(5000);
			await db.transaction((txn) => {
				for (const key of keys) {
					txn.putSync(key, `${value}-${key.length}`);
				}
			});
			await db.flush();
			const expected = keys.map((key) => `${value}-${key.length}`);
			expect(db.getManySync(keys)).toEqual(expected);
			expect(await db.getMany(keys)).toEqual(expected);
		}));

	it('supports every key encoding', async () => {
		for (const keyEncoding of ['uint32', 'binary', 'ordered-binary'] as const) {
			await dbRunner({ dbOptions: [{ keyEncoding }] }, async ({ db }) => {
				const keys =
					keyEncoding === 'uint32'
						? [1, 2, 3]
						: keyEncoding === 'binary'
							? [Buffer.from('x'), Buffer.from('y'), Buffer.from('z')]
							: [['a', 1], 'b', 42];
				await db.put(keys[0], 'one');
				await db.put(keys[2], 'three');
				expect(db.getManySync(keys)).toEqual(['one', undefined, 'three']);
			});
		}
	});

	it('supports every value encoding', async () => {
		for (const encoding of ['msgpack', 'ordered-binary', 'binary'] as const) {
			await dbRunner({ dbOptions: [{ encoding }] }, async ({ db }) => {
				const values =
					encoding === 'binary'
						? [Buffer.from('one'), Buffer.from('three')]
						: [
								['a', 1.1],
								['b', 2.5, 'c'],
							];
				await db.put('k1', values[0]);
				await db.put('k3', values[1]);
				const keys = ['k1', 'k2', 'k3'];
				const expected = keys.map((key) => db.getSync(key));
				expect(expected).toEqual([values[0], undefined, values[1]]);
				expect(db.getManySync(keys)).toEqual(expected);
				await db.flush();
				expect(await db.getMany(keys)).toEqual(expected);
			});
		}
	});

	it('rejects an undefined key', () =>
		dbRunner(async ({ db }) => {
			expect(() => db.getManySync(['a', undefined as any])).toThrow('Key is required');
			await expect(db.getMany(['a', undefined as any])).rejects.toThrow('Key is required');
		}));

	it('reads a read-only database', async () => {
		const seeded = await seedColdDatabase(10);
		const { db, path } = seeded;
		const reader = RocksDatabase.open(path, { readOnly: true });
		try {
			const keys = ['key-0', 'missing', 'key-9'];
			const expected = [{ i: 0, label: 'value-0' }, undefined, { i: 9, label: 'value-9' }];
			expect(reader.getManySync(keys)).toEqual(expected);
			expect(await reader.getMany(keys)).toEqual(expected);
			expect(db.getManySync(keys)).toEqual(expected);
		} finally {
			reader.close();
			closeAndRemove(seeded);
		}
	});

	it('errors if the database is not open', () =>
		dbRunner({ skipOpen: true }, async ({ db }) => {
			expect(() => db.getManySync(['a', 'b'])).toThrow('Database not open');
			await expect(db.getMany(['a', 'b'])).rejects.toThrow('Database not open');
		}));

	it('settles a pending batch when the database closes', async () => {
		const seeded = await seedColdDatabase(50);
		const { db } = seeded;
		try {
			const pending = db.getMany(Array.from({ length: 50 }, (_, i) => `key-${i}`));
			expect(pending).toBeInstanceOf(Promise);
			db.close();
			const outcome = await (pending as Promise<unknown[]>).then(
				(values) => values.length,
				(error: Error) => error.message
			);
			// Either the read finished before the close, or the worker saw the close and aborted.
			if (typeof outcome === 'number') {
				expect(outcome).toBe(50);
			} else {
				expect(outcome).toMatch(/Database closed during get many operation/);
			}
		} finally {
			closeAndRemove(seeded);
		}
	});
});

for (const { name, options } of [
	{ name: 'optimistic', options: {} },
	{ name: 'pessimistic', options: { pessimistic: true } },
]) {
	describe(`getMany() in a transaction (${name})`, () => {
		it('returns exactly what get() does, including uncommitted writes, deletes and duplicates', () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await db.put('kept', 'committed');
				await db.put('deleted', 'committed');
				await db.put('changed', 'old');
				await db.transaction(async (txn: Transaction) => {
					txn.putSync('changed', 'new');
					txn.putSync('added', 'staged');
					txn.removeSync('deleted');
					const keys = ['kept', 'deleted', 'changed', 'added', 'missing', 'changed'];
					const expected = keys.map((key) => txn.getSync(key));
					expect(expected).toEqual(['committed', undefined, 'new', 'staged', undefined, 'new']);
					expect(txn.getManySync(keys)).toEqual(expected);
					expect(await txn.getMany(keys)).toEqual(expected);
					expect(db.getManySync(keys, { transaction: txn })).toEqual(expected);
					expect(await db.getMany(keys, { transaction: txn })).toEqual(expected);
				});
			}));

		it('reads from the transaction snapshot', () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await db.put('a', 1);
				await db.put('b', 1);
				await db.transaction(async (txn: Transaction) => {
					expect(txn.getManySync(['a', 'b'])).toEqual([1, 1]);
					await db.put('a', 2);
					expect(txn.getManySync(['a', 'b'])).toEqual([1, 1]);
					expect(await txn.getMany(['b', 'a'])).toEqual([1, 1]);
				});
				expect(db.getManySync(['a', 'b'])).toEqual([2, 1]);
			}));

		it('reads a cold batch with one promise, through the transaction and another column family', async () => {
			const seeded = await seedColdDatabase(30, options);
			const { db, other } = seeded;
			try {
				await db.transaction(async (txn: Transaction) => {
					txn.putSync('key-1', 'staged');
					const keys = ['key-0', 'key-1', 'key-29', 'missing'];
					const pending = txn.getMany(keys);
					expect(pending).toBeInstanceOf(Promise);
					expect(await pending).toEqual([
						{ i: 0, label: 'value-0' },
						'staged',
						{ i: 29, label: 'value-29' },
						undefined,
					]);
					// the transaction was created on `db`; this batch reads `other`'s column family
					const otherKeys = ['other-0', 'key-0', 'other-29'];
					const otherPending = other.getMany(otherKeys, { transaction: txn });
					expect(otherPending).toBeInstanceOf(Promise);
					expect(await otherPending).toEqual(['other-value-0', undefined, 'other-value-29']);
				});
			} finally {
				closeAndRemove(seeded);
			}
		});

		it('errors once the transaction has committed', () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				let committed: Transaction | undefined;
				await db.transaction((txn: Transaction) => {
					committed = txn;
					txn.putSync('a', 1);
				});
				expect(() => committed!.getManySync(['a', 'b'])).toThrow('Transaction is closed');
				await expect(committed!.getMany(['a', 'b'])).rejects.toThrow('Transaction is closed');
				// one key takes the single-key path, which used to dereference the closed handle
				expect(() => committed!.getManySync(['a'])).toThrow('Transaction is closed');
				expect(() => committed!.getSync('a')).toThrow('Transaction is closed');
				await expect(committed!.getMany(['a'])).rejects.toThrow('Transaction is closed');
			}));
	});
}
