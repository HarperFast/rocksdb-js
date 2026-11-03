import { RocksDatabase } from '../src/database.ts';
import type { Transaction } from '../src/transaction.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { rmSync } from 'node:fs';
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
	it('returns decoded values in key order, with undefined for missing and duplicate keys', () =>
		dbRunner(async ({ db }) => {
			await db.put('a', { x: 1 });
			await db.put('b', 'hello');
			await db.put(5, [1, 2, 3]);
			const keys = ['a', 'missing', 'b', 5, 'a'];
			const expected = [{ x: 1 }, undefined, 'hello', [1, 2, 3], { x: 1 }];
			expect(db.getManySync(keys)).toEqual(expected);
			// in the memtable, so the batch is answered synchronously
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
			// the async read filled the block cache
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
			expect([50, 'Database closed during get many operation']).toContain(outcome);
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
				expect(() => committed!.getManySync(['a', 'b'])).toThrow();
			}));
	});
}
