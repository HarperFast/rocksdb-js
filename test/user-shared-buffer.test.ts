import { describe, expect, it, should } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';

describe('User Shared Buffer', () => {
	it('should create a new user shared buffer', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = RocksDatabase.open(dbPath);

			const defaultIncrementer = new BigInt64Array(1);
			const sharedBuffer1 = db.getUserSharedBuffer('incrementer-test', defaultIncrementer.buffer);
			expect(sharedBuffer1).toBeInstanceOf(ArrayBuffer);

			const incrementer = new BigInt64Array(sharedBuffer1);
			incrementer[0] = 4n;
			expect(Atomics.add(incrementer, 0, 1n)).toBe(4n);

			const secondDefaultIncrementer = new BigInt64Array(1); // should not get used
			const sharedBuffer2 = db.getUserSharedBuffer('incrementer-test', secondDefaultIncrementer.buffer);
			expect(sharedBuffer2).toBeInstanceOf(ArrayBuffer);

			const nextIncrementer = new BigInt64Array(sharedBuffer2); // should return same incrementer
			expect(incrementer[0]).toBe(5n);
			expect(Atomics.add(nextIncrementer, 0, 1n)).toBe(5n);
			expect(incrementer[0]).toBe(6n);
			expect(secondDefaultIncrementer[0]).toBe(0n);
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should get next id using shared buffer', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = RocksDatabase.open(dbPath);

			const incrementer = new BigInt64Array(
				db.getUserSharedBuffer('next-id', new BigInt64Array(1).buffer)
			);
			incrementer[0] = 1n;

			const getNextId = () => Atomics.add(incrementer, 0, 1n);

			expect(getNextId()).toBe(1n);
			expect(getNextId()).toBe(2n);
			expect(getNextId()).toBe(3n);
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should throw an error if the default buffer is not an ArrayBuffer', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = RocksDatabase.open(dbPath);
			expect(() => db!.getUserSharedBuffer('incrementer-test', undefined as any))
				.toThrow('Default buffer must be an ArrayBuffer');
			expect(() => db!.getUserSharedBuffer('incrementer-test', 'hello' as any))
				.toThrow('Default buffer must be an ArrayBuffer');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});
});
