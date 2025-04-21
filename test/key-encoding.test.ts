import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';

describe('Key Encoding', () => {
	// describe('uint32', () => {
	// });

	// describe('binary', () => {
	// });

	describe('ordered-binary', () => {
		it('should encode key with string', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				const key = 'foo';

				db = await RocksDatabase.open(dbPath, {
					keyEncoding: 'ordered-binary'
				});
				await db.put(key, 'bar');

				const value = await db.get(key);
				expect(value.toString()).toBe('bar');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	// it('should encode key with uint32', async () => {
	// 	let db: RocksDatabase | null = null;
	// 	const dbPath = generateDBPath();

	// 	try {
	// 		const key = 1234;

	// 		db = await RocksDatabase.open(dbPath);
	// 		db.put(key, 'bar');

	// 		await expect(db.get(key)).resolves.toBe('bar');
	// 	} finally {
	// 		db?.close();
	// 		await rimraf(dbPath);
	// 	}
	// });

	// it('should encode key with uint64', async () => {
	// 	let db: RocksDatabase | null = null;
	// 	const dbPath = generateDBPath();

	// 	try {
	// 		const key = 1234567890;

	// 		db = await RocksDatabase.open(dbPath);
	// 		db.put(key, 'bar');

	// 		await expect(db.get(key)).resolves.toBe('bar');
	// 	} finally {
	// 		db?.close();
	// 		await rimraf(dbPath);
	// 	}
	// });

	// it('should encode key with uint64', async () => {
	// 	let db: RocksDatabase | null = null;
	// 	const dbPath = generateDBPath();

	// 	try {
	// 		const key = 1234567890;

	// 		db = await RocksDatabase.open(dbPath);
	// 		db.put(key, 'bar');

	// 		await expect(db.get(key)).resolves.toBe('bar');
	// 	} finally {
	// 		db?.close();
	// 		await rimraf(dbPath);
	// 	}
	// });
});
