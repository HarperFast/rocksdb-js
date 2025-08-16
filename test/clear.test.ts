import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';

describe('Clear', () => {
	describe('clear()', () => {
		it('should error if database is not open', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = new RocksDatabase(dbPath);
				await expect(db.clear()).rejects.toThrow('Database not open');
			} finally {
				await rimraf(dbPath);
			}
		});

		it('should clear all data in a database with default batch size', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				for (let i = 0; i < 1000; ++i) {
					db.putSync(`foo-${i}`, `bar-${i}`);
				}
				expect(db.getSync('foo-0')).toBe('bar-0');
				await expect(db.clear()).resolves.toBe(1000);
				expect(db.getSync('foo-0')).toBeUndefined();
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should clear a database with no data', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				await expect(db.clear()).resolves.toBe(0);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should clear all data in a database using single small batch', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				for (let i = 0; i < 10; ++i) {
					db.putSync(`foo-${i}`, `bar-${i}`);
				}
				expect(db.getSync('foo-0')).toBe('bar-0');
				await expect(db.clear({ batchSize: 100 })).resolves.toBe(10);
				expect(db.getSync('foo-0')).toBeUndefined();
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should clear all data in a database using multiple small batches', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				for (let i = 0; i < 987; ++i) {
					db.putSync(`foo-${i}`, `bar-${i}`);
				}
				expect(db.getSync('foo-0')).toBe('bar-0');
				await expect(db.clear({ batchSize: 100 })).resolves.toBe(987);
				expect(db.getSync('foo-0')).toBeUndefined();
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should cancel clear if database closed while clearing', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				// note: this test can be flaky because sometimes it can clear
				// faster than the close
				for (let i = 0; i < 100000; ++i) {
					db.putSync(`foo-${i}`, `bar-${i}`);
				}
				const promise = db.clear({ batchSize: 10 });
				db.close();
				await expect(promise).rejects.toThrow('Database closed during clear operation');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	describe('clearSync()', () => {
		it('should error if database is not open', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = new RocksDatabase(dbPath);
				expect(() => db!.clearSync()).toThrow('Database not open');
			} finally {
				await rimraf(dbPath);
			}
		});

		it('should clear all data in a database with default batch size', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				for (let i = 0; i < 1000; ++i) {
					db.putSync(`foo-${i}`, `bar-${i}`);
				}
				expect(db.getSync('foo-0')).toBe('bar-0');
				expect(db.clearSync()).toBe(1000);
				expect(db.getSync('foo-0')).toBeUndefined();
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should clear a database with no data', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				expect(db.clearSync()).toBe(0);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should clear all data in a database using single small batch', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				for (let i = 0; i < 10; ++i) {
					db.putSync(`foo-${i}`, `bar-${i}`);
				}
				expect(db.getSync('foo-0')).toBe('bar-0');
				expect(db.clearSync({ batchSize: 100 })).toBe(10);
				expect(db.getSync('foo-0')).toBeUndefined();
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should clear all data in a database using multiple small batches', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			try {
				db = RocksDatabase.open(dbPath);
				for (let i = 0; i < 987; ++i) {
					db.putSync(`foo-${i}`, `bar-${i}`);
				}
				expect(db.getSync('foo-0')).toBe('bar-0');
				expect(db.clearSync({ batchSize: 100 })).toBe(987);
				expect(db.getSync('foo-0')).toBeUndefined();
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});
});
