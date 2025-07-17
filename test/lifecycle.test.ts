import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';

describe('Lifecycle', () => {
	it('should open and close database', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = new RocksDatabase(dbPath);

			expect(db.isOpen()).toBe(false);

			db.close(); // noop

			db.open();
			db.open(); // noop

			expect(db.isOpen()).toBe(true);
			expect(db.get('foo')).toBeUndefined();

			db.close();

			expect(db.isOpen()).toBe(false);

			await expect(db.get('foo')).rejects.toThrow('Database not open');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should open and close multiple databases', async () => {
		let db: RocksDatabase | null = null;
		let db2: RocksDatabase | null = null;
		let db3: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = new RocksDatabase(dbPath);
			db2 = new RocksDatabase(dbPath, { name: 'foo' });
			db3 = new RocksDatabase(dbPath, { name: 'foo' });

			expect(db.isOpen()).toBe(false);
			expect(db2.isOpen()).toBe(false);
			expect(db3.isOpen()).toBe(false);

			db.close(); // noop
			db2.close(); // noop
			db3.close(); // noop

			db.open();
			db.open(); // noop

			db2.open();
			db2.open(); // noop

			db3.open();
			db3.open(); // noop

			expect(db.isOpen()).toBe(true);
			expect(db.get('foo')).toBeUndefined();

			expect(db2.isOpen()).toBe(true);
			expect(db2.get('foo')).toBeUndefined();

			expect(db3.isOpen()).toBe(true);
			expect(db3.get('foo')).toBeUndefined();

			db.close();
			db2.close();
			db3.close();

			expect(db.isOpen()).toBe(false);
			expect(db2.isOpen()).toBe(false);
			expect(db3.isOpen()).toBe(false);

			await expect(db.get('foo')).rejects.toThrow('Database not open');
			await expect(db2.get('foo')).rejects.toThrow('Database not open');
			await expect(db3.get('foo')).rejects.toThrow('Database not open');
		} finally {
			db?.close();
			db2?.close();
			await rimraf(dbPath);
		}
	});
});
