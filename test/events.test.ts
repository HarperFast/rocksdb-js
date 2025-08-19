import { describe, expect, it, vi } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import { withResolvers } from '../src/util.js';

describe('Events', () => {
	it('should emit to listeners', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = RocksDatabase.open(dbPath);
			const spy = vi.fn();
			const spy2 = vi.fn();

			expect(db.emit('foo')).toBe(false); // noop

			let resolvers = [
				withResolvers(),
			];

			db.addListener('foo', value => {
				spy();
				resolvers[0].resolve(value);
			});

			expect(db.emit('foo')).toBe(true);
			await Promise.all(resolvers.map(r => r.promise));
			expect(spy).toHaveBeenCalledTimes(1);

			resolvers = [
				withResolvers(),
				withResolvers(),
			];

			// add second listener
			const callback2 = () => {
				spy2();
				resolvers[1].resolve();
			};
			db.addListener('foo', callback2);

			expect(db.emit('foo')).toBe(true);
			await Promise.all(resolvers.map(r => r.promise));
			expect(spy).toHaveBeenCalledTimes(2);
			expect(spy2).toHaveBeenCalledTimes(1);

			// remove listener
			expect(db.removeListener('foo', callback2)).toBe(true);

			resolvers = [
				withResolvers(),
			];
			expect(db.emit('foo', 'bar')).toBe(true);
			await expect(Promise.all(resolvers.map(r => r.promise))).resolves.toEqual(['bar']);
			expect(spy).toHaveBeenCalledTimes(3);
			expect(spy2).toHaveBeenCalledTimes(1);
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it.only('should bound events to database instance', async () => {
		let db: RocksDatabase | null = null;
		let db2: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = RocksDatabase.open(dbPath);
			db2 = RocksDatabase.open(dbPath);

			const spy = vi.fn();
			const spy2 = vi.fn();

			db.addListener('foo', () => spy());
			db2.addListener('foo', () => spy2());

			// db.emit('foo');

			// expect(spy).toHaveBeenCalledTimes(1);
			// expect(spy2).toHaveBeenCalledTimes(0);

			// db2.emit('foo');

			// expect(spy).toHaveBeenCalledTimes(1);
			// expect(spy2).toHaveBeenCalledTimes(1);
		} finally {
			db?.close();
			db2?.close();
			await rimraf(dbPath);
		}
	});

	it('should error if database is not open', async () => {
		const dbPath = generateDBPath();
		let db: RocksDatabase | null = null;

		try {
			db = new RocksDatabase(dbPath);
			expect(() => db!.addListener('foo', () => {})).toThrow('Database not open');
			expect(() => db!.emit('foo')).toThrow('Database not open');
			expect(() => db!.removeListener('foo', () => {})).toThrow('Database not open');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should error if callback is not a function', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = RocksDatabase.open(dbPath);
			expect(() => db!.addListener('foo', 'foo' as any))
				.toThrow('Callback must be a function');
			expect(() => db!.removeListener('foo', 'foo' as any))
				.toThrow('Callback must be a function');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});
});
