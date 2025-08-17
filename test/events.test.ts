import { describe, expect, it, vi } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import { setTimeout as delay } from 'node:timers/promises';
import { withResolvers } from '../src/util.js';

describe('Events', () => {
	it('should add an event listener and emit an event', async () => {
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

			db.addEventListener('foo', () => {
				spy();
				resolvers[0].resolve();
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
			db.addEventListener('foo', callback2);

			expect(db.emit('foo')).toBe(true);
			await Promise.all(resolvers.map(r => r.promise));
			expect(spy).toHaveBeenCalledTimes(2);
			expect(spy2).toHaveBeenCalledTimes(1);

			// remove listener
			expect(db.removeEventListener('foo', callback2)).toBe(true);

			resolvers = [
				withResolvers(),
			];
			expect(db.emit('foo')).toBe(true);
			await Promise.all(resolvers.map(r => r.promise));
			expect(spy).toHaveBeenCalledTimes(3);
			expect(spy2).toHaveBeenCalledTimes(1);
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should error if database is not open', async () => {
		const dbPath = generateDBPath();
		let db: RocksDatabase | null = null;

		try {
			db = new RocksDatabase(dbPath);
			expect(() => db!.addEventListener('foo', () => {})).toThrow('Database not open');
			expect(() => db!.emit('foo')).toThrow('Database not open');
			expect(() => db!.removeEventListener('foo', () => {})).toThrow('Database not open');
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
			expect(() => db!.addEventListener('foo', 'foo' as any))
				.toThrow('Callback must be a function');
			expect(() => db!.removeEventListener('foo', 'foo' as any))
				.toThrow('Callback must be a function');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});
});
