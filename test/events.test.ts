import { describe, expect, it, vi } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import { withResolvers } from '../src/util.js';
import { setTimeout as delay } from 'node:timers/promises';

describe('Events', () => {
	it('should emit to listeners', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = RocksDatabase.open(dbPath);
			const spy = vi.fn();
			const spy2 = vi.fn();

			expect(db.listeners('foo')).toBe(0);
			expect(db.emit('foo')).toBe(false); // noop

			let resolvers = [
				withResolvers(),
			];

			db.addListener('foo', value => {
				spy();
				resolvers[0].resolve(value);
			});

			expect(db.listeners('foo')).toBe(1);
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

			expect(db.listeners('foo')).toBe(2);
			expect(db.emit('foo')).toBe(true);
			await Promise.all(resolvers.map(r => r.promise));
			expect(spy).toHaveBeenCalledTimes(2);
			expect(spy2).toHaveBeenCalledTimes(1);

			// remove listener
			expect(db.removeListener('foo', callback2)).toBe(true);
			expect(db.listeners('foo')).toBe(1);

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

	it('should emit with arguments', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = RocksDatabase.open(dbPath);

			let resolver = withResolvers();
			db.addListener('foo', (...args) => {
				resolver.resolve(args);
			});
			db.emit('foo', 'bar');
			await expect(resolver.promise).resolves.toEqual(['bar']);

			resolver = withResolvers();
			db.emit('foo', 1234);
			await expect(resolver.promise).resolves.toEqual([1234]);

			resolver = withResolvers();
			db.emit('foo', true);
			await expect(resolver.promise).resolves.toEqual([true]);

			resolver = withResolvers();
			db.emit('foo', false);
			await expect(resolver.promise).resolves.toEqual([false]);

			resolver = withResolvers();
			db.emit('foo', null);
			await expect(resolver.promise).resolves.toEqual([null]);

			resolver = withResolvers();
			db.emit('foo', [1, 2, 3]);
			await expect(resolver.promise).resolves.toEqual([[1, 2, 3]]);

			resolver = withResolvers();
			db.emit('foo', { foo: 'bar' });
			await expect(resolver.promise).resolves.toEqual([{ foo: 'bar' }]);

			resolver = withResolvers();
			db.emit('foo', 'bar', 1234, true, false, null, [1, 2, 3], { foo: 'bar' });
			await expect(resolver.promise).resolves.toEqual([
				'bar', 1234, true, false, null, [1, 2, 3], { foo: 'bar' }
			]);
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should bound events to database', async () => {
		let db: RocksDatabase | null = null;
		let db2: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = RocksDatabase.open(dbPath);
			db2 = RocksDatabase.open(dbPath);

			const spy = vi.fn();
			const spy2 = vi.fn();

			let resolvers = [
				withResolvers(),
				withResolvers(),
			];

			const callback = () => {
				spy();
				resolvers[0].resolve();
			};
			const callback2 = () => {
				spy2();
				resolvers[1].resolve();
			};

			db.addListener('foo', callback);
			db2.addListener('foo', callback2);

			db.emit('foo');
			await Promise.race(resolvers.map(r => r.promise));
			expect(spy).toHaveBeenCalledTimes(1);
			expect(spy2).toHaveBeenCalledTimes(0);

			resolvers = [
				withResolvers(),
				withResolvers(),
			];
			db.emit('foo');
			await Promise.race(resolvers.map(r => r.promise));
			expect(spy).toHaveBeenCalledTimes(2);
			expect(spy2).toHaveBeenCalledTimes(0);

			resolvers = [
				withResolvers(),
				withResolvers(),
			];
			db2.emit('foo');
			await Promise.race(resolvers.map(r => r.promise));
			expect(spy).toHaveBeenCalledTimes(2);
			expect(spy2).toHaveBeenCalledTimes(1);

			db.removeListener('foo', callback);
			db2.removeListener('foo', callback2);

			resolvers = [
				withResolvers(),
				withResolvers(),
			];
			db2.emit('foo');
			db2.emit('foo');

			await Promise.race([
				...resolvers.map(r => r.promise.then(() => {
					throw new Error('Expected listeners to not be called');
				})),
				delay(250),
			]);

			expect(spy).toHaveBeenCalledTimes(2);
			expect(spy2).toHaveBeenCalledTimes(1);
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
			expect(() => db!.listeners('foo')).toThrow('Database not open');
			expect(() => db!.removeListener('foo', () => {})).toThrow('Database not open');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should error if event is not a string', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();
		try {
			db = RocksDatabase.open(dbPath);
			expect(() => db!.addListener(123 as any, () => {})).toThrow('Event is required');
			expect(() => db!.emit(123 as any)).toThrow('Event is required');
			expect(() => db!.listeners(123 as any)).toThrow('Event is required');
			expect(() => db!.removeListener(123 as any, () => {})).toThrow('Event is required');
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
