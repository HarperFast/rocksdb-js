import EventEmitter, { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';
import { describe, expect, it, vi } from 'vitest';
import { withResolvers } from '../src/util.js';
import { createWorkerBootstrapScript, dbRunner, generateDBPath } from './lib/util.js';

describe('Events', () => {
	it('should notify to listeners', () =>
		dbRunner(async ({ db }) => {
			const spy = vi.fn();
			const spy2 = vi.fn();

			expect(db.listeners('foo')).toBe(0);
			expect(db.notify('foo')).toBe(false); // noop

			let resolvers = [withResolvers<void>()];

			db.addListener('foo', value => {
				spy();
				resolvers[0].resolve(value);
			});

			expect(db.listeners('foo')).toBe(1);
			expect(db.notify('foo')).toBe(true);
			await Promise.all(resolvers.map(r => r.promise));
			expect(spy).toHaveBeenCalledTimes(1);

			resolvers = [withResolvers<void>(), withResolvers<void>()];

			// add second listener
			const callback2 = () => {
				spy2();
				resolvers[1].resolve();
			};
			db.addListener('foo', callback2);

			expect(db.listeners('foo')).toBe(2);
			expect(db.notify('foo')).toBe(true);
			await Promise.all(resolvers.map(r => r.promise));
			expect(spy).toHaveBeenCalledTimes(2);
			expect(spy2).toHaveBeenCalledTimes(1);

			// remove listener
			expect(db.removeListener('foo', callback2)).toBe(true);
			expect(db.listeners('foo')).toBe(1);

			resolvers = [withResolvers<void>()];
			expect(db.notify('foo', 'bar')).toBe(true);
			await expect(Promise.all(resolvers.map(r => r.promise))).resolves.toEqual(['bar']);
			expect(spy).toHaveBeenCalledTimes(3);
			expect(spy2).toHaveBeenCalledTimes(1);
		}));

	it('should notify with arguments', () =>
		dbRunner(async ({ db }) => {
			let resolver = withResolvers<any[]>();
			db.addListener('foo', (...args) => {
				resolver.resolve(args);
			});
			db.notify('foo', 'bar');
			await expect(resolver.promise).resolves.toEqual(['bar']);

			resolver = withResolvers<any[]>();
			db.notify('foo', 1234);
			await expect(resolver.promise).resolves.toEqual([1234]);

			resolver = withResolvers<any[]>();
			db.notify('foo', true);
			await expect(resolver.promise).resolves.toEqual([true]);

			resolver = withResolvers<any[]>();
			db.notify('foo', false);
			await expect(resolver.promise).resolves.toEqual([false]);

			resolver = withResolvers<any[]>();
			db.notify('foo', null);
			await expect(resolver.promise).resolves.toEqual([null]);

			resolver = withResolvers<any[]>();
			db.notify('foo', [1, 2, 3]);
			await expect(resolver.promise).resolves.toEqual([[1, 2, 3]]);

			resolver = withResolvers<any[]>();
			db.notify('foo', { foo: 'bar' });
			await expect(resolver.promise).resolves.toEqual([{ foo: 'bar' }]);

			resolver = withResolvers<any[]>();
			db.notify('foo', 'bar', 1234, true, false, null, [1, 2, 3], { foo: 'bar' });
			await expect(resolver.promise).resolves.toEqual(['bar', 1234, true, false, null, [1, 2, 3], {
				foo: 'bar',
			}]);
		}));

	it('should notify once', () =>
		dbRunner(async ({ db }) => {
			const { promise, resolve } = withResolvers<void>();
			let callCount = 0;

			db.once('foo', () => {
				callCount++;
				resolve();
			});

			db.notify('foo');
			await promise; // Wait for first callback
			expect(callCount).toBe(1);

			db.notify('foo');
			// Give time for potential second callback (shouldn't happen)
			await new Promise(resolve => setTimeout(resolve, 50));
			expect(callCount).toBe(1);
		}));

	it("should work with Node's once()", () =>
		dbRunner(async ({ db }) => {
			const promise = once(db as unknown as EventEmitter, 'foo');
			db.notify('foo', 'bar', 'baz');
			await expect(promise).resolves.toEqual(['bar', 'baz']);
		}));

	it('should remove listeners after notify', () =>
		dbRunner(async ({ db }) => {
			const spy1 = vi.fn();
			const spy2 = vi.fn();

			const { promise, resolve } = withResolvers<void>();

			const callback1 = () => {
				spy1();
				db.removeListener('foo', callback1);
				db.removeListener('foo', callback2);
				resolve();
			};
			const callback2 = () => {
				spy2();
			};

			db.addListener('foo', callback1);
			db.addListener('foo', callback2);

			expect(db.listeners('foo')).toBe(2);

			db.notify('foo');
			await promise;
			expect(spy1).toHaveBeenCalledTimes(1);
			expect(spy2.mock.calls.length).toBeLessThanOrEqual(1);
			expect(db.listeners('foo')).toBe(0);

			db.notify('foo');
			await delay(250); // we have nothing to await, so we wait for 250ms
			expect(spy1).toHaveBeenCalledTimes(1);
			expect(spy2.mock.calls.length).toBeLessThanOrEqual(1);
			expect(db.listeners('foo')).toBe(0);
		}));

	it('should bound events to database', () =>
		dbRunner(
			{ dbOptions: [{}, { name: 'db2' }, { path: generateDBPath() }] },
			async ({ db }, { db: db2 }, { db: db3 }) => {
				const spy = vi.fn();
				const spy2 = vi.fn();
				const spy3 = vi.fn();

				let resolvers = [withResolvers<void>(), withResolvers<void>(), withResolvers<void>()];

				const callback = () => {
					spy();
					resolvers[0].resolve();
				};
				const callback2 = () => {
					spy2();
					resolvers[1].resolve();
				};
				const callback3 = () => {
					// this callback should never be called
					spy3();
					resolvers[2].resolve();
				};

				db.addListener('foo', callback);
				db2.addListener('foo', callback2);
				db3.addListener('foo', callback3);

				db.notify('foo');
				await resolvers[0].promise;
				await resolvers[1].promise;
				await Promise.race([resolvers[2].promise, delay(100)]);
				expect(spy).toHaveBeenCalledTimes(1);
				expect(spy2).toHaveBeenCalledTimes(1);
				expect(spy3).toHaveBeenCalledTimes(0);

				resolvers = [withResolvers<void>(), withResolvers<void>(), withResolvers<void>()];
				db.notify('foo');
				await resolvers[0].promise;
				await resolvers[1].promise;
				await Promise.race([resolvers[2].promise, delay(100)]);
				expect(spy).toHaveBeenCalledTimes(2);
				expect(spy2).toHaveBeenCalledTimes(2);
				expect(spy3).toHaveBeenCalledTimes(0);

				resolvers = [withResolvers<void>(), withResolvers<void>(), withResolvers<void>()];
				db2.notify('foo');
				await resolvers[0].promise;
				await resolvers[1].promise;
				await Promise.race([resolvers[2].promise, delay(100)]);
				expect(spy).toHaveBeenCalledTimes(3);
				expect(spy2).toHaveBeenCalledTimes(3);
				expect(spy3).toHaveBeenCalledTimes(0);

				db.removeListener('foo', callback);
				db2.removeListener('foo', callback2);

				resolvers = [withResolvers<void>(), withResolvers<void>(), withResolvers<void>()];
				db2.notify('foo');
				db2.notify('foo');

				await Promise.race([
					...resolvers.map(r =>
						r.promise.then(() => {
							throw new Error('Expected listeners to not be called');
						})
					),
					delay(250),
				]);

				expect(spy).toHaveBeenCalledTimes(3);
				expect(spy2).toHaveBeenCalledTimes(3);
				expect(spy3).toHaveBeenCalledTimes(0);
			}
		));

	it('should notify events from worker threads', () =>
		dbRunner(async ({ db, dbPath }) => {
			const worker = new Worker(createWorkerBootstrapScript('./test/workers/events-worker.mts'), {
				eval: true,
				workerData: { path: dbPath },
			});

			let resolver = withResolvers<void>();

			await new Promise<void>((resolve, reject) => {
				worker.on('error', reject);
				worker.on('message', event => {
					try {
						if (event.started) {
							resolve();
						} else if (event.parentEvent) {
							resolver.resolve(event.parentEvent);
						}
					} catch (error) {
						reject(error);
					}
				});
				worker.on('exit', () => resolver.resolve());
			});

			resolver = withResolvers<void>();
			db.addListener('worker-event', value => resolver.resolve(value));
			worker.postMessage({ notify: true });
			await expect(resolver.promise).resolves.toBe('foo');

			resolver = withResolvers<void>();
			db.notify('parent-event', 'bar');
			await expect(resolver.promise).resolves.toBe('bar');

			resolver = withResolvers<void>();
			worker.postMessage({ close: true });

			if (process.versions.deno) {
				// deno doesn't emit an `exit` event when the worker quits, but
				// `terminate()` will trigger the `exit` event
				await delay(100);
				worker.terminate();
			}

			await resolver.promise;
		}));

	it('should error if database is not open', () =>
		dbRunner({ skipOpen: true }, async ({ db }) => {
			expect(() => db.addListener('foo', () => {})).toThrow('Database not open');
			expect(() => db.notify('foo')).toThrow('Database not open');
			expect(() => db.listeners('foo')).toThrow('Database not open');
			expect(() => db.removeListener('foo', () => {})).toThrow('Database not open');
		}));

	it('should error if event is not a string', () =>
		dbRunner(async ({ db }) => {
			expect(() => db.addListener(123 as any, () => {})).toThrow('Event is required');
			expect(() => db.notify(123 as any)).toThrow('Event is required');
			expect(() => db.listeners(123 as any)).toThrow('Event is required');
			expect(() => db.removeListener(123 as any, () => {})).toThrow('Event is required');
		}));

	it('should error if callback is not a function', () =>
		dbRunner(async ({ db }) => {
			expect(() => db.addListener('foo', 'foo' as any)).toThrow('Callback must be a function');
			expect(() => db.removeListener('foo', 'foo' as any)).toThrow('Callback must be a function');
		}));
});
