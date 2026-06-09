import { RocksDatabase } from '../src/database.js';
import { withResolvers } from '../src/util.js';
import { createWorkerBootstrapScript, dbRunner } from './lib/util.js';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';
import { afterEach, describe, expect, it, vi } from 'vitest';

const noop = () => {};

describe('Global Events', () => {
	afterEach(() => {
		RocksDatabase.off('global-events-test', noop);
		RocksDatabase.off('global-events-test:other', noop);
	});

	it('should register and unregister a listener', () => {
		expect(RocksDatabase.listenerCount('global-events-test')).toBe(0);

		const cb = vi.fn();
		RocksDatabase.on('global-events-test', cb);
		expect(RocksDatabase.listenerCount('global-events-test')).toBe(1);

		expect(RocksDatabase.off('global-events-test', cb)).toBe(true);
		expect(RocksDatabase.listenerCount('global-events-test')).toBe(0);

		// removing again should report no-match
		expect(RocksDatabase.off('global-events-test', cb)).toBe(false);
	});

	it('should dispatch to a registered listener', async () => {
		const { promise, resolve } = withResolvers<any[]>();
		const cb = (...args: any[]) => resolve(args);
		RocksDatabase.on('global-events-test', cb);
		try {
			expect(RocksDatabase.notify('global-events-test', 'hello', 42)).toBe(true);
			await expect(promise).resolves.toEqual(['hello', 42]);
		} finally {
			RocksDatabase.off('global-events-test', cb);
		}
	});

	it('should return false from notify when there are no listeners', () => {
		expect(RocksDatabase.notify('global-events-test:nobody-home')).toBe(false);
	});

	it('should dispatch to multiple listeners and stop after removal', async () => {
		const cb1 = vi.fn();
		const cb2 = vi.fn();
		const cb3 = vi.fn();

		const resolvers = [withResolvers<void>(), withResolvers<void>(), withResolvers<void>()];
		const wrap1 = () => {
			cb1();
			resolvers[0].resolve();
		};
		const wrap2 = () => {
			cb2();
			resolvers[1].resolve();
		};
		const wrap3 = () => {
			cb3();
			resolvers[2].resolve();
		};

		RocksDatabase.on('global-events-test', wrap1);
		RocksDatabase.on('global-events-test', wrap2);
		RocksDatabase.on('global-events-test', wrap3);
		try {
			expect(RocksDatabase.listenerCount('global-events-test')).toBe(3);
			RocksDatabase.notify('global-events-test');
			await Promise.all(resolvers.map((r) => r.promise));
			expect(cb1).toHaveBeenCalledTimes(1);
			expect(cb2).toHaveBeenCalledTimes(1);
			expect(cb3).toHaveBeenCalledTimes(1);

			RocksDatabase.off('global-events-test', wrap2);
			expect(RocksDatabase.listenerCount('global-events-test')).toBe(2);

			RocksDatabase.notify('global-events-test');
			// give cb2 a chance to fire — it shouldn't
			await delay(50);
			expect(cb1).toHaveBeenCalledTimes(2);
			expect(cb2).toHaveBeenCalledTimes(1);
			expect(cb3).toHaveBeenCalledTimes(2);
		} finally {
			RocksDatabase.off('global-events-test', wrap1);
			RocksDatabase.off('global-events-test', wrap3);
		}
	});

	it('should be isolated from per-database events', () =>
		dbRunner(async ({ db }) => {
			const globalCb = vi.fn();
			const dbCb = vi.fn();

			const globalDone = withResolvers<void>();
			const dbDone = withResolvers<void>();

			const globalHandler = () => {
				globalCb();
				globalDone.resolve();
			};
			const dbHandler = () => {
				dbCb();
				dbDone.resolve();
			};

			RocksDatabase.on('global-events-test:shared-name', globalHandler);
			db.addListener('global-events-test:shared-name', dbHandler);
			try {
				// per-DB notify should only hit the per-DB listener
				db.notify('global-events-test:shared-name');
				await dbDone.promise;
				await delay(50);
				expect(dbCb).toHaveBeenCalledTimes(1);
				expect(globalCb).toHaveBeenCalledTimes(0);

				// global notify should only hit the global listener
				RocksDatabase.notify('global-events-test:shared-name');
				await globalDone.promise;
				expect(dbCb).toHaveBeenCalledTimes(1);
				expect(globalCb).toHaveBeenCalledTimes(1);
			} finally {
				RocksDatabase.off('global-events-test:shared-name', globalHandler);
				db.removeListener('global-events-test:shared-name', dbHandler);
			}
		}));

	it('should support addListener as an alias of on', () => {
		const cb = vi.fn();
		RocksDatabase.addListener('global-events-test', cb);
		try {
			expect(RocksDatabase.listenerCount('global-events-test')).toBe(1);
		} finally {
			RocksDatabase.off('global-events-test', cb);
		}
	});

	it('should keep dispatching across keys as listeners churn (count gate stays accurate)', async () => {
		// Exercises the lock-free listener-count gate that lets notify skip the
		// mutex when nothing is listening. The dangerous failure is an
		// undercounted gate that reads zero while a listener still exists, which
		// would silently drop a dispatch. Register on two keys, remove one, and
		// assert the other still fires; then remove all and assert the gate
		// reports no listeners again.
		const cbA = vi.fn();
		const { promise, resolve } = withResolvers<void>();
		const cbB = () => {
			cbA(); // reuse the spy so we can assert call count
			resolve();
		};
		RocksDatabase.on('global-events-test', cbA);
		RocksDatabase.on('global-events-test:other', cbB);
		try {
			// drop the first key's listener; the second must still dispatch
			expect(RocksDatabase.off('global-events-test', cbA)).toBe(true);
			expect(RocksDatabase.listenerCount('global-events-test')).toBe(0);
			expect(RocksDatabase.listenerCount('global-events-test:other')).toBe(1);

			expect(RocksDatabase.notify('global-events-test:other')).toBe(true);
			await promise;
			expect(cbA).toHaveBeenCalledTimes(1);
		} finally {
			RocksDatabase.off('global-events-test:other', cbB);
		}

		// with all listeners gone the gate engages: notify is a no-op
		expect(RocksDatabase.listenerCount('global-events-test:other')).toBe(0);
		expect(RocksDatabase.notify('global-events-test:other')).toBe(false);
	});

	it('should treat removeListener as an alias of off', () => {
		const cb = vi.fn();
		RocksDatabase.on('global-events-test', cb);
		expect(RocksDatabase.removeListener('global-events-test', cb)).toBe(true);
		expect(RocksDatabase.listenerCount('global-events-test')).toBe(0);
	});

	it('should isolate listeners per event key', () => {
		const cb = vi.fn();
		RocksDatabase.on('global-events-test', cb);
		try {
			expect(RocksDatabase.listenerCount('global-events-test')).toBe(1);
			expect(RocksDatabase.listenerCount('global-events-test:other')).toBe(0);
		} finally {
			RocksDatabase.off('global-events-test', cb);
		}
	});

	it('should reject non-function callbacks', () => {
		expect(() => RocksDatabase.on('global-events-test', 'nope' as any)).toThrow(
			'Callback must be a function'
		);
		expect(() => RocksDatabase.off('global-events-test', 42 as any)).toThrow(
			'Callback must be a function'
		);
	});

	it('should reject missing event names', () => {
		expect(() => RocksDatabase.on(undefined as any, noop)).toThrow('Event is required');
		expect(() => RocksDatabase.off(undefined as any, noop)).toThrow('Event is required');
		expect(() => RocksDatabase.listenerCount(undefined as any)).toThrow('Event is required');
		expect(() => RocksDatabase.notify(undefined as any)).toThrow('Event is required');
	});

	it('should cross worker_threads boundaries in both directions', async () => {
		const worker = new Worker(
			createWorkerBootstrapScript('./test/workers/global-events-worker.mts'),
			{ eval: true }
		);

		const started = withResolvers<void>();
		const fromWorker = withResolvers<any>();
		const fromMainAck = withResolvers<any>();

		worker.on('message', (msg) => {
			if (msg.started) started.resolve();
			else if (msg.fromMain !== undefined) fromMainAck.resolve(msg.fromMain);
		});
		worker.on('error', (e) => started.reject(e));

		try {
			await started.promise;

			// worker → main: worker calls RocksDatabase.notify; main listener fires
			const mainHandler = (value: any) => fromWorker.resolve(value);
			RocksDatabase.on('global-events-test:from-worker', mainHandler);
			worker.postMessage({ notify: 'hello-from-worker' });
			await expect(fromWorker.promise).resolves.toBe('hello-from-worker');
			RocksDatabase.off('global-events-test:from-worker', mainHandler);

			// main → worker: main calls RocksDatabase.notify; worker listener fires
			// (the worker registered its listener at startup and posts back via parentPort)
			RocksDatabase.notify('global-events-test:from-main', 'hello-from-main');
			await expect(fromMainAck.promise).resolves.toBe('hello-from-main');
		} finally {
			worker.postMessage({ exit: true });
			await new Promise<void>((resolve) => worker.on('exit', () => resolve()));
		}
	});

	it("should clean up a worker's listeners when the worker exits", async () => {
		// The worker registers a listener for 'global-events-test:from-main' at
		// startup and then exits without removing it. Before the env-cleanup
		// purge was wired up, the singleton would retain a dangling tsfn
		// pointer here and the subsequent notify from this thread would
		// dereference freed memory. After the fix, the listener is gone and
		// the notify is a no-op for the worker side.
		const worker = new Worker(
			createWorkerBootstrapScript('./test/workers/global-events-worker.mts'),
			{ eval: true }
		);

		const started = withResolvers<void>();
		worker.on('message', (msg) => {
			if (msg.started) started.resolve();
		});
		worker.on('error', (e) => started.reject(e));
		await started.promise;

		worker.postMessage({ exit: true });
		await new Promise<void>((resolve) => worker.on('exit', () => resolve()));

		// Give the worker's env cleanup hook a moment to drain.
		await delay(100);

		// Without the cleanup hook this would crash with a stale tsfn deref;
		// with it, notify just returns false (no listeners) or true (only the
		// caller's listener), neither of which fires the dead worker's callback.
		const mainHandler = vi.fn();
		RocksDatabase.on('global-events-test:from-main', mainHandler);
		try {
			RocksDatabase.notify('global-events-test:from-main', 'after-exit');
			await delay(50);
			// Only the main-thread listener should have fired; the dead worker's
			// listener was removed by the env-cleanup hook.
			expect(mainHandler).toHaveBeenCalledTimes(1);
			expect(mainHandler).toHaveBeenCalledWith('after-exit');
		} finally {
			RocksDatabase.off('global-events-test:from-main', mainHandler);
		}
	});
});
