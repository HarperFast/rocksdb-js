import {
	generateRandomKeys,
	workerDescribe as describe,
	workerBenchmark as benchmark
} from './setup.js';
import { threadId } from 'worker_threads';
import { setImmediate as rest, setTimeout as delay } from 'timers/promises';

describe('Transaction log with workers', () => {
	const ENTRY_COUNT = 1000;
	const data = Buffer.alloc(100, 'a');

	describe('write log with 100 byte records', () => {
		benchmark('rocksdb', {
			numWorkers: 4,
			async setup(ctx) {
				const db = ctx.db;
				const log = db.useLog('0');
				ctx.log = log;
			},
			bench: concurrent(({ db, value, log, start, duration }) => {
				return db.transaction((txn) => {
					log.addEntry(data, txn.id);
				});
			}),
		});

		benchmark('lmdb', {
			numWorkers: 4,
			async setup(ctx) {
				let start = Date.now();
				ctx.index = start;
			},
			bench: concurrent((ctx) => {
				const { db } = ctx;
				return db.put(ctx.index++, data);
			}),
		});
	});
});

function concurrent<C>(execute: (ctx: C) => Promise<void>, concurrencySlowdown = 100): (ctx: C) => Promise<void> {
	let outstanding = 0;
	let previousRejection: Error;
	return async (ctx: C) => {
		outstanding++;
		execute(ctx).then(() => outstanding--, (error) => {
			previousRejection = error
		});
		if (previousRejection) {
			// a previous execution rejected to an error, so we need to throw it now to surface it
			let error = previousRejection;
			previousRejection = null; // reset this
			throw error;
		}
		if (concurrencySlowdown) {
			return delay(concurrencySlowdown - outstanding);
		} else {
			return rest();
		}
	}
}