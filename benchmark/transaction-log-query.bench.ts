import { describe } from 'vitest';
import { benchmark, generateRandomKeys, generateTestData } from './setup.js';
import { ABORT } from 'lmdb';
import { TransactionLogReader } from '../src';
const CONCURRENCY_SLOWDOWN = 100;

describe('transaction-log-query', () => {
	const SMALL_DATASET = 100;
	const smallDataset = generateTestData(SMALL_DATASET, 20, 100);
	const ENTRY_COUNT = 1000;
	const data = Buffer.alloc(100, 'a');

	describe('transaction log', () => {
		describe('write log with 100 byte records', () => {
			benchmark('rocksdb', {
				async setup(ctx) {
					const db = ctx.db;
					const log = db.useLog('0');
					ctx.log = log;
				},
				bench: concurrent(({ db, value, log, start, duration },  outstanding) => {
					return db.transaction((txn) => {
						log.addEntry(data, txn.id);
					});
				}),
			});

			benchmark('lmdb', {
				async setup(ctx) {
					let start = Date.now();
					ctx.index = start;
				},
				bench: concurrent((ctx, outstanding) => {
					const { db } = ctx;
					return db.put(ctx.index++, data);
				}),
			});
		});
		describe('read 100 iterators while write log with 100 byte records', () => {
			benchmark('rocksdb', {
				async setup(ctx) {
					const db = ctx.db;
					const log = db.useLog('0');
					ctx.log = log;
					ctx.iterators = Array(100).fill(null).map(() => log.query({ start: 1 }));
				},
				bench: concurrent(({ db, iterators, log },  outstanding) => {
					let last;
					for (let iterator of iterators) {
						for (let entry of iterators) {
							last = entry.timestamp;
						}
					}
					return db.transaction((txn) => {
						log.addEntry(data, txn.id);
					});
				}),
			});

			benchmark('lmdb', {
				async setup(ctx) {
					let start = Date.now();
					ctx.index = start;
					ctx.last = 0;
				},
				bench: concurrent((ctx, outstanding) => {
					const { db, last } = ctx;
					for (let i = 0; i < 100;i ++) {
						for (let { key, value } of db.getRange({ start: ctx.last })) {
							ctx.last = key + 1;
						}
					}
					return db.put(ctx.index++, data);
				}),
			});
		});
		describe('read randomly from log with 1000 100 byte records', () => {
			benchmark('rocksdb', {
				async setup(ctx) {
					const db = ctx.db;
					const log = db.useLog('0');
					ctx.log = log;
					const value = Buffer.alloc(100, 'a');
					ctx.start = Date.now();
					for (let i = 0; i < ENTRY_COUNT; i++) {
						await db.transaction(async (txn) => {
							log.addEntry(value, txn.id);
						});
						await new Promise(resolve => setTimeout(resolve, 1));
					}
					ctx.duration = Date.now() - ctx.start;
				},
				async bench({ db, data, log, start, duration }) {
					let result = Array.from(log.query({start: start + Math.random() * duration}));
				}
			});

			benchmark('lmdb', {
				async setup(ctx) {
					let start = Date.now();
					const value = Buffer.alloc(100, 'a');
					ctx.start = start;
					for (let i = 0; i < ENTRY_COUNT; i++) {
						ctx.db.putSync((start + i) as unknown as string, value);
					}
					ctx.duration = ENTRY_COUNT;
				},
				async bench({ db, data, start, duration }) {
					// @ts-ignore
					let result = Array.from(db.getRange({start: start + Math.random() * duration}));
				}
			});
		});
	});
});
function concurrent<C>(execute: (ctx: C) => Promise<void>, concurrencySlowdown = CONCURRENCY_SLOWDOWN): (ctx: C) => Promise<void> {
	let outstanding = 0;
	let rejection;
	return (ctx: C) => {
		outstanding++;
		execute(ctx, outstanding).then(() => outstanding--, (error) => { rejection = error });
		if (rejection) {
			let error = rejection;
			rejection = null;
			throw rejection;
		}
		return new Promise(outstanding > concurrencySlowdown ? (resolve) => setTimeout(resolve, concurrencySlowdown - outstanding) : setImmediate) as Promise<void>;
	}
}