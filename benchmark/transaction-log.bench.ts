import { describe } from 'vitest';
import { benchmark, generateTestData } from './setup.js';

describe('Transaction log', () => {
	const ENTRY_COUNT = 1000;
	const data = Buffer.alloc(100, 'a');

	describe('write log with 100 byte records', () => {
		benchmark('rocksdb', {
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
	describe('read 100 iterators while write log with 100 byte records', () => {
		benchmark('rocksdb', {
			async setup(ctx) {
				const db = ctx.db;
				const log = db.useLog('0');
				ctx.log = log;
				ctx.iterators = Array(100).fill(null).map(() => log.query({ start: 1 }));
			},
			bench: concurrent(({ db, iterators, log }) => {
				let last;
				for (let iterator of iterators) {
					for (let entry of iterator) {
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
			bench: concurrent((ctx) => {
				const { db, last } = ctx;
				for (let i = 0; i < 100; i++) {
					for (let { key, value } of db.getRange({ start: ctx.last })) {
						ctx.last = key + 1;
					}
				}
				return db.put(ctx.index++, data);
			}),
		});
	});
	describe('read one entry from random position from log with 1000 100 byte records', () => {
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
				for (let entry of log.query({ start: start + Math.random() * duration })) {
					break; // read just one
				}
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
				for (let entry of db.getRange({ start: start + Math.random() * duration })) {
					break; // read just one
				}
			}
		});
	});
});
function concurrent<C>(execute: (ctx: C) => Promise<void>, concurrencySlowdown = 100): (ctx: C) => Promise<void> {
	let outstanding = 0;
	let rejection;
	return (ctx: C) => {
		outstanding++;
		execute(ctx).then(() => outstanding--, (error) => { rejection = error });
		if (rejection) {
			let error = rejection;
			rejection = null;
			throw rejection;
		}
		return new Promise(outstanding > concurrencySlowdown ? (resolve) => setTimeout(resolve, concurrencySlowdown - outstanding) : setImmediate) as Promise<void>;
	}
}