import { describe } from 'vitest';
import { benchmark, concurrent, type BenchmarkContext, type LMDBDatabase } from './setup.js';
import type { RocksDatabase } from '../dist/index.mjs';
import type { TransactionLogQueryOptions } from '../src/load-binding.js';

describe('Transaction log', () => {
	const ENTRY_COUNT = 1000;
	const data = Buffer.alloc(100, 'a');

	describe('write log with 100 byte records', () => {
		benchmark('rocksdb', concurrent({
			async setup(ctx:  BenchmarkContext<RocksDatabase>) {
				const db = ctx.db;
				const log = db.useLog('0');
				ctx.log = log;
			},
			async bench({ db, value, log, start, duration }) {
				return db.transaction((txn) => {
					log.addEntry(data, txn.id);
				});
			},
		}));

		benchmark('lmdb', concurrent({
			async setup(ctx) {
				let start = Date.now();
				ctx.index = start;
			},
			bench(ctx) {
				const { db } = ctx;
				return db.put(ctx.index++, data) as unknown as Promise<void>;
			},
		}));
	});
	describe('read 100 iterators while write log with 100 byte records', () => {
		benchmark('rocksdb', concurrent({
			async setup(ctx) {
				const db = ctx.db;
				const log = db.useLog('0');
				ctx.log = log;
				ctx.iterators = Array(100).fill(null).map(() => log.query({ start: 1 }));
			},
			bench({ db, iterators, log }) {
				let last;
				for (let iterator of iterators) {
					for (let entry of iterator) {
						last = entry.timestamp;
					}
				}
				return db.transaction((txn) => {
					log.addEntry(data, txn.id);
				}) as Promise<void>;
			},
			concurrencyMaximum: 2,
		}));

		benchmark('lmdb', concurrent({
			async setup(ctx) {
				let start = Date.now();
				ctx.index = start;
				ctx.last = 0;
			},
			bench(ctx) {
				const { db, last } = ctx;
				for (let i = 0; i < 100; i++) {
					for (let { key, value } of db.getRange({ start: ctx.last })) {
						ctx.last = key + 1;
					}
				}
				return db.put(ctx.index++, data) as Promise<void>;
			},
			concurrencyMaximum: 2,
		}));
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
