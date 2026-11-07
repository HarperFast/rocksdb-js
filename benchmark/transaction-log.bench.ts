import type { RocksDatabase } from '../dist/index.mjs';
import { benchmark, type BenchmarkContext, concurrent, type LMDBDatabase } from './setup.ts';
import { setImmediate as rest } from 'node:timers/promises';
import { describe } from 'vitest';

describe('Transaction log', () => {
	const ENTRY_COUNT = 1000;
	const data = Buffer.alloc(100, 'a');

	benchmark('rocksdb', {
		name: 'create an unbounded transaction-log query',
		mode: 'essential',
		setup(ctx: BenchmarkContext<RocksDatabase>) {
			ctx.log = ctx.db.useLog('0');
			ctx.db.transactionSync((txn) => ctx.log.addEntry(data, txn.id));
		},
		bench({ log }) {
			log.query({ start: 0 });
		},
	});

	benchmark('rocksdb', {
		name: 'drain a transaction-log query that filters every entry',
		mode: 'essential',
		setup(ctx: BenchmarkContext<RocksDatabase>) {
			ctx.log = ctx.db.useLog('0');
			ctx.db.transactionSync((txn) => {
				txn.setTimestamp(10_000);
				for (let i = 0; i < 10_000; i++) ctx.log.addEntry(Buffer.from('x'), txn.id);
			});
		},
		bench({ log }) {
			log.query({ start: 0, end: 1 }).next();
		},
	});

	benchmark('rocksdb', {
		name: 'write with a transaction-bound log',
		mode: 'essential',
		bench({ db }: BenchmarkContext<RocksDatabase>) {
			db.transactionSync((txn) => txn.useLog('0').addEntry(data));
		},
	});

	describe('write log with 100 byte records', () => {
		benchmark(
			'rocksdb',
			concurrent({
				async setup(ctx: BenchmarkContext<RocksDatabase>) {
					const db = ctx.db;
					const log = db.useLog('0');
					ctx.log = log;
				},
				async bench({ db, log }) {
					await db.transaction((txn) => {
						log.addEntry(data, txn.id);
					});
					await rest();
				},
			})
		);

		benchmark(
			'lmdb',
			concurrent({
				async setup(ctx: BenchmarkContext<LMDBDatabase>) {
					let start = Date.now();
					ctx.index = start;
				},
				bench(ctx: BenchmarkContext<LMDBDatabase>) {
					const { db } = ctx;
					return db.put(String(ctx.index++), data) as unknown as Promise<void>;
				},
			})
		);
	});
	describe('read 100 iterators while write log with 100 byte records', () => {
		benchmark(
			'rocksdb',
			concurrent({
				mode: 'essential',
				async setup(ctx: BenchmarkContext<RocksDatabase>) {
					const db = ctx.db;
					const log = db.useLog('0');
					ctx.log = log;
					ctx.iterators = Array(100)
						.fill(null)
						.map(() => log.query({ start: 1 }));
				},
				bench({ db, iterators, log }) {
					let _last: number | undefined;
					for (let iterator of iterators) {
						for (let entry of iterator) {
							_last = entry.timestamp;
						}
					}
					return db.transaction((txn) => {
						log.addEntry(data, txn.id);
					}) as Promise<void>;
				},
				concurrencyMaximum: 2,
			})
		);

		benchmark(
			'lmdb',
			concurrent({
				mode: 'essential',
				async setup(ctx: BenchmarkContext<LMDBDatabase>) {
					let start = Date.now();
					ctx.index = start;
					ctx.last = 0;
				},
				bench(ctx: BenchmarkContext<LMDBDatabase>) {
					const { db } = ctx;
					for (let i = 0; i < 100; i++) {
						for (let { key } of db.getRange({ start: ctx.last })) {
							ctx.last = key + 1;
						}
					}
					return db.put(String(ctx.index++), data) as unknown as Promise<void>;
				},
				concurrencyMaximum: 2,
			})
		);
	});
	describe('read one entry from random position from log with 1000 100 byte records', () => {
		benchmark('rocksdb', {
			mode: 'essential',
			async setup(ctx: BenchmarkContext<RocksDatabase>) {
				const db = ctx.db;
				const log = db.useLog('0');
				ctx.log = log;
				const value = Buffer.alloc(100, 'a');
				ctx.start = Date.now();
				for (let i = 0; i < ENTRY_COUNT; i++) {
					await db.transaction(async (txn) => {
						log.addEntry(value, txn.id);
					});
					await new Promise((resolve) => setTimeout(resolve, 1));
				}
				ctx.duration = Date.now() - ctx.start;
			},
			async bench({ log, start, duration }) {
				for (let _entry of log.query({ start: start + Math.random() * duration })) {
					break; // read just one
				}
			},
		});

		benchmark('lmdb', {
			mode: 'essential',
			async setup(ctx: BenchmarkContext<LMDBDatabase>) {
				let start = Date.now();
				const value = Buffer.alloc(100, 'a');
				ctx.start = start;
				for (let i = 0; i < ENTRY_COUNT; i++) {
					ctx.db.putSync((start + i) as unknown as string, value);
				}
				ctx.duration = ENTRY_COUNT;
			},
			async bench({ db, start, duration }) {
				for (let _entry of db.getRange({ start: start + Math.random() * duration })) {
					break; // read just one
				}
			},
		});
	});
});
