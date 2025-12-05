import {
	concurrent,
	workerDescribe as describe,
	workerBenchmark as benchmark
	type BenchmarkContext,
	type LMDBDatabase
} from './setup.js';
import type { RocksDatabase } from '../dist/index.mjs';

describe('Transaction log with workers', () => {
	const data = Buffer.alloc(100, 'a');

	describe('write log with 100 byte records', () => {
		benchmark('rocksdb', concurrent({
			numWorkers: 4,
			async setup(ctx) {
				const db = ctx.db;
				const log = db.useLog('0');
				ctx.log = log;
			},
			bench({ db, value, log, start, duration }) {
				return db.transaction((txn) => {
					log.addEntry(data, txn.id);
				}) as Promise<void>;
			},
		}));

		benchmark('lmdb', concurrent({
			numWorkers: 4,
			async setup(ctx) {
				let start = Date.now();
				ctx.index = start;
			},
			bench(ctx) {
				const { db } = ctx;
				return db.put(ctx.index++, data);
			},
		}));
	});
});
