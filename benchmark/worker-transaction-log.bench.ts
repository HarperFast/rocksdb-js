import type { RocksDatabase } from '../dist/index.mjs';
import {
	type BenchmarkContext,
	concurrent,
	type LMDBDatabase,
	workerBenchmark as benchmark,
	workerDescribe as describe,
} from './setup.js';

describe('Transaction log with workers', () => {
	const data = Buffer.alloc(100, 'a');

	describe('write log with 100 byte records', () => {
		benchmark(
			'rocksdb',
			concurrent({
				mode: 'essential',
				numWorkers: 4,
				async setup(ctx: BenchmarkContext<RocksDatabase>) {
					const db = ctx.db;
					const log = db.useLog('0');
					ctx.log = log;
				},
				bench({ db, log }) {
					return db.transaction((txn) => {
						log.addEntry(data, txn.id);
					}) as Promise<void>;
				},
			})
		);

		benchmark(
			'lmdb',
			concurrent({
				mode: 'essential',
				numWorkers: 4,
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
});
