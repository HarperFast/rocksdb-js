import type { RocksDatabase } from '../dist/index.mjs';
import { benchmark, type BenchmarkContext, concurrent, type LMDBDatabase } from './setup.js';
import { describe } from 'vitest';

describe('Transaction log perf driver', () => {
	const data = Buffer.alloc(100, 0xab);

	describe('write log with 100 byte records (WAL disabled, tight loop)', () => {
		benchmark(
			'rocksdb',
			concurrent({
				dbOptions: { disableWAL: true },
				async setup(ctx: BenchmarkContext<RocksDatabase>) {
					ctx.log = ctx.db.useLog('perf');
				},
				async bench({ db, log }) {
					await db.transaction((txn) => {
						log.addEntry(data, txn.id);
					});
				},
				restEachTurn: false,
			})
		);

		benchmark(
			'lmdb',
			concurrent({
				async setup(ctx: BenchmarkContext<LMDBDatabase>) {
					ctx.index = Date.now();
				},
				bench(ctx: BenchmarkContext<LMDBDatabase>) {
					const { db } = ctx;
					return db.put(String(ctx.index++), data) as unknown as Promise<void>;
				},
				restEachTurn: false,
			})
		);
	});
});
