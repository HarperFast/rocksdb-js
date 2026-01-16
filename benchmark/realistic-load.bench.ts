import type { RocksDatabase } from '../dist/index.mjs';
import {
	type BenchmarkContext,
	concurrent,
	type LMDBDatabase,
	workerBenchmark as benchmark,
	workerDescribe as describe,
} from './setup.js';

const DELETE_RATIO = 0.2;
const NUM_KEYS = 5_000;
describe('Realistic write load with workers', () => {
	const aaaa = Buffer.alloc(1500, 'a');
	const ITERATIONS = 100;
	describe('write variable records with transaction log', () => {
		benchmark(
			'rocksdb',
			concurrent({
				mode: 'essential',
				numWorkers: 4,
				concurrencyMaximum: 32,
				dbOptions: { disableWAL: true },
				setup(ctx: BenchmarkContext<RocksDatabase>) {
					const db = ctx.db;
					const log = db.useLog('0');
					ctx.log = log;
				},
				async bench({ db, log }) {
					for (let i = 0; i < ITERATIONS; i++) {
						await db.transaction((txn) => {
							const key = Math.floor(Math.random() * NUM_KEYS).toString();
							if (Math.random() < DELETE_RATIO) {
								log.addEntry(aaaa.subarray(0, 30), txn.id);
								db.removeSync(key, { transaction: txn });
							} else {
								const data = aaaa.subarray(0, Math.random() * 1500);
								log.addEntry(data, txn.id);
								db.putSync(key, data, { transaction: txn });
							}
						}).catch((error) => {
							if (error.code !== 'ERR_BUSY') {
								console.error('Error occurred during transaction:', error);
							}
						});
					}
				},
			})
		);

		benchmark(
			'lmdb',
			concurrent({
				mode: 'essential',
				numWorkers: 4,
				concurrencyMaximum: 32,
				async setup(ctx: BenchmarkContext<LMDBDatabase>) {
					let start = Date.now();
					ctx.index = start;
					ctx.lastTime = Date.now();
				},
				async bench(ctx: BenchmarkContext<LMDBDatabase>) {
					const { db } = ctx;
					for (let i = 0; i < ITERATIONS; i++) {
						let auditTime = ctx.lastTime = Math.max(ctx.lastTime + 0.001, Date.now());
						const key = Math.floor(Math.random() * NUM_KEYS).toString();
						if (Math.random() < DELETE_RATIO) {
							db.put('audit' + auditTime, aaaa.subarray(0, 30));
							await db.remove(key);
						} else {
							const data = aaaa.subarray(0, Math.random() * 1500);
							db.put('audit' + auditTime, data);
							await db.put(key, data);
						}
					}
				},
			})
		);
	});
});
