import {
	concurrent,
	workerDescribe as describe,
	workerBenchmark as benchmark
} from './setup.js';

describe('Transaction log with workers', () => {
	const ENTRY_COUNT = 1000;
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
				});
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
