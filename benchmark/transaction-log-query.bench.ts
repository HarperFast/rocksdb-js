import { describe } from 'vitest';
import { benchmark, generateRandomKeys, generateTestData } from './setup.js';
import { ABORT } from 'lmdb';
import { TransactionLogReader } from '../src';

describe('transaction-log-query', () => {
	const SMALL_DATASET = 100;
	const smallDataset = generateTestData(SMALL_DATASET, 20, 100);

	describe('small log', () => {
		describe('log with 10 100 byte records', () => {

			benchmark('rocksdb', {
				async setup(ctx) {
					const db = ctx.db;
					const log = db.useLog('0');
					global.queryLog = ctx.queryLog = new TransactionLogReader(log);
					const value = Buffer.alloc(100, 'a');
					ctx.start = Date.now();
					for (let i = 0; i < 40; i++) {
						await db.transaction(async (txn) => {
							log.addEntry(value, txn.id);
						});
						await new Promise(resolve => setTimeout(resolve, 10));
					}
					ctx.duration = Date.now() - ctx.start;
				},
				async bench({ db, data, queryLog, start, duration }) {
					let result = Array.from(queryLog.query({start: start + Math.random() * duration}));
				}
			});

			benchmark('lmdb', {
				setup(ctx) {
					let start = Date.now();
					const value = Buffer.alloc(100, 'a');
					for (let i = 0; i < 40; i++) {
						ctx.db.putSync((start + i) as unknown as string, value);
					}
				},
				async bench({ db, data }) {
					// @ts-ignore
					let result = Array.from(db.getRange({}));
				}
			});
		});
	});
});
