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
					ctx.queryLog = new TransactionLogReader(log);
					const value = Buffer.alloc(100, 'a');
					for (let i = 0; i < 10; i++) {
						await db.transaction(async (txn) => {
							log.addEntryCopy(value, txn.id);
						});
					}
				},
				async bench({ db, data, queryLog }) {
					for (const item of queryLog.query(10, 1000000000000000)) {
					}
				}
			});

			benchmark('lmdb', {
				setup(ctx) {
					let start = Date.now();
					const value = Buffer.alloc(100, 'a');
					for (let i = 0; i < 10; i++) {
						ctx.db.putSync((start + i) as unknown as string, value);
					}
				},
				async bench({ db, data }) {
					// @ts-ignore
					for (const item of db.getRange({start: 10, end: 1000000000000000})) {
					}
				}
			});
		});
	});
});
