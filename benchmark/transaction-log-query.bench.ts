import { describe } from 'vitest';
import { benchmark, generateRandomKeys, generateTestData } from './setup.js';
import { ABORT } from 'lmdb';
import { TransactionLogReader } from '../src';

describe('transaction-log-query', () => {
	const SMALL_DATASET = 100;
	const smallDataset = generateTestData(SMALL_DATASET, 20, 100);
	const ENTRY_COUNT = 1000;

	describe('small log', () => {
		describe('log with 10 100 byte records', () => {

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
					let result = Array.from(log.query({start: start + Math.random() * duration}));
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
					let result = Array.from(db.getRange({start: start + Math.random() * duration}));
				}
			});
		});
	});
});
