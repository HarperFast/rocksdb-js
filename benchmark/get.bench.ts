import {
	benchmark,
	generateRandomKeys,
	generateSequentialKeys,
	generateTestData,
} from './setup.js';
import { describe } from 'vitest';

const SMALL_DATASET = 100;

// NOTE: lmdb's `get()` is sync and its `getAsync()` doesn't work

describe('get()', () => {
	describe('rocksdb - random vs sequential keys (100 records)', () => {
		benchmark('rocksdb', {
			name: 'random',
			setup(ctx) {
				ctx.data = generateRandomKeys(SMALL_DATASET);
				for (const key of ctx.data) {
					ctx.db.putSync(key, 'test-value');
				}
			},
			async bench({ data, db }) {
				for (const key of data) {
					await db.get(key);
				}
			},
		});

		benchmark('rocksdb', {
			name: 'sequential',
			setup(ctx) {
				ctx.data = generateSequentialKeys(SMALL_DATASET);
				for (const key of ctx.data) {
					ctx.db.putSync(key, 'test-value');
				}
			},
			async bench({ data, db }) {
				for (const key of data) {
					await db.get(key);
				}
			},
		});
	});

	describe('random keys - max 1978 lmdb key size (100 records)', () => {
		benchmark('rocksdb', {
			setup(ctx) {
				ctx.data = generateRandomKeys(SMALL_DATASET, 1978);
				for (const key of ctx.data) {
					ctx.db.putSync(key, 'test-value');
				}
			},
			async bench({ data, db }) {
				for (const key of data) {
					await db.get(key);
				}
			},
		});
	});

	describe('rocksdb - async vs sync', () => {
		function setup(ctx) {
			ctx.data = generateTestData(SMALL_DATASET, 20, 100);
			for (const item of ctx.data) {
				ctx.db.putSync(item.key, item.value);
			}
		}

		benchmark('rocksdb', {
			name: 'async',
			setup,
			async bench({ db, data }) {
				for (const item of data) {
					await db.get(item.key);
				}
			},
		});

		benchmark('rocksdb', {
			name: 'sync',
			setup,
			bench({ db, data }) {
				for (const item of data) {
					db.getSync(item.key);
				}
			},
		});
	});
});
