import { describe } from 'vitest'
import { benchmark, generateRandomKeys, generateSequentialKeys } from './setup.js'

describe('putSync()', () => {
	const SMALL_DATASET = 100;

	describe('random keys - small key size (100 records)', () => {
		benchmark('rocksdb', {
			setup(ctx) {
				ctx.data = generateRandomKeys(SMALL_DATASET);
			},
			bench({ data, db }) {
				for (const key of data) {
					db.putSync(key, 'test-value');
				}
			}
		});

		benchmark('lmdb', {
			setup(ctx) {
				ctx.data = generateRandomKeys(SMALL_DATASET);
			},
			bench({ data, db }) {
				for (const key of data) {
					db.putSync(key, 'test-value');
				}
			}
		});
	});

	describe('random keys - max 1978 lmdb key size (100 records)', () => {
		benchmark('rocksdb', {
			setup(ctx) {
				ctx.data = generateRandomKeys(SMALL_DATASET, 1978);
			},
			bench({ data, db }) {
				for (const key of data) {
					db.putSync(key, 'test-value');
				}
			}
		});

		benchmark('lmdb', {
			setup(ctx) {
				ctx.data = generateRandomKeys(SMALL_DATASET, 1978);
			},
			bench({ data, db }) {
				for (const key of data) {
					db.putSync(key, 'test-value');
				}
			}
		});
	});

	describe('sequential keys (100 records)', () => {
		benchmark('rocksdb', {
			setup(ctx) {
				ctx.data = generateSequentialKeys(SMALL_DATASET);
			},
			bench({ data, db }) {
				for (const key of data) {
					db.putSync(key, 'test-value');
				}
			}
		});

		benchmark('lmdb', {
			setup(ctx) {
				ctx.data = generateSequentialKeys(SMALL_DATASET);
			},
			bench({ data, db }) {
				for (const key of data) {
					db.putSync(key, 'test-value');
				}
			}
		});
	});
});
