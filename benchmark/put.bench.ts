import { describe } from 'vitest';
import { benchmark, generateTestData } from './setup.js';

describe('put', () => {
	const SMALL_DATASET = 100;
	const smallDataset = generateTestData(SMALL_DATASET, 20, 100);

	describe('small dataset (100 records)', () => {
		benchmark('rocksdb', {
			setup(ctx) {
				ctx.data = smallDataset;
			},
			async bench({ data, db }) {
				for (const item of data) {
					await db.put(item.key, item.value);
				}
			},
		});

		benchmark('lmdb', {
			setup(ctx) {
				ctx.data = smallDataset;
			},
			async bench({ data, db }) {
				for (const item of data) {
					await db.put(item.key, item.value);
				}
			},
		});
	});

	// NOTE: rocksdb-js' `put()` is sync under the hood, so this simply tests
	// Node.js' promise handling overhead
	describe('async vs sync overhead', () => {
		function setup(ctx) {
			ctx.data = smallDataset;
		}

		benchmark('rocksdb', {
			name: 'async',
			setup,
			async bench({ db, data }) {
				for (const item of data) {
					await db.put(item.key, item.value);
				}
			},
		});

		benchmark('rocksdb', {
			name: 'sync',
			setup,
			bench({ db, data }) {
				for (const item of data) {
					db.putSync(item.key, item.value);
				}
			},
		});
	});
});
