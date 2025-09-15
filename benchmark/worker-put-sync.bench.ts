import {
	generateRandomKeys,
	workerDescribe as describe,
	workerBenchmark as benchmark
} from './setup.js';

describe('putSync()', () => {
	const SMALL_DATASET = 100;

	describe('random keys - small key size (100 records, 1 worker)', () => {
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

	describe('random keys - small key size (100 records, 2 workers)', () => {
		benchmark('rocksdb', {
			numWorkers: 2,
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
			numWorkers: 2,
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

	describe('random keys - small key size (100 records, 10 workers)', () => {
		benchmark('rocksdb', {
			numWorkers: 10,
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
			numWorkers: 10,
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
});
