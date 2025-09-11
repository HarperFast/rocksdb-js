import {
	generateRandomKeys,
	workerDescribe as describe,
	workerBenchmark as benchmark
} from './setup.js';

describe('Worker', () => {
	const SMALL_DATASET = 100;

	describe('random keys - small key size (100 records, 1 worker)', () => {
		function setup(ctx) {
			ctx.data = generateRandomKeys(SMALL_DATASET);
			for (const key of ctx.data) {
				ctx.db.putSync(key, 'test-value');
			}
		}

		benchmark('rocksdb', {
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.getSync(key);
				}
			}
		});

		benchmark('lmdb', {
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.get(key);
				}
			}
		});
	});

	describe('random keys - small key size (100 records, 2 workers)', () => {
		function setup(ctx) {
			ctx.data = generateRandomKeys(SMALL_DATASET);
			for (const key of ctx.data) {
				ctx.db.putSync(key, 'test-value');
			}
		}

		benchmark('rocksdb', {
			numWorkers: 2,
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.getSync(key);
				}
			}
		});

		benchmark('lmdb', {
			numWorkers: 2,
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.get(key);
				}
			}
		});
	});
});
