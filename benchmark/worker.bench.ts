import {
	generateRandomKeys,
	generateSequentialKeys,
	generateTestData,
	randomString,
	workerDescribe as describe,
	workerBenchmark as benchmark
} from './setup.js';

describe.skip('Worker', () => {
	const SMALL_DATASET = 100;

	describe('random keys - small key size (100 records)', () => {
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

		// benchmark('lmdb', {
		// 	setup,
		// 	bench({ data, db }) {
		// 		for (const key of data) {
		// 			db.get(key);
		// 		}
		// 	}
		// });
	});
});