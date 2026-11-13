import { benchmark, generateTestData } from './setup.ts';
import { describe } from 'vitest';

// Cold-cache and per-batch-size comparisons are in multi-get.mts, which runs the arms interleaved.

for (const batchSize of [2, 64]) {
	const setup = (ctx) => {
		const data = generateTestData(1000, 20, 100);
		for (const item of data) {
			ctx.db.putSync(item.key, item.value);
		}
		ctx.data = data.slice(0, batchSize).map((item) => item.key);
	};

	describe(`read ${batchSize} keys`, () => {
		benchmark('rocksdb', {
			name: 'getSync() per key',
			mode: 'essential',
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.getSync(key);
				}
			},
		});

		benchmark('rocksdb', {
			name: 'getManySync()',
			mode: 'essential',
			setup,
			bench({ data, db }) {
				db.getManySync(data);
			},
		});

		benchmark('rocksdb', {
			name: 'get() per key',
			setup,
			async bench({ data, db }) {
				await Promise.all(data.map((key) => db.get(key)));
			},
		});

		benchmark('rocksdb', {
			name: 'getMany()',
			setup,
			async bench({ data, db }) {
				await db.getMany(data);
			},
		});
	});
}
