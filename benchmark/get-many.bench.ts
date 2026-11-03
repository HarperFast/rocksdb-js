import { benchmark, generateTestData } from './setup.ts';
import { describe } from 'vitest';

// Cold-cache and per-batch-size comparisons are in multi-get.mts, which runs the arms interleaved.

const BATCH_SIZE = 64;

function setup(ctx) {
	const data = generateTestData(1000, 20, 100);
	for (const item of data) {
		ctx.db.putSync(item.key, item.value);
	}
	ctx.data = data.slice(0, BATCH_SIZE).map((item) => item.key);
}

describe(`read ${BATCH_SIZE} keys`, () => {
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
