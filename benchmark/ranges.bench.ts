import { describe } from 'vitest'
import { benchmark, generateTestData, generateSequentialKeys } from './setup.js'

const SMALL_DATASET = 100;
const RANGE_SIZE = 50;

function setupRangeTestData(ctx: any, datasetSize: number) {
	ctx.data = generateTestData(datasetSize, 20, 100);
	ctx.data.sort((a: any, b: any) => a.key.localeCompare(b.key));

	for (const item of ctx.data) {
		ctx.db.putSync(item.key, item.value);
	}

	const startIndex = Math.floor(datasetSize * 0.25); // start at 25%
	const endIndex = Math.min(startIndex + RANGE_SIZE, datasetSize - 1); // take next RANGE_SIZE items

	ctx.startKey = ctx.data[startIndex].key;
	ctx.endKey = ctx.data[endIndex].key;
	ctx.rangeData = ctx.data.slice(startIndex, endIndex + 1);
}

describe('getRange()', () => {
	describe('small range (100 records, 50 range)', () => {
		benchmark('rocksdb', {
			setup(ctx) {
				setupRangeTestData(ctx, SMALL_DATASET);
			},
			bench({ db, startKey, endKey }) {
				for (const _entry of db.getRange({ start: startKey, end: endKey })) {}
			}
		});

		benchmark('lmdb', {
			setup(ctx) {
				setupRangeTestData(ctx, SMALL_DATASET);
			},
			bench({ db, startKey, endKey }) {
				for (const _entry of db.getRange({ start: startKey, end: endKey })) {}
			}
		});
	});

	describe('full scan vs range scan', () => {
		benchmark('rocksdb', {
			name: 'rocksdb full scan',
			setup(ctx) {
				setupRangeTestData(ctx, SMALL_DATASET);
			},
			bench({ db }) {
				for (const _entry of db.getRange()) {}
			}
		});

		benchmark('lmdb', {
			name: 'lmdb full scan',
			setup(ctx) {
				setupRangeTestData(ctx, SMALL_DATASET);
			},
			bench({ db }) {
				for (const _entry of db.getRange()) {}
			}
		});

		benchmark('rocksdb', {
			name: 'rocksdb range scan',
			setup(ctx) {
				setupRangeTestData(ctx, SMALL_DATASET);
			},
			bench({ db, startKey, endKey }) {
				for (const _entry of db.getRange({ start: startKey, end: endKey })) {}
			}
		});

		benchmark('lmdb', {
			name: 'lmdb range scan',
			setup(ctx) {
				setupRangeTestData(ctx, SMALL_DATASET);
			},
			bench({ db, startKey, endKey }) {
				for (const _entry of db.getRange({ start: startKey, end: endKey })) {}
			}
		});
	});
});

describe('getKeys()', () => {
	describe('keys only (100 records, 50 range)', () => {
		benchmark('rocksdb', {
			setup(ctx) {
				setupRangeTestData(ctx, SMALL_DATASET);
			},
			bench({ db, startKey, endKey }) {
				for (const _key of db.getKeys({ start: startKey, end: endKey })) {}
			}
		});

		benchmark('lmdb', {
			setup(ctx) {
				setupRangeTestData(ctx, SMALL_DATASET);
			},
			bench({ db, startKey, endKey }) {
				for (const _key of db.getKeys({ start: startKey, end: endKey })) {}
			}
		});
	});
});

describe('Reverse iteration', () => {
	describe('reverse range (100 records, 50 range)', () => {
		benchmark('rocksdb', {
			setup(ctx) {
				setupRangeTestData(ctx, SMALL_DATASET);
			},
			bench({ db, startKey, endKey }) {
				for (const _entry of db.getRange({
					start: startKey,
					end: endKey,
					reverse: true
				})) {}
			}
		});

		benchmark('lmdb', {
			setup(ctx) {
				setupRangeTestData(ctx, SMALL_DATASET);
			},
			bench({ db, startKey, endKey }) {
				for (const _entry of db.getRange({
					start: startKey,
					end: endKey,
					reverse: true
				})) {}
			}
		});
	});

	describe('rocksdb - reverse vs forward', () => {
		function setup(ctx: any) {
			setupRangeTestData(ctx, SMALL_DATASET);
		}

		benchmark('rocksdb', {
			name: 'forward',
			setup,
			bench({ db, startKey, endKey }) {
				for (const _entry of db.getRange({ start: startKey, end: endKey })) {}
			}
		});

		benchmark('rocksdb', {
			name: 'reverse',
			setup,
			bench({ db, startKey, endKey }) {
				for (const _entry of db.getRange({ start: startKey, end: endKey, reverse: true })) {}
			}
		});
	});
});

describe('Range query patterns', () => {
	describe('prefix scan performance', () => {
		function setup(ctx: any) {
			ctx.data = generateSequentialKeys(SMALL_DATASET, 'prefix-a');

			for (const key of ctx.data) {
				ctx.db.putSync(key, 'test-value');
			}

			ctx.startKey = 'prefix-a';
			ctx.endKey = 'prefix-b'; // exclusive end to get only prefix-a items
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, startKey, endKey }) {
				for (const _entry of db.getRange({ start: startKey, end: endKey })) {}
			}
		});

		benchmark('lmdb', {
			setup,
			bench({ db, startKey, endKey }) {
				for (const _entry of db.getRange({ start: startKey, end: endKey })) {}
			}
		});
	});
});

describe('Sparse data patterns', () => {
	describe('sparse - range over gaps', () => {
		function setup(ctx: any) {
			const allKeys = generateSequentialKeys(SMALL_DATASET * 10, 'sparse');
			ctx.sparseKeys = [];

			for (let i = 0; i < allKeys.length; i += 10) {
				ctx.sparseKeys.push(allKeys[i]);
				ctx.db.putSync(allKeys[i], 'test-value');
			}

			const startIndex = Math.floor(ctx.sparseKeys.length * 0.25);
			const endIndex = Math.floor(ctx.sparseKeys.length * 0.75);

			ctx.startKey = ctx.sparseKeys[startIndex];
			ctx.endKey = ctx.sparseKeys[endIndex];
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, startKey, endKey }) {
				for (const _entry of db.getRange({ start: startKey, end: endKey })) {}
			}
		});

		benchmark('lmdb', {
			setup,
			bench({ db, startKey, endKey }) {
				for (const _entry of db.getRange({ start: startKey, end: endKey })) {}
			}
		});
	});

	describe('sparse - prefix with gaps', () => {
		function setup(ctx: any) {
			const prefixes = ['prefix-a', 'prefix-b', 'prefix-c'];
			ctx.allKeys = [];

			for (const prefix of prefixes) {
				const prefixKeys = generateSequentialKeys(SMALL_DATASET, prefix);
				for (let i = 0; i < prefixKeys.length; i += 5) {
					ctx.allKeys.push(prefixKeys[i]);
					ctx.db.putSync(prefixKeys[i], 'test-value');
				}
			}

			ctx.startKey = 'prefix-a';
			ctx.endKey = 'prefix-b';
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, startKey, endKey }) {
				for (const _entry of db.getRange({ start: startKey, end: endKey })) {}
			}
		});

		benchmark('lmdb', {
			setup,
			bench({ db, startKey, endKey }) {
				for (const _entry of db.getRange({ start: startKey, end: endKey })) {}
			}
		});
	});
});
