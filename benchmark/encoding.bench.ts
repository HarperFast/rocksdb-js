import { describe } from 'vitest';
import { benchmark, generateTestData, randomString } from './setup.js';

const SMALL_DATASET = 100;

describe('Key encoding', () => {
	describe('ordered-binary keys - strings (100 records)', () => {
		function setup(ctx) {
			ctx.keys = Array.from({ length: SMALL_DATASET }, (_, i) => `string-key-${i}`);
			for (const key of ctx.keys) {
				ctx.db.putSync(key, 'test-value');
			}
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, keys }) {
				for (const key of keys) {
					db.getSync(key);
				}
			},
		});

		benchmark('lmdb', {
			setup,
			bench({ db, keys }) {
				for (const key of keys) {
					db.get(key);
				}
			},
		});
	});

	describe('ordered-binary keys - numbers (100 records)', () => {
		function setup(ctx) {
			for (let i = 0; i < SMALL_DATASET; i++) {
				ctx.db.putSync(i, 'test-value');
			}
		}

		benchmark('rocksdb', {
			setup,
			bench({ db }) {
				for (let i = 0; i < SMALL_DATASET; i++) {
					db.getSync(i);
				}
			},
		});

		benchmark('lmdb', {
			setup,
			bench({ db }) {
				for (let i = 0; i < SMALL_DATASET; i++) {
					db.get(i as any);
				}
			},
		});
	});

	describe('ordered-binary keys - mixed types (100 records)', () => {
		function setup(ctx) {
			ctx.keys = [];

			for (let i = 0; i < SMALL_DATASET; i++) {
				let key;
				switch (i % 5) {
					case 0:
						key = `string-${i}`;
						break;
					case 1:
						key = i;
						break;
					case 2:
						key = true;
						break;
					case 3:
						key = false;
						break;
					case 4:
						key = [i, `item-${i}`];
						break;
				}
				ctx.keys.push(key);
				ctx.db.putSync(key, 'test-value');
			}
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, keys }) {
				for (const key of keys) {
					db.getSync(key);
				}
			},
		});

		benchmark('lmdb', {
			setup,
			bench({ db, keys }) {
				for (const key of keys) {
					db.get(key);
				}
			},
		});
	});
});

describe('Value encoding', () => {
	describe('msgpack values - strings (100 records)', () => {
		function setup(ctx) {
			ctx.data = generateTestData(SMALL_DATASET, 20, 100);
			for (const item of ctx.data) {
				ctx.db.putSync(item.key, item.value);
			}
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, data }) {
				for (const item of data) {
					db.getSync(item.key);
				}
			},
		});

		benchmark('lmdb', {
			setup,
			bench({ db, data }) {
				for (const item of data) {
					db.get(item.key);
				}
			},
		});
	});

	describe('msgpack values - numbers (100 records)', () => {
		function setup(ctx) {
			ctx.numbers = Array.from(
				{ length: SMALL_DATASET },
				(_, i) => ({ key: `num-${i}`, value: Math.random() * 1000000 })
			);
			for (const item of ctx.numbers) {
				ctx.db.putSync(item.key, item.value);
			}
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, numbers }) {
				for (const item of numbers) {
					db.getSync(item.key);
				}
			},
		});

		benchmark('lmdb', {
			setup,
			bench({ db, numbers }) {
				for (const item of numbers) {
					db.get(item.key);
				}
			},
		});
	});

	describe('msgpack values - arrays (100 records)', () => {
		function setup(ctx) {
			ctx.arrays = Array.from(
				{ length: SMALL_DATASET },
				(_, i) => ({
					key: `arr-${i}`,
					value: Array.from({ length: 20 }, (_, j) => `item-${i}-${j}`),
				})
			);
			for (const item of ctx.arrays) {
				ctx.db.putSync(item.key, item.value);
			}
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, arrays }) {
				for (const item of arrays) {
					db.getSync(item.key);
				}
			},
		});

		benchmark('lmdb', {
			setup,
			bench({ db, arrays }) {
				for (const item of arrays) {
					db.get(item.key);
				}
			},
		});
	});

	describe('msgpack values - small objects (100 records)', () => {
		function setup(ctx) {
			ctx.objects = Array.from(
				{ length: SMALL_DATASET },
				(_, i) => ({
					key: `obj-${i}`,
					value: {
						id: i,
						name: `Object ${i}`,
						data: randomString(50),
						timestamp: Date.now(),
						nested: { prop1: `value-${i}`, prop2: i * 2, array: [i, i + 1, i + 2] },
					},
				})
			);
			for (const item of ctx.objects) {
				ctx.db.putSync(item.key, item.value);
			}
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, objects }) {
				for (const item of objects) {
					db.getSync(item.key);
				}
			},
		});

		benchmark('lmdb', {
			setup,
			bench({ db, objects }) {
				for (const item of objects) {
					db.get(item.key);
				}
			},
		});
	});

	describe('msgpack values - large objects (100 records)', () => {
		function setup(ctx) {
			ctx.objects = Array.from(
				{ length: 100 },
				(_, i) => ({
					key: `large-${i}`,
					value: {
						id: i,
						content: randomString(10_000),
						metadata: {
							created: new Date(),
							tags: Array.from({ length: 100 }, (_, j) => `tag-${j}`),
							properties: Object.fromEntries(
								Array.from({ length: 50 }, (_, k) => [`prop${k}`, `value${k}`])
							),
						},
					},
				})
			);
			for (const item of ctx.objects) {
				ctx.db.putSync(item.key, item.value);
			}
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, objects }) {
				for (const item of objects) {
					db.getSync(item.key);
				}
			},
		});

		benchmark('lmdb', {
			setup,
			bench({ db, objects }) {
				for (const item of objects) {
					db.get(item.key);
				}
			},
		});
	});
});
