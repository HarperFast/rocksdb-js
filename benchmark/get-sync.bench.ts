import { describe } from 'vitest';
import {
	benchmark,
	generateRandomKeys,
	generateSequentialKeys,
	generateTestData,
	randomString,
} from './setup.js';

const SMALL_DATASET = 100;

describe('getSync()', () => {
	describe('random keys - small key size (100 records)', () => {
		function setup(ctx) {
			ctx.data = generateRandomKeys(SMALL_DATASET);
			for (const key of ctx.data) {
				ctx.db.putSync(key, 'test-value');
			}
		}

		benchmark('rocksdb', {
			mode: 'essential',
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.getSync(key);
				}
			}
		});

		benchmark('lmdb', {
			mode: 'essential',
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.get(key);
				}
			}
		});
	});

	describe('sequential keys - small key size (100 records)', () => {
		function setup(ctx) {
			ctx.data = generateSequentialKeys(SMALL_DATASET);
			for (const key of ctx.data) {
				ctx.db.putSync(key, 'test-value');
			}
		}

		benchmark('rocksdb', {
			mode: 'essential',
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.getSync(key);
				}
			}
		});

		benchmark('lmdb', {
			mode: 'essential',
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.get(key);
				}
			}
		});
	});

	describe('rocksdb - random vs sequential keys (100 records)', () => {
		benchmark('rocksdb', {
			name: 'random',
			setup(ctx) {
				ctx.data = generateRandomKeys(SMALL_DATASET);
				for (const key of ctx.data) {
					ctx.db.putSync(key, 'test-value');
				}
			},
			bench({ data, db }) {
				for (const key of data) {
					db.getSync(key);
				}
			}
		});

		benchmark('rocksdb', {
			name: 'sequential',
			setup(ctx) {
				ctx.data = generateSequentialKeys(SMALL_DATASET);
				for (const key of ctx.data) {
					ctx.db.putSync(key, 'test-value');
				}
			},
			bench({ data, db }) {
				for (const key of data) {
					db.getSync(key);
				}
			}
		});
	});

	describe('random keys - max 1978 lmdb key size (100 records)', () => {
		function setup(ctx) {
			ctx.data = generateRandomKeys(SMALL_DATASET, 1978);
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

	describe('random access pattern (100 records)', () => {
		function setup(ctx) {
			const data = generateTestData(SMALL_DATASET, 20, 100)
			for (const item of data) {
				ctx.db.putSync(item.key, item.value);
			}

			ctx.data = data.map(item => item.key).sort(() => Math.random() - 0.5);
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

	describe('get same key multiple times', () => {
		function setup(ctx) {
			ctx.db.putSync('test-key', 'test-value');
		}

		benchmark('rocksdb', {
			setup,
			bench({ db }) {
				for (let i = 0; i < 1000; i++) {
					db.getSync('test-key');
				}
			}
		});

		benchmark('lmdb', {
			setup,
			bench({ db }) {
				for (let i = 0; i < 1000; i++) {
					db.get('test-key');
				}
			}
		});
	});

	describe('get non-existent key', () => {
		function setup(ctx) {
			ctx.db.putSync('test-key', 'test-value');
		}

		benchmark('rocksdb', {
			setup,
			bench({ db }) {
				for (let i = 0; i < 1000; i++) {
					db.getSync(`non-existent-key-${i}`);
				}
			}
		});

		benchmark('lmdb', {
			setup,
			bench({ db }) {
				for (let i = 0; i < 1000; i++) {
					db.get(`non-existent-key-${i}`);
				}
			}
		});
	});

	describe('get 100KB value (100 records)', () => {
		const value = randomString(100 * 1024);

		function setup(ctx) {
			ctx.data = generateRandomKeys(SMALL_DATASET);
			for (const key of ctx.data) {
				ctx.db.putSync(key, value);
			}
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, data }) {
				for (const key of data) {
					db.getSync(key);
				}
			}
		});

		benchmark('lmdb', {
			setup,
			bench({ db, data }) {
				for (const key of data) {
					db.get(key);
				}
			}
		});
	});

	describe('get 1MB value (100 records)', () => {
		const value = randomString(1024 * 1024);

		function setup(ctx) {
			ctx.data = generateRandomKeys(SMALL_DATASET);
			for (const key of ctx.data) {
				ctx.db.putSync(key, value);
			}
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, data }) {
				for (const key of data) {
					db.getSync(key);
				}
			}
		});

		benchmark('lmdb', {
			setup,
			bench({ db, data }) {
				for (const key of data) {
					db.get(key);
				}
			}
		});
	});

	describe('get 10MB value (100 records)', () => {
		const value = randomString(10 * 1024 * 1024);

		function setup(ctx) {
			ctx.data = generateRandomKeys(SMALL_DATASET);
			for (const key of ctx.data) {
				ctx.db.putSync(key, value);
			}
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, data }) {
				for (const key of data) {
					db.getSync(key);
				}
			}
		});

		benchmark('lmdb', {
			setup,
			bench({ db, data }) {
				for (const key of data) {
					db.get(key);
				}
			}
		});
	});
});
