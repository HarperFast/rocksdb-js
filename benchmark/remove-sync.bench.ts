import { describe } from 'vitest';
import {
	benchmark,
	generateRandomKeys,
	generateSequentialKeys,
	generateTestData,
} from './setup.js';

const SMALL_DATASET = 100;

describe('removeSync()', () => {
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
					db.removeSync(key);
				}
			},
		});

		benchmark('lmdb', {
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.removeSync(key);
				}
			},
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
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.removeSync(key);
				}
			},
		});

		benchmark('lmdb', {
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.removeSync(key);
				}
			},
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
					db.removeSync(key);
				}
			},
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
					db.removeSync(key);
				}
			},
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
					db.removeSync(key);
				}
			},
		});

		benchmark('lmdb', {
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.removeSync(key);
				}
			},
		});
	});

	describe('random access pattern (100 records)', () => {
		function setup(ctx) {
			const data = generateTestData(SMALL_DATASET, 20, 100);
			for (const item of data) {
				ctx.db.putSync(item.key, item.value);
			}

			ctx.data = data.map(item => item.key).sort(() => Math.random() - 0.5);
		}

		benchmark('rocksdb', {
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.removeSync(key);
				}
			},
		});

		benchmark('lmdb', {
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.removeSync(key);
				}
			},
		});
	});

	describe('non-existent keys (100 records)', () => {
		function setup(ctx) {
			ctx.data = generateRandomKeys(SMALL_DATASET);
		}

		benchmark('rocksdb', {
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.removeSync(key);
				}
			},
		});

		benchmark('lmdb', {
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.removeSync(key);
				}
			},
		});
	});
});
