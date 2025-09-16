import { describe } from 'vitest';
import { benchmark, generateRandomKeys, generateSequentialKeys, randomString } from './setup.js';

describe('putSync()', () => {
	const SMALL_DATASET = 100;

	describe('random keys - insert - small key size (100 records)', () => {
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

	describe('random keys - update - small key size (100 records)', () => {
		function setup(ctx) {
			const data = generateRandomKeys(SMALL_DATASET);
			for (const key of data) {
				ctx.db.putSync(key, 'test-value');
			}
			ctx.data = data.sort(() => Math.random() - 0.5);
		}

		benchmark('rocksdb', {
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.putSync(key, 'test-value-updated');
				}
			}
		});

		benchmark('lmdb', {
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.putSync(key, 'test-value-updated');
				}
			}
		});
	});

	describe('random keys - insert - max 1978 lmdb key size (100 records)', () => {
		benchmark('rocksdb', {
			setup(ctx) {
				ctx.data = generateRandomKeys(SMALL_DATASET, 1978);
			},
			bench({ data, db }) {
				for (const key of data) {
					db.putSync(key, 'test-value');
				}
			}
		});

		benchmark('lmdb', {
			setup(ctx) {
				ctx.data = generateRandomKeys(SMALL_DATASET, 1978);
			},
			bench({ data, db }) {
				for (const key of data) {
					db.putSync(key, 'test-value');
				}
			}
		});
	});

	describe('random keys - update - max 1978 lmdb key size (100 records)', () => {
		function setup(ctx) {
			const data = generateRandomKeys(SMALL_DATASET, 1978);
			for (const key of data) {
				ctx.db.putSync(key, 'test-value');
			}
			ctx.data = data.sort(() => Math.random() - 0.5);
		}

		benchmark('rocksdb', {
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.putSync(key, 'test-value-updated');
				}
			}
		});

		benchmark('lmdb', {
			setup,
			bench({ data, db }) {
				for (const key of data) {
					db.putSync(key, 'test-value-updated');
				}
			}
		});
	});

	describe('sequential keys - insert (100 records)', () => {
		benchmark('rocksdb', {
			setup(ctx) {
				ctx.data = generateSequentialKeys(SMALL_DATASET);
				for (const key of ctx.data) {
					ctx.db.putSync(key, 'test-value');
				}
			},
			bench({ data, db }) {
				for (const key of data) {
					db.putSync(key, 'test-value-updated');
				}
			}
		});

		benchmark('lmdb', {
			setup(ctx) {
				ctx.data = generateSequentialKeys(SMALL_DATASET);
				for (const key of ctx.data) {
					ctx.db.putSync(key, 'test-value');
				}
			},
			bench({ data, db }) {
				for (const key of data) {
					db.putSync(key, 'test-value-updated');
				}
			}
		});
	});

	describe('put 100KB value (100 records)', () => {
		const value = randomString(100 * 1024);

		function setup(ctx) {
			ctx.data = generateRandomKeys(SMALL_DATASET);
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, data }) {
				for (const key of data) {
					db.putSync(key, value);
				}
			}
		});

		benchmark('lmdb', {
			setup,
			bench({ db, data }) {
				for (const key of data) {
					db.putSync(key, value);
				}
			}
		});
	});

	describe('put 1MB value (100 records)', () => {
		const value = randomString(1024 * 1024);

		function setup(ctx) {
			ctx.data = generateRandomKeys(SMALL_DATASET);
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, data }) {
				for (const key of data) {
					db.putSync(key, value);
				}
			}
		});

		benchmark('lmdb', {
			setup,
			bench({ db, data }) {
				for (const key of data) {
					db.putSync(key, value);
				}
			}
		});
	});

	describe('get 10MB value (100 records)', () => {
		const value = randomString(10 * 1024 * 1024);

		function setup(ctx) {
			ctx.data = generateRandomKeys(SMALL_DATASET);
		}

		benchmark('rocksdb', {
			setup,
			bench({ db, data }) {
				for (const key of data) {
					db.putSync(key, value);
				}
			}
		});

		benchmark('lmdb', {
			setup,
			bench({ db, data }) {
				for (const key of data) {
					db.putSync(key, value);
				}
			}
		});
	});
});
