import { describe } from 'vitest'
import { benchmark, generateTestData } from './setup.js'
import { ABORT } from 'lmdb';

describe('transaction', () => {
	const SMALL_DATASET = 100;
	const smallDataset = generateTestData(SMALL_DATASET, 20, 100);

	describe('optimistic', () => {
		describe('simple put operations (100 records)', () => {
			function setup(ctx) {
				ctx.data = smallDataset;
			}

			benchmark('rocksdb', {
				setup,
				async bench({ db, data }) {
					for (const item of data) {
						await db.transaction(async (txn) => {
							await txn.put(item.key, item.value);
						});
					}
				}
			});

			benchmark('lmdb', {
				setup,
				async bench({ db, data }) {
					for (const item of data) {
						await db.transaction(async () => {
							await db.put(item.key, item.value);
						});
					}
				}
			});
		});

		describe('batch operations (100 records)', () => {
			function setup(ctx) {
				ctx.data = smallDataset;
			}

			benchmark('rocksdb', {
				setup,
				async bench({ db, data }) {
					await db.transaction(async (txn) => {
						for (const item of data) {
							await txn.put(item.key, item.value);
						}
					});
				}
			});

			benchmark('lmdb', {
				setup,
				async bench({ db, data }) {
					await db.transaction(async () => {
						for (const item of data) {
							await db.put(item.key, item.value);
						}
					});
				}
			});
		});

		describe('read-write operations (100 records)', () => {
			function setup(ctx) {
				ctx.data = smallDataset;
			}

			benchmark('rocksdb', {
				setup,
				async bench({ db, data }) {
					for (let i = 0; i < SMALL_DATASET; i++) {
						const key = data[i].key;
						await db.transaction(async (txn) => {
							const existing = await txn.get(key);
							await txn.put(key, `modified-${existing}`);
						});
					}
				}
			});

			benchmark('lmdb', {
				setup,
				async bench({ db, data }) {
					for (let i = 0; i < SMALL_DATASET; i++) {
						const key = data[i].key;
						await db.transaction(async () => {
							const existing = await db.get(key);
							await db.put(key, `modified-${existing}`);
						});
					}
				}
			});
		});

		describe('concurrent non-conflicting operations (100 records)', () => {
			function setup(ctx) {
				ctx.data = smallDataset;
			}

			benchmark('rocksdb', {
				setup,
				async bench({ db, data }) {
					await Promise.all(data.map(async (item, i) => {
						await db.transaction(async (txn) => {
							txn.putSync(`${item.key}-${i}`, item.value);
						});
					}));
				}
			});

			benchmark('lmdb', {
				setup,
				async bench({ db, data }) {
					await Promise.all(data.map(async (item, i) => {
						await db.transaction(async () => {
							db.putSync(`${item.key}-${i}`, item.value);
						});
					}));
				}
			});
		});

		describe.skip('rollback operations (100 records)', () => {
			function setup(ctx) {
				ctx.data = smallDataset;
			}

			benchmark('rocksdb', {
				setup,
				async bench({ db, data }) {
					await db.transaction(async (txn) => {
						for (const item of data) {
							await txn.put(item.key, item.value);
						}
						txn.abort();
					});
				}
			});

			benchmark('lmdb', {
				setup,
				async bench({ db, data }) {
					await db.transaction(async () => {
						for (const item of data) {
							await db.put(item.key, item.value);
						}
						return ABORT;
					});
				}
			});
		});

		describe('rocksdb - large transaction vs many small', () => {
			function setup(ctx) {
				ctx.data = smallDataset;
			}

			benchmark('rocksdb', {
				setup,
				async bench({ db, data }) {
					await db.transaction(async (txn) => {
						for (const item of data) {
							await txn.put(item.key, item.value);
						}
					});
				}
			});

			benchmark('rocksdb', {
				setup,
				async bench({ db, data }) {
					for (const item of data) {
						await db.transaction(async (txn) => {
							await txn.put(item.key, item.value);
						});
					}
				}
			});
		});

		describe('lmdb - large transaction vs many small', () => {
			function setup(ctx) {
				ctx.data = smallDataset;
			}

			benchmark('lmdb', {
				setup,
				async bench({ db, data }) {
					await db.transaction(async () => {
						for (const item of data) {
							await db.put(item.key, item.value);
						}
					});
				}
			});

			benchmark('lmdb', {
				setup,
				async bench({ db, data }) {
					for (const item of data) {
						await db.transaction(async () => {
							await db.put(item.key, item.value);
						});
					}
				}
			});
		});

		describe('empty transaction overhead', () => {
			benchmark('rocksdb', {
				async bench({ db }) {
					await db.transaction(async () => {});
				}
			});

			benchmark('lmdb', {
				async bench({ db }) {
					await db.transaction(async () => {});
				}
			});
		});

		describe('transaction with only reads (100 records)', () => {
			function setup(ctx) {
				ctx.data = smallDataset;
				for (const item of ctx.data) {
					ctx.db.putSync(item.key, item.value);
				}
			}

			benchmark('rocksdb', {
				setup,
				async bench({ db, data }) {
					for (let i = 0; i < SMALL_DATASET; i++) {
						await db.transaction(async (txn) => {
							for (let j = 0; j < 10; j++) {
								const item = data[j % data.length];
								txn.getSync(item.key);
							}
						});
					}
				}
			});

			benchmark('lmdb', {
				setup,
				async bench({ db, data }) {
					for (let i = 0; i < SMALL_DATASET; i++) {
						await db.transaction(async () => {
							for (let j = 0; j < 10; j++) {
								const item = data[j % data.length];
								db.get(item.key);
							}
						});
					}
				}
			});
		});
	});

	describe('pessimistic', () => {
		describe('simple put operations (100 records)', () => {
			function setup(ctx) {
				ctx.data = smallDataset;
			}

			benchmark('rocksdb', {
				dbOptions: { pessimistic: true },
				setup,
				async bench({ db, data }) {
					for (const item of data) {
						await db.transaction(async (txn) => {
							await txn.put(item.key, item.value);
						});
					}
				}
			});

			benchmark('lmdb', {
				setup,
				async bench({ db, data }) {
					for (const item of data) {
						await db.transaction(async () => {
							await db.put(item.key, item.value);
						});
					}
				}
			});
		});
	});
});
