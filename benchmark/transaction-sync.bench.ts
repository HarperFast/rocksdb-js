import { describe } from 'vitest';
import { benchmark, generateTestData } from './setup.js'
import { ABORT } from 'lmdb';

describe('transaction sync', () => {
	const SMALL_DATASET = 100;
	const smallDataset = generateTestData(SMALL_DATASET, 20, 100);

	describe('optimistic', () => {
		describe('simple put operations (100 records)', () => {
			function setup(ctx) {
				ctx.data = smallDataset;
			}

			benchmark('rocksdb', {
				setup,
				bench({ db, data }) {
					for (const item of data) {
						db.transactionSync((txn) => {
							txn.putSync(item.key, item.value);
						});
					}
				}
			});

			benchmark('lmdb', {
				setup,
				bench({ db, data }) {
					for (const item of data) {
						db.transactionSync(() => {
							db.putSync(item.key, item.value);
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
				bench({ db, data }) {
					db.transactionSync((txn) => {
						for (const item of data) {
							txn.putSync(item.key, item.value);
						}
					});
				}
			});

			benchmark('lmdb', {
				setup,
				bench({ db, data }) {
					db.transactionSync(() => {
						for (const item of data) {
							db.putSync(item.key, item.value);
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
				bench({ db, data }) {
					for (let i = 0; i < SMALL_DATASET; i++) {
						const key = data[i].key;
						db.transactionSync((txn) => {
							const existing = txn.getSync(key);
							txn.putSync(key, `modified-${existing}`);
						});
					}
				}
			});

			benchmark('lmdb', {
				setup,
				bench({ db, data }) {
					for (let i = 0; i < SMALL_DATASET; i++) {
						const key = data[i].key;
						db.transactionSync(() => {
							const existing = db.get(key);
							db.putSync(key, `modified-${existing}`);
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
						db.transactionSync((txn) => {
							txn.putSync(`${item.key}-${i}`, item.value);
						});
					}));
				}
			});

			benchmark('lmdb', {
				setup,
				async bench({ db, data }) {
					await Promise.all(data.map(async (item, i) => {
						db.transactionSync(() => {
							db.putSync(`${item.key}-${i}`, item.value);
						});
					}));
				}
			});
		});

		describe('rollback operations (100 records)', () => {
			function setup(ctx) {
				ctx.data = smallDataset;
			}

			benchmark('rocksdb', {
				setup,
				bench({ db, data }) {
					db.transactionSync((txn) => {
						for (const item of data) {
							txn.putSync(item.key, item.value);
						}
						txn.abort();
					});
				}
			});

			benchmark('lmdb', {
				setup,
				bench({ db, data }) {
					db.transactionSync(() => {
						for (const item of data) {
							db.putSync(item.key, item.value);
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
				bench({ db, data }) {
					db.transactionSync((txn) => {
						for (const item of data) {
							txn.putSync(item.key, item.value);
						}
					});
				}
			});

			benchmark('rocksdb', {
				setup,
				bench({ db, data }) {
					for (const item of data) {
						db.transactionSync((txn) => {
							txn.putSync(item.key, item.value);
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
				bench({ db, data }) {
					db.transactionSync(() => {
						for (const item of data) {
							db.putSync(item.key, item.value);
						}
					});
				}
			});

			benchmark('lmdb', {
				setup,
				bench({ db, data }) {
					for (const item of data) {
						db.transactionSync(() => {
							db.putSync(item.key, item.value);
						});
					}
				}
			});
		});

		describe('empty transaction overhead', () => {
			benchmark('rocksdb', {
				bench({ db }) {
					db.transactionSync(() => {});
				}
			});

			benchmark('lmdb', {
				bench({ db }) {
					db.transactionSync(() => {});
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
				bench({ db, data }) {
					for (let i = 0; i < SMALL_DATASET; i++) {
						db.transactionSync((txn) => {
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
				bench({ db, data }) {
					for (let i = 0; i < SMALL_DATASET; i++) {
						db.transactionSync(() => {
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
				bench({ db, data }) {
					for (const item of data) {
						db.transactionSync((txn) => {
							txn.putSync(item.key, item.value);
						});
					}
				}
			});

			benchmark('lmdb', {
				setup,
				bench({ db, data }) {
					for (const item of data) {
						db.transactionSync(() => {
							db.putSync(item.key, item.value);
						});
					}
				}
			});
		});
	});
});
