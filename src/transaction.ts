import { DBI } from './dbi';
import { Store } from './store.js';
import type { NativeTransaction } from './util/load-binding.js';

/**
 * Provides transaction level operations to a transaction callback.
 */
export class Transaction extends DBI {
	#txn: NativeTransaction;

	constructor(store: Store) {
		const txn = store.db.createTransaction();
		super(store, txn);
		this.#txn = txn;
	}

	async abort() {
		this.#txn.abort();
	}
	
	async commit() {
		this.#txn.commit();
	}
}
