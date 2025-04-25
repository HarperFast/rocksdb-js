import { DBI } from './dbi';
import { Store } from './store.js';
import type { NativeTransaction } from './load-binding.js';

/**
 * Provides transaction level operations to a transaction callback.
 */
export class Transaction extends DBI {
	#txn: NativeTransaction;

	/**
	 * Create a new transaction.
	 * 
	 * @param store - The base store interface for this transaction.
	 */
	constructor(store: Store) {
		const txn = store.db.createTransaction();
		super(store, txn);
		this.#txn = txn;
	}

	/**
	 * Abort the transaction.
	 */
	abort() {
		this.#txn.abort();
	}
	
	/**
	 * Commit the transaction.
	 */
	commit(): Promise<void> {
		return new Promise<void>((resolve, reject) => {
			this.#txn.commit(resolve, reject);
		});
	}

	/**
	 * Get the transaction id.
	 */
	get id() {
		return this.#txn.id;
	}
}
