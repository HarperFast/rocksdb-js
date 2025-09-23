import { DBI } from './dbi';
import { Store } from './store.js';
import { NativeTransaction, type TransactionOptions } from './load-binding.js';

/**
 * Provides transaction level operations to a transaction callback.
 */
export class Transaction extends DBI {
	#txn: NativeTransaction;

	/**
	 * Create a new transaction.
	 *
	 * @param store - The base store interface for this transaction.
	 * @param options - The options for the transaction.
	 */
	constructor(store: Store, options?: TransactionOptions) {
		const txn = new NativeTransaction(store.db, options);
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
	async commit(): Promise<void> {
		try {
			await new Promise<void>((resolve, reject) => {
				// this.notify('beforecommit');
				this.#txn.commit(resolve, reject);
			});
		} finally {
			this.notify('aftercommit', {
				next: null,
				last: null,
				txnId: this.#txn.id
			});
		}
	}

	commitSync(): void {
		try {
			// this.notify('beforecommit');
			this.#txn.commitSync();
		} finally {
			this.notify('aftercommit', {
				next: null,
				last: null,
				txnId: this.#txn.id
			});
		}
	}

	/**
	 * Get the transaction id.
	 */
	get id() {
		return this.#txn.id;
	}

	useLog(name: string) {
		return this.#txn.useLog(name);
	}
}
