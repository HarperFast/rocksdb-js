import { DBI } from './dbi';
import { NativeTransaction, type TransactionOptions } from './load-binding.js';
import { Store } from './store.js';

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
	abort(): void {
		this.#txn.abort();
	}

	/**
	 * Commit the transaction.
	 */
	async commit(): Promise<void> {
		try {
			await new Promise<void>((resolve, reject) => {
				this.notify('beforecommit');
				this.#txn.commit(resolve, reject);
			});
		} finally {
			this.notify('aftercommit', { next: null, last: null, txnId: this.#txn.id });
		}
	}

	/**
	 * Commit the transaction synchronously.
	 */
	commitSync(): void {
		try {
			this.notify('beforecommit');
			this.#txn.commitSync();
		} finally {
			this.notify('aftercommit', { next: null, last: null, txnId: this.#txn.id });
		}
	}

	/**
	 * Returns the transaction start timestamp in seconds. Defaults to the time at which
	 * the transaction was created.
	 *
	 * @returns The transaction start timestamp in seconds.
	 */
	getTimestamp(): number {
		return this.#txn.getTimestamp();
	}

	/**
	 * Get the transaction id.
	 */
	get id(): number {
		return this.#txn.id;
	}

	/**
	 * Set the transaction start timestamp in seconds.
	 *
	 * @param timestamp - The timestamp to set in seconds.
	 */
	setTimestamp(timestamp?: number): void {
		this.#txn.setTimestamp(timestamp);
	}
}
