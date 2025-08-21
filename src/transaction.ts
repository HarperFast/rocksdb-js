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
	commit(): Promise<void> {
		return new Promise<void>((resolve, reject) => {
			this.emit('beforecommit');
			this.#txn.commit(resolve, reject);
			this.emit('aftercommit', {
				next: null,
				last: null,
				txnId: this.#txn.id
			});
		});
	}

	commitSync(): void {
		this.emit('beforecommit');
		this.#txn.commitSync();
		this.emit('aftercommit', {
			next: null,
			last: null,
			txnId: this.#txn.id
		});
	}

	/**
	 * Get the transaction id.
	 */
	get id() {
		return this.#txn.id;
	}
}
