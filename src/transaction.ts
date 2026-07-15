import { DBI } from './dbi';
import { constants, NativeTransaction, type NativeTransactionOptions } from './load-binding.js';
import { Store } from './store.js';

/**
 * Sentinel value returned by `commit()` when `coordinatedRetry: true` and the
 * transaction encountered an IsBusy conflict. The native layer parks on VT
 * slots and resolves only after the conflicting transaction releases its write
 * intent, so callers should retry immediately without any backoff delay.
 */
export const RETRY_NOW: number = constants.RETRY_NOW_VALUE;

export class TransactionAlreadyAbortedError extends Error {
	readonly code = 'ERR_ALREADY_ABORTED';
}

/**
 * Base for commit conflicts the native layer has already recovered from by resetting the
 * transaction in place (fresh RocksDB snapshot; `committedPosition` preserved, so a staged
 * transaction-log batch stays write-once): the caller should re-run the transaction body
 * and retry the commit — the handling is identical for every subclass. `hasLog` reports
 * whether the transaction wrote to a transaction log (drives the default retry decision).
 *
 * {@link TransactionAbandonedError} shares this field shape but deliberately does NOT
 * extend this class — an abandoned transaction is not retryable, and retryability is the
 * boundary this hierarchy expresses.
 */
export class TransactionRetryableError extends Error {
	readonly hasLog: boolean;
	readonly txn: Transaction;

	constructor(error: Error, txn: Transaction) {
		super(error.message);
		this.hasLog = (error as Error & { hasLog?: boolean }).hasLog ?? false;
		this.txn = txn;
	}
}

/**
 * An optimistic commit detected a write conflict with a concurrently committed
 * transaction (RocksDB `Busy`).
 */
export class TransactionIsBusyError extends TransactionRetryableError {
	readonly code = 'ERR_BUSY';
}

/**
 * An optimistic commit could not be validated: the transaction's snapshot was stranded
 * outside the memtable window (its sequence history was flushed away — RocksDB
 * `TryAgain`), so the conflict check had nothing to validate against.
 */
export class TransactionTryAgainError extends TransactionRetryableError {
	readonly code = 'ERR_TRY_AGAIN';
}

export class TransactionAbandonedError extends Error {
	readonly code = 'ERR_TRANSACTION_ABANDONED';
	readonly hasLog: boolean;
	readonly txn: Transaction;

	constructor(error: Error, txn: Transaction) {
		super(error.message);
		this.hasLog = (error as Error & { hasLog?: boolean }).hasLog ?? false;
		this.txn = txn;
	}
}

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
	constructor(store: Store, options?: NativeTransactionOptions) {
		if (store.readOnly) {
			super(store);
			this.#txn = { id: 0 } as NativeTransaction;
			this.abort = this.commitSync = this.setTimestamp = () => {};
			this.commit = async () => {};
			this.getTimestamp = () => 0;
		} else {
			const txn = new NativeTransaction(store.db, options);
			super(store, txn);
			this.#txn = txn;
		}
	}

	/**
	 * Abort the transaction.
	 */
	abort(): void {
		try {
			this.#txn.abort();
		} catch (err) {
			if (err instanceof Error && 'code' in err && err.code === 'ERR_TRANSACTION_ABANDONED') {
				throw new TransactionAbandonedError(err, this);
			}
			throw err;
		}
	}

	/**
	 * Commit the transaction.
	 *
	 * Returns `RETRY_NOW` when `coordinatedRetry: true` and an IsBusy conflict
	 * was detected. The caller should retry the transaction body immediately.
	 */
	async commit(): Promise<typeof RETRY_NOW | void> {
		try {
			const result = await new Promise<number | void>((resolve, reject) => {
				this.notify('beforecommit');
				this.#txn.commit(resolve, reject);
			});
			if (result === RETRY_NOW) {
				return RETRY_NOW;
			}
		} catch (err) {
			throw this.#handleCommitError(err);
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
		} catch (err) {
			throw this.#handleCommitError(err);
		} finally {
			this.notify('aftercommit', { next: null, last: null, txnId: this.#txn.id });
		}
	}

	/**
	 * Detect if error is an already aborted or busy error and return the appropriate error class.
	 *
	 * @param err - The error to check.
	 * @returns The specialized error.
	 */
	#handleCommitError(err: unknown): Error {
		if (err instanceof Error && 'code' in err) {
			if (err.code === 'ERR_ALREADY_ABORTED') {
				return new TransactionAlreadyAbortedError(err.message);
			}
			if (err.code === 'ERR_BUSY') {
				return new TransactionIsBusyError(err, this);
			}
			if (err.code === 'ERR_TRY_AGAIN') {
				return new TransactionTryAgainError(err, this);
			}
		}
		return err as Error;
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
