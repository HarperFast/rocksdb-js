import { DBI } from './dbi.ts';
import type { Key } from './encoding.ts';
import { constants, NativeTransaction, type NativeTransactionOptions } from './load-binding.ts';
import { type PutOptions, Store } from './store.ts';

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
 * transaction in place (fresh RocksDB snapshot). While {@link Transaction.hasDurableLog} is
 * `false` the caller re-runs the transaction body and retries the commit; once it is `true`
 * the transaction is not reset and still holds the write set the log describes, so the caller
 * retries `commit()` without staging anything again. `hasLog` reports whether the
 * transaction carried transaction-log entries, staged or already written (it drives the
 * default retry decision).
 *
 * {@link TransactionLoggedError} deliberately does NOT extend this class: it reports a refused
 * abort, not a commit outcome, and retryability is the boundary this hierarchy expresses.
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

/**
 * `abort()`, `abandonWrites()`, `put()` or `remove()` was refused because a commit attempt
 * already wrote the transaction's log batch ({@link Transaction.hasDurableLog}). Nothing was
 * changed: the transaction is still pending with the write set the log describes, and the
 * caller keeps calling `commit()` until it succeeds.
 */
export class TransactionLoggedError extends Error {
	readonly code = 'ERR_TRANSACTION_LOGGED';
	readonly txn: Transaction;

	constructor(error: Error, txn: Transaction, options?: ErrorOptions) {
		super(error.message, options);
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
			this.#txn = { id: 0, hasDurableLog: false } as NativeTransaction;
			this.abandonWrites = this.abort = this.commitSync = this.setTimestamp = () => {};
			this.commit = async () => {};
			this.getTimestamp = () => 0;
		} else {
			const txn = new NativeTransaction(store.db, options);
			super(store, txn);
			this.#txn = txn;
		}
	}

	/**
	 * Release the staged writes' verification-table write intents without
	 * closing the transaction, barring any later commit or write. For a
	 * transaction kept open only for its outstanding read iterators after the
	 * writes were replayed onto another transaction: otherwise other writers'
	 * coordinated-retry commits park on those intents until it is aborted.
	 * Reads keep working, including read-your-own-writes.
	 */
	abandonWrites(): void {
		try {
			this.#txn.abandonWrites();
		} catch (err) {
			throw this.#handleLoggedRefusal(err);
		}
	}

	/**
	 * Abort the transaction. Refused with {@link TransactionLoggedError}, before anything is
	 * rolled back, once a commit attempt has written the transaction's log batch.
	 */
	abort(): void {
		try {
			this.#txn.abort();
		} catch (err) {
			throw this.#handleLoggedRefusal(err);
		}
	}

	put(key: Key, value: any, options?: PutOptions): Promise<void> {
		return super.put(key, value, options).catch((err: unknown) => {
			throw this.#handleLoggedRefusal(err);
		});
	}

	putSync(key: Key, value: any, options?: PutOptions): void {
		try {
			super.putSync(key, value, options);
		} catch (err) {
			throw this.#handleLoggedRefusal(err);
		}
	}

	remove(key: Key, options?: unknown): Promise<void> {
		return super.remove(key, options).catch((err: unknown) => {
			throw this.#handleLoggedRefusal(err);
		});
	}

	removeSync(key: Key, options?: unknown): void {
		try {
			super.removeSync(key, options);
		} catch (err) {
			throw this.#handleLoggedRefusal(err);
		}
	}

	/**
	 * Translates a native `ERR_TRANSACTION_LOGGED` into {@link TransactionLoggedError} for a
	 * write routed through a database with `{ transaction }`.
	 */
	static loggedRefusal(err: unknown, txn: unknown): unknown {
		if (
			txn instanceof Transaction &&
			err instanceof Error &&
			'code' in err &&
			err.code === 'ERR_TRANSACTION_LOGGED'
		) {
			return new TransactionLoggedError(err, txn);
		}
		return err;
	}

	/**
	 * Whether a commit attempt has written this transaction's transaction-log batch. An
	 * optimistic conflict is checked before the batch is written, so an ordinary conflict
	 * retry leaves this `false`; once `true` it stays `true` across retries, after the commit
	 * lands, and after close.
	 */
	get hasDurableLog(): boolean {
		return this.#txn.hasDurableLog;
	}

	#handleLoggedRefusal(err: unknown): unknown {
		if (err instanceof Error && 'code' in err && err.code === 'ERR_TRANSACTION_LOGGED') {
			return new TransactionLoggedError(err, this);
		}
		return err;
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
	 * Returns the transaction timestamp in milliseconds since the Unix epoch. It
	 * defaults to a process-wide monotonic value assigned when the transaction
	 * was created. A producer may use this value as its write identity; the
	 * transaction log uses it as the batch key.
	 *
	 * @returns The transaction timestamp in milliseconds.
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
	 * Overrides the transaction timestamp in milliseconds since the Unix epoch.
	 * Call this before reading the timestamp or staging writes when adopting an
	 * origin timestamp for replication or replay. Once a write or log entry is
	 * staged, the timestamp is frozen, but reapplying the same timestamp remains
	 * idempotent while the transaction is pending. The value must be finite,
	 * positive, and below 8640000000000000.
	 *
	 * @param timestamp - The timestamp to set in milliseconds.
	 */
	setTimestamp(timestamp?: number): void {
		this.#txn.setTimestamp(timestamp);
	}
}
