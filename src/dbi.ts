import type { Key } from './encoding.js';
import type { Store } from './store.js';
import { NativeDatabase, NativeTransaction } from './load-binding.js';
import type { Transaction } from './transaction.js';
import { when, withResolvers, type MaybePromise } from './util.js';

export interface DBITransactional {
	transaction: Transaction;
};

/**
 * The base class for all database operations. This base class is shared by
 * `RocksDatabase` and `Transaction`.
 *
 * This class is not meant to be used directly.
 */
export class DBI<T extends DBITransactional | unknown = unknown> {
	/**
	 * The RocksDB context for `get()`, `put()`, and `remove()`.
	 */
	#context: NativeDatabase | NativeTransaction;

	/**
	 * The database store instance. The store instance is tied to the database
	 * instance and shared with transaction instances.
	 */
	store: Store;

	/**
	 * Initializes the DBI context.
	 *
	 * @param store - The store instance.
	 * @param transaction - The transaction instance.
	 */
	constructor(store: Store, transaction?: NativeTransaction) {
		if (new.target === DBI) {
			throw new Error('DBI is an abstract class and cannot be instantiated directly');
		}

		// this ideally should not be public, but JavaScript doesn't support
		// protected properties
		this.store = store;

		this.#context = transaction || store.db;
	}

	/**
	 * Retrieves the value for the given key, then returns the decoded value.
	 */
	get(key: Key, options?: GetOptions & T): MaybePromise<any | undefined> {
		if (this.store.decoderCopies) {
			return when(
				() => this.getBinaryFast(key, options),
				result => result === undefined ? undefined : this.store.decodeValue(result as Buffer)
			);
		}

		return when(
			() => this.getBinary(key, options),
			result => result === undefined
				? undefined
				: this.store.encoding === 'binary' || !this.store.decoder
					? result
					: this.store.decodeValue(result as Buffer)
		);
	}

	getSync(key: Key, options?: GetOptions & T) {
		if (this.store.decoderCopies) {
			const bytes = this.getBinaryFastSync(key, options);
			return bytes === undefined ? undefined : this.store.decodeValue(bytes as Buffer);
		}

		if (this.store.encoding === 'binary') {
			return this.getBinarySync(key, options);
		}

		if (this.store.decoder) {
			const result = this.getBinarySync(key, options);
			return result ? this.store.decodeValue(result) : undefined;
		}

		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		const keyBuffer = this.store.encodeKey(key);
		const result = this.#context.getSync(keyBuffer, getTxnId(options));
		return this.store.decodeValue(result);
	}

	/**
	 * Retrieves the binary data for the given key. This is just like `get()`,
	 * but bypasses the decoder.
	 *
	 * Note: Used by HDBreplication.
	 */
	getBinary(key: Key, options?: GetOptions & T): MaybePromise<Buffer | undefined> {
		if (!this.store.isOpen()) {
			return Promise.reject(new Error('Database not open'));
		}

		const keyBuffer = this.store.encodeKey(key);
		let result: Buffer | undefined;
		let error: Error | undefined;
		let resolve: (value: Buffer | undefined) => void | undefined;
		let reject: (error: Error) => void | undefined;

		const status = this.#context.get(
			keyBuffer,
			value => {
				result = value;
				resolve?.(value);
			},
			err => {
				error = err;
				reject?.(err);
			},
			getTxnId(options)
		);

		if (error) {
			return Promise.reject(error);
		}
		if (status === 0) {
			return result;
		}

		let promise: Promise<Buffer | undefined>;
		({ resolve, reject, promise } = withResolvers<Buffer | undefined>());
		return promise;
	}

	/**
	 * Synchronously retrieves the binary data for the given key.
	 */
	getBinarySync(key: Key, options?: GetOptions & T): Buffer | undefined {
		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		const keyBuffer = this.store.encodeKey(key);
		return this.#context.getSync(keyBuffer, getTxnId(options));
	}

	/**
	 * Retrieves the binary data for the given key using a preallocated,
	 * reusable buffer. Data in the buffer is only valid until the next get
	 * operation (including cursor operations).
	 *
	 * Note: The reusable buffer slightly differs from a typical buffer:
	 * - `.length` is set to the size of the value
	 * - `.byteLength` is set to the size of the full allocated memory area for
	 *   the buffer (usually much larger).
	 */
	getBinaryFast(key: Key, options?: GetOptions & T): MaybePromise<Buffer | undefined> {
		if (!this.store.isOpen()) {
			return Promise.reject(new Error('Database not open'));
		}

		const keyBuffer = this.store.encodeKey(key);
		let result: Buffer | undefined;
		let error: Error | undefined;
		let resolve: (value: Buffer | undefined) => void | undefined;
		let reject: (error: Error) => void | undefined;

		// TODO: specify the shared buffer to write the value to
		const status = this.#context.get(
			keyBuffer,
			value => {
				result = value;
				resolve?.(value);
			},
			err => {
				error = err;
				reject?.(err);
			},
			getTxnId(options)
		);

		if (error) {
			return Promise.reject(error);
		}
		if (status === 0) {
			return result;
		}

		let promise: Promise<Buffer | undefined>;
		({ resolve, reject, promise } = withResolvers<Buffer | undefined>());
		return promise;
	}

	getBinaryFastSync(key: Key, options?: GetOptions & T): Buffer | undefined {
		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		const keyBuffer = this.store.encodeKey(key);
		return this.#context.getSync(keyBuffer, getTxnId(options));
		// TODO: return UNMODIFIED if the value is not modified
	}

	/**
	 * Retrieves all keys within a range.
	 */
	getKeys(_options?: GetRangeOptions & T) {
		if (!this.store.isOpen()) {
			return Promise.reject(new Error('Database not open'));
		}
	}

	getKeysSync(_options?: GetRangeOptions & T) {
		//
	}

	getRange(_options?: GetRangeOptions & T) {
		//
	}

	getRangeSync(_options?: GetRangeOptions & T) {
		//
	}

	getValues(_key: Key, _options?: GetRangeOptions & T) {
		//
	}

	getValuesSync(_key: Key, _options?: GetRangeOptions & T) {
		//
	}

	getValuesCount(_key: Key, _options?: GetRangeOptions & T) {
		//
	}

	getValuesCountSync(_key: Key, _options?: GetRangeOptions & T) {
		//
	}

	/**
	 * Stores a value for the given key.
	 */
	async put(key: Key, value: any, options?: PutOptions & T): Promise<void> {
		this.putSync(key, value, options);
	}

	/**
	 * Synchronously stores a value for the given key.
	 */
	putSync(key: Key, value: any, options?: PutOptions & T) {
		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		const keyBuffer = this.store.encodeKey(key);
		const valueBuffer = this.store.encodeValue(value);
		this.#context.putSync(keyBuffer, valueBuffer, getTxnId(options));
	}

	/**
	 * Removes a value for the given key. If the key does not exist, it will
	 * not error.
	 */
	async remove(key: Key, options?: T): Promise<void> {
		this.removeSync(key, options);
	}

	/**
	 * Removes a value for the given key. If the key does not exist, it will
	 * not error.
	 */
	removeSync(key: Key, options?: T): void {
		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		const keyBuffer = this.store.encodeKey(key);
		this.#context.removeSync(keyBuffer, getTxnId(options));
	}
}

/**
 * Checks if the data method options object contains a transaction ID and
 * returns it.
 */
function getTxnId(options?: DBITransactional | unknown) {
	let txnId;
	if (options && typeof options === 'object' && 'transaction' in options) {
		txnId = (options.transaction as Transaction)?.id;
		if (txnId === undefined) {
			throw new TypeError('Invalid transaction');
		}
	}
	return txnId;
}

interface GetOptions {
	// ifNotTxnId?: number;
	// currentThread?: boolean;
}

interface GetRangeOptions {
	end?: Key | Uint8Array;
	exactMatch?: boolean;
	exclusiveStart?: boolean;
	inclusiveEnd?: boolean;
	limit?: number;
	key?: Key;
	offset?: number;
	onlyCount?: boolean;
	reverse?: boolean;
	snapshot?: boolean;
	start?: Key | Uint8Array;
	values?: boolean;
	valuesForKey?: boolean;
};

interface PutOptions {
	append?: boolean;
	instructedWrite?: boolean;
	noDupData?: boolean;
	noOverwrite?: boolean;
};
