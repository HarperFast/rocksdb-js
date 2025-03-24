import type { Key } from './types.js';
import type { Store } from './store.js';
import type { NativeDatabase, NativeTransaction } from './util/load-binding.js';
import type { Transaction } from './transaction.js';

export type DBITransactional = {
	transaction: Transaction;
};


/**
 * The base class for all database operations. This base class is shared by
 * `RocksDatabase` and `Transaction`.
 *
 * This class is not meant to be used directly.
 */
export class DBI<T = unknown> {
	#context: NativeDatabase | NativeTransaction;
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

	doesExist(_key: Key, _versionOrValue: number | Buffer) {
		//
	}

	/**
	 * Retrieves the value for the given key, then returns the decoded value.
	 */
	async get(key: Key, options?: GetOptions & T): Promise<any | undefined> {
		// TODO: Remove async?
		// TODO: Return Promise<any> | any?
		// TODO: Call this.getBinaryFast(key, options)
		// TODO: decode the bytes into a value/object
		// TODO: if decoder copies, then call getBinaryFast()

		if (this.store.encoding === 'binary' || this.store.decoder) {
			const bytes = this.getBinary(key, options);

			if (this.store.decoder) {
				// TODO: decode
			}

			return bytes;
		}

		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		const result = this.#context.get(key);

		if (result && this.store.encoding === 'json') {
			return JSON.parse(result.toString());
		}

		return result;
	}

	/**
	 * Retrieves the binary data for the given key. This is just like `get()`,
	 * but bypasses the decoder.
	 *
	 * Note: Used by HDBreplication.
	 */
	async getBinary(key: Key, options?: GetOptions & T): Promise<Buffer | undefined> {
		const _value = this.getBinaryFast(key, options);
		return Buffer.from('TODO');
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
	async getBinaryFast(key: Key, _options?: GetOptions & T): Promise<Buffer | undefined> {
		const _keyLength = this.store.writeKey(key, this.store.keyBuffer, 0);
		return Buffer.from('TODO');
	}

	/**
	 * Retrieves a value for the given key as an "entry" object.
	 *
	 * An entry object contains a `value` property and when versions are enabled,
	 * it also contains a `version` property.
	 */
	getEntry(key: Key, options?: GetOptions & T) {
		const value = this.get(key, options);

		if (value !== undefined) {
			// TODO: if versions are enabled, add a `version` property
			return {
				value,
			};
		}
	}

	/**
	 * Retrieves all keys within a range.
	 */
	getKeys(_options?: GetRangeOptions & T) {
		//
	}

	getRange(_options?: GetRangeOptions & T) {
		//
	}

	getValues(_key: Key, _options?: GetRangeOptions & T) {
		//
	}

	getValuesCount(_key: Key, _options?: GetRangeOptions & T) {
		//
	}

	/**
	 * Stores a value for the given key.
	 */
	put(key: Key, value: any, _options?: PutOptions & T) {
		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}
		this.#context.put(key, value);
	}

	/**
	 * Removes a value for the given key. If the key does not exist, it will
	 * not error.
	 */
	remove(key: Key, _ifVersionOrValue?: symbol | number | null, _options?: T) {
		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}
		this.#context.remove(key);
	}
}

type GetOptions = {
	ifNotTxnId?: number;
};

type GetRangeOptions = {
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
	versions?: boolean;
};

type PutOptions = {
	append?: boolean;
	ifVersion?: number;
	instructedWrite?: boolean;
	noDupData?: boolean;
	noOverwrite?: boolean;
	version?: number;
};
