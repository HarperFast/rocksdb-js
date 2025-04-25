import type { Key } from './types.js';
import type { Store } from './store.js';
import { NativeDatabase, NativeTransaction } from './util/load-binding.js';
import type { Transaction } from './transaction.js';

export type DBITransactional = {
	transaction: Transaction;
};

const UNMODIFIED = Symbol('UNMODIFIED');

/**
 * The base class for all database operations. This base class is shared by
 * `RocksDatabase` and `Transaction`.
 *
 * This class is not meant to be used directly.
 */
export class DBI<T extends DBITransactional | unknown = unknown> {
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
		if (this.store.decoderCopies) {
			let bytes = await this.getBinaryFast(key, options);
			return !bytes ? undefined : bytes === UNMODIFIED ? {} : this.store.decodeValue(bytes as Buffer);
		}

		if (this.store.encoding === 'binary') {
			return this.getBinary(key, options);
		}

		if (this.store.decoder) {
			const result = await this.getBinary(key, options);
			return result ? this.store.decodeValue(result) : undefined;
		}

		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		const keyBuffer = this.store.encodeKey(key);
		const result = this.#context.get(keyBuffer, getTxnId(options));
		return this.store.decodeValue(result);
	}

	/**
	 * Retrieves the binary data for the given key. This is just like `get()`,
	 * but bypasses the decoder.
	 *
	 * Note: Used by HDBreplication.
	 */
	async getBinary(key: Key, options?: GetOptions & T): Promise<Buffer | undefined> {
		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		const keyBuffer = this.store.encodeKey(key);
		return this.#context.get(keyBuffer, getTxnId(options));
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
	async getBinaryFast(key: Key, options?: GetOptions & T): Promise<Buffer | symbol | undefined> {
		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		const keyBuffer = this.store.encodeKey(key);
		return this.#context.get(keyBuffer, getTxnId(options));
		// TODO: return UNMODIFIED if the value is not modified
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
	async put(key: Key, value: any, options?: PutOptions & T): Promise<void> {
		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		const keyBuffer = this.store.encodeKey(key);
		const valueBuffer = this.store.encodeValue(value);

		this.#context.put(keyBuffer, valueBuffer, getTxnId(options));
	}

	/**
	 * Removes a value for the given key. If the key does not exist, it will
	 * not error.
	 */
	async remove(key: Key, _ifVersionOrValue?: symbol | number | null, options?: T): Promise<void> {
		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		const keyBuffer = this.store.encodeKey(key);
		this.#context.remove(keyBuffer, getTxnId(options));
	}
}

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
