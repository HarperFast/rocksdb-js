import type { Key } from './types.js';
import type { Store } from './store.js';
import type { DBContext } from './util/load-binding.js';

export class DBI {
	store: Store;

	constructor(store: Store) {
		this.store = store;
	}

	doesExist(_key: Key, _versionOrValue: number | Buffer) {
		//
	}

	/**
	 * Retrieves the value for the given key, then returns the decoded value.
	 */
	async get(key: Key, options?: GetOptions): Promise<any | undefined> {
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

		// TODO: pass in the context
		const result = this.store.db.get(key);

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
	async getBinary(key: Key, options?: GetOptions): Promise<Buffer | undefined> {
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
	async getBinaryFast(key: Key, _options?: GetOptions): Promise<Buffer | undefined> {
		const _keyLength = this.store.writeKey(key, this.store.keyBuffer, 0);
		return Buffer.from('TODO');
	}

	/**
	 * Retrieves a value for the given key as an "entry" object.
	 *
	 * An entry object contains a `value` property and when versions are enabled,
	 * it also contains a `version` property.
	 */
	getEntry(key: Key, options?: GetOptions) {
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
	getKeys(_options?: GetRangeOptions) {
		//
	}

	getRange(_options?: GetRangeOptions) {
		//
	}

	getValues(_key: Key, _options?: GetRangeOptions) {
		//
	}

	getValuesCount(_key: Key, _options?: GetRangeOptions) {
		//
	}

	put(key: Key, value: any, _options?: PutOptions) {
		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		// TODO: pass in the context
		this.store.db.put(key, value);
	}

	remove(key: Key, _ifVersionOrValue?: symbol | number | null) {
		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		// TODO: pass in the context
		this.store.db.remove(key);
	}
}

type GetOptions = {
	ifNotTxnId?: number;
	// transaction?: Transaction;
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
	// transaction?: Transaction;
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
