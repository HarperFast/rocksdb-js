import type { BufferWithDataView, Key } from './encoding.js';
import type { NativeTransaction, TransactionLog } from './load-binding.js';
import type { GetOptions, PutOptions, Store, StoreContext } from './store.js';
import type { Transaction } from './transaction.js';
import { type MaybePromise, when } from './util.js';

export interface RocksDBOptions {
	/**
	 * When `true`, RocksDB will do some enhancements for prefetching the data.
	 * Defaults to `true`. Note that RocksDB defaults this to `false`.
	 */
	adaptiveReadahead?: boolean;

	/**
	 * When `true`, RocksDB will prefetch some data async and apply it if reads
	 * are sequential and its internal automatic prefetching. Defaults to
	 * `true`. Note that RocksDB defaults this to `false`.
	 */
	asyncIO?: boolean;

	/**
	 * When `true`, RocksDB will auto-tune the readahead size during scans
	 * internally based on the block cache data when block caching is enabled,
	 * an end key (e.g. upper bound) is set, and prefix is the same as the start
	 * key. Defaults to `true`.
	 */
	autoReadaheadSize?: boolean;

	/**
	 * When `true`, after the iterator is closed, a background job is scheduled
	 * to flush the job queue and delete obsolete files. Defaults to `true`.
	 * Note that RocksDB defaults this to `false`.
	 */
	backgroundPurgeOnIteratorCleanup?: boolean;

	/**
	 * When `true`, the iterator will fill the block cache. Filling the block
	 * cache is not desirable for bulk scans and could impact eviction order.
	 * Defaults to `false`. Note that RocksDB defaults this to `true`.
	 */
	fillCache?: boolean;

	/**
	 * The RocksDB readahead size. RocksDB does auto-readahead for iterators
	 * when there is more than two reads for a table file. The readahead
	 * starts at 8KB and doubles on every additional read up to 256KB. This
	 * option can help if most of the range scans are large and if a larger
	 * readahead than that enabled by auto-readahead is needed. Using a large
	 * readahead size (> 2MB) can typically improve the performance of forward
	 * iteration on spinning disks. Defaults to `0`.
	 */
	readaheadSize?: number;

	/**
	 * When `true`, creates a "tailing iterator" which is a special iterator
	 * that has a view of the complete database including newly added data and
	 * is optimized for sequential reads. This will return records that were
	 * inserted into the database after the creation of the iterator. Defaults
	 * to `false`.
	 */
	tailing?: boolean;
}

export interface RangeOptions extends RocksDBOptions {
	/**
	 * The range end key, otherwise known as the "upper bound". Defaults to
	 * the last key in the database.
	 */
	end?: Key | Uint8Array;

	/**
	 * When `true`, the iterator will exclude the first key if it matches the start key.
	 * Defaults to `false`.
	 */
	exclusiveStart?: boolean;

	/**
	 * When `true`, the iterator will include the last key if it matches the end
	 * key. Defaults to `false`.
	 */
	inclusiveEnd?: boolean;

	/**
	 * The range start key, otherwise known as the "lower bound". Defaults to
	 * the first key in the database.
	 */
	start?: Key | Uint8Array;
}

export interface IteratorOptions extends RangeOptions {
	// decoder?: (value: any) => any,

	// exactMatch?: boolean;

	// limit?: number;

	/**
	 * A specific key to match which may result in zero, one, or many values.
	 */
	key?: Key;

	// offset?: number;

	/**
	 * When `true`, only returns the number of values for the given query.
	 */
	onlyCount?: boolean;

	/**
	 * When `true`, the iterator will iterate in reverse order. Defaults to
	 * `false`.
	 */
	reverse?: boolean;

	// snapshot?: boolean;

	/**
	 * When `true`, decodes and returns the value. When `false`, the value is
	 * omitted. Defaults to `true`.
	 */
	values?: boolean;

	/**
	 * When `true`, the iterator will only return the values.
	 */
	valuesOnly?: boolean;
}

export interface DBITransactional {
	transaction?: Transaction;
}

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
	_context: StoreContext;

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

		this._context = transaction || store.db;
	}

	/**
	 * Adds a listener for the given key.
	 *
	 * @param event - The event name to add the listener for.
	 * @param callback - The callback to add.
	 */
	addListener(event: string, callback: (...args: any[]) => void): this {
		this.store.db.addListener(event, callback);
		return this;
	}

	/**
	 * Retrieves the value for the given key, then returns the decoded value.
	 */
	get(key: Key, options?: GetOptions & T): MaybePromise<any | undefined> {
		if (this.store.decoderCopies) {
			return when(
				() => this.getBinaryFast(key, options),
				(result) => {
					if (result === undefined) {
						return undefined;
					}

					if (options?.skipDecode) {
						return result;
					}

					return this.store.decodeValue(result as BufferWithDataView);
				}
			);
		}

		return when(
			() => this.getBinary(key, options),
			(result) =>
				result === undefined
					? undefined
					: this.store.encoding === 'binary' || !this.store.decoder || options?.skipDecode
						? result
						: this.store.decodeValue(result as BufferWithDataView)
		);
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

		return this.store.get(this._context, key, true, this.store.getTxnId(options));
	}

	/**
	 * Synchronously retrieves the binary data for the given key.
	 */
	getBinarySync(key: Key, options?: GetOptions & T): Buffer | undefined {
		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		return this.store.getSync(this._context, key, true, options);
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

		return this.store.get(this._context, key, false, this.store.getTxnId(options));
	}

	/**
	 * Synchronously retrieves the binary data for the given key using a
	 * preallocated, reusable buffer. Data in the buffer is only valid until the
	 * next get operation (including cursor operations).
	 */
	getBinaryFastSync(key: Key, options?: GetOptions & T): Buffer | undefined {
		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		return this.store.getSync(this._context, key, false, options);
	}

	/**
	 * Retrieves all keys within a range.
	 */
	getKeys(options?: IteratorOptions & T): any | undefined {
		return this.store.getKeys(this._context, options);
	}

	/**
	 * Retrieves the number of keys within a range.
	 *
	 * @param options - The range options.
	 * @returns The number of keys within the range.
	 *
	 * @example
	 * ```typescript
	 * const total = db.getKeysCount();
	 * const range = db.getKeysCount({ start: 'a', end: 'z' });
	 * ```
	 */
	getKeysCount(options?: RangeOptions & T): number {
		return this.store.getKeysCount(this._context, options);
	}

	/**
	 * Retrieves a range of keys and their values.
	 *
	 * @param options - The iterator options.
	 * @returns A range iterable.
	 *
	 * @example
	 * ```typescript
	 * for (const { key, value } of db.getRange()) {
	 *   console.log({ key, value });
	 * }
	 *
	 * for (const { key, value } of db.getRange({ start: 'a', end: 'z' })) {
	 *   console.log({ key, value });
	 * }
	 * ```
	 */
	getRange(options?: IteratorOptions & T): any | undefined {
		return this.store.getRange(this._context, options);
	}

	/**
	 * Synchronously retrieves the value for the given key, then returns the
	 * decoded value.
	 */
	getSync(key: Key, options?: GetOptions & T): any | undefined {
		if (this.store.decoderCopies) {
			const bytes = this.getBinaryFastSync(key, options);
			return bytes === undefined ? undefined : this.store.decodeValue(bytes as BufferWithDataView);
		}

		if (this.store.encoding === 'binary') {
			return this.getBinarySync(key, options);
		}

		if (this.store.decoder) {
			const result = this.getBinarySync(key, options);
			return result ? this.store.decodeValue(result as BufferWithDataView) : undefined;
		}

		if (!this.store.isOpen()) {
			throw new Error('Database not open');
		}

		return this.store.decodeValue(this.store.getSync(this._context, key, true, options));
	}

	/**
	 * Gets the number of listeners for the given key.
	 *
	 * @param event - The event name to get the listeners for.
	 * @returns The number of listeners for the given key.
	 */
	listeners(event: string | BufferWithDataView): number {
		return this.store.db.listeners(event);
	}

	/**
	 * Notifies an event for the given key.
	 *
	 * @param event - The event name to emit the event for.
	 * @param args - The arguments to emit.
	 * @returns `true` if there were listeners, `false` otherwise.
	 */
	notify(event: string, ...args: any[]): boolean {
		return this.store.db.notify(event, args);
	}

	/**
	 * Alias for `removeListener()`.
	 *
	 * @param event - The event name to remove the listener for.
	 * @param callback - The callback to remove.
	 */
	off(event: string, callback: (...args: any[]) => void): this {
		this.store.db.removeListener(event, callback);
		return this;
	}

	/**
	 * Alias for `addListener()`.
	 *
	 * @param event - The event name to add the listener for.
	 * @param callback - The callback to add.
	 */
	on(event: string, callback: (...args: any[]) => void): this {
		this.store.db.addListener(event, callback);
		return this;
	}

	/**
	 * Adds a one-time listener, then automatically removes it.
	 *
	 * @param event - The event name to add the listener for.
	 * @param callback - The callback to add.
	 */
	once(event: string, callback: (...args: any[]) => void): this {
		const wrapper = (...args: any[]) => {
			this.removeListener(event, wrapper);
			callback(...args);
		};
		this.store.db.addListener(event, wrapper);
		return this;
	}

	/**
	 * Stores a value for the given key.
	 *
	 * @param key - The key to store the value for.
	 * @param value - The value to store.
	 * @param options - The put options.
	 * @returns The key and value.
	 *
	 * @example
	 * ```typescript
	 * await db.put('a', 'b');
	 * ```
	 */
	async put(key: Key, value: any, options?: PutOptions & T): Promise<void> {
		return this.store.putSync(this._context, key, value, options);
	}

	/**
	 * Synchronously stores a value for the given key.
	 *
	 * @param key - The key to store the value for.
	 * @param value - The value to store.
	 * @param options - The put options.
	 * @returns The key and value.
	 *
	 * @example
	 * ```typescript
	 * db.putSync('a', 'b');
	 * ```
	 */
	putSync(key: Key, value: any, options?: PutOptions & T): void {
		return this.store.putSync(this._context, key, value, options);
	}

	/**
	 * Removes a value for the given key. If the key does not exist, it will
	 * not error.
	 *
	 * @param key - The key to remove the value for.
	 * @param options - The remove options.
	 * @returns The key and value.
	 *
	 * @example
	 * ```typescript
	 * await db.remove('a');
	 * ```
	 */
	async remove(key: Key, options?: T): Promise<void> {
		return this.store.removeSync(this._context, key, options);
	}

	/**
	 * Removes a value for the given key. If the key does not exist, it will
	 * not error.
	 *
	 * @param key - The key to remove the value for.
	 * @param options - The remove options.
	 * @returns The key and value.
	 *
	 * @example
	 * ```typescript
	 * db.removeSync('a');
	 * ```
	 */
	removeSync(key: Key, options?: T): void {
		return this.store.removeSync(this._context, key, options);
	}

	/**
	 * Removes an event listener. You must specify the exact same callback that was
	 * used in `addListener()`.
	 *
	 * @param event - The event name to remove the listener for.
	 * @param callback - The callback to remove.
	 */
	removeListener(event: string, callback: (value?: any) => void): boolean {
		return this.store.db.removeListener(event, callback);
	}

	/**
	 * Get or create a transaction log instance.
	 *
	 * @param name - The name of the transaction log.
	 * @returns The transaction log.
	 */
	useLog(name: string | number): TransactionLog {
		return this.store.useLog(this._context, name);
	}
}
