import { Transaction } from './transaction.js';
import { DBI, type DBITransactional } from './dbi.js';
import { Store, type StoreOptions } from './store.js';
import { config, type TransactionOptions, type RocksDatabaseConfig } from './load-binding.js';
import * as orderedBinary from 'ordered-binary';
import { Encoder as MsgpackEncoder } from 'msgpackr';
import type { EncoderFunction, Key } from './encoding.js';

export interface RocksDatabaseOptions extends StoreOptions {
	/**
	 * The column family name.
	 *
	 * @default 'default'
	 */
	name?: string;

	/**
	 * A custom store.
	 */
	store?: Store;
};

/**
 * The main class for interacting with a RocksDB database.
 *
 * Before using this class, you must open the database first.
 *
 * @example
 * ```ts
 * const db = RocksDatabase.open('/path/to/database');
 * await db.put('key', 'value');
 * const value = await db.get('key');
 * db.close();
 * ```
 */
export class RocksDatabase extends DBI<DBITransactional> {
	constructor(
		pathOrStore: string | Store,
		options?: RocksDatabaseOptions
	) {
		if (typeof pathOrStore === 'string') {
			super(new Store(pathOrStore, options));
		} else if (pathOrStore instanceof Store) {
			super(pathOrStore);
		} else {
			throw new TypeError('Invalid database path or store');
		}
	}

	async clear(): Promise<void> {
		//
	}

	/**
	 * Closes the database.
	 *
	 * @example
	 * ```ts
	 * const db = RocksDatabase.open('/path/to/database');
	 * db.close();
	 * ```
	 */
	close() {
		this.store.close();
	}

	/**
	 * Set global database settings.
	 *
	 * @param options - The options for the database.
	 *
	 * @example
	 * ```ts
	 * RocksDatabase.config({ blockCacheSize: 1024 * 1024 });
	 * ```
	 */
	static config(options: RocksDatabaseConfig): void {
		config(options);
	}

	// committed

	async drop(): Promise<void> {
		//
	}

	dropSync() {
		//
	}

	// flushed

	/**
	 * Returns a number representing a unix timestamp of the oldest unreleased
	 * snapshot.
	 *
	 * @returns The oldest snapshot timestamp.
	 */
	getOldestSnapshotTimestamp() {
		return this.store.db.getOldestSnapshotTimestamp();
	}

	getStats() {
		return {
			free: {},
			root: {},
		};
	}

	getUserSharedBuffer(_key: Key, _defaultBuffer?: Buffer) {
		//
	}

	/**
	 * Returns whether the database has a lock for the given key.
	 *
	 * @param key - The key to check.
	 * @returns `true` if the database has a lock for the given key, `false`
	 * otherwise.
	 *
	 * @example
	 * ```ts
	 * const db = RocksDatabase.open('/path/to/database');
	 * db.hasLock('foo'); // false
	 * db.tryLock('foo', () => {});
	 * db.hasLock('foo'); // true
	 * ```
	 */
	hasLock(key: Key): boolean {
		return this.store.hasLock(key);
	}

	async ifNoExists(_key: Key): Promise<void> {
		//
	}

	/**
	 * Returns whether the database is open.
	 *
	 * @returns `true` if the database is open, `false` otherwise.
	 */
	isOpen() {
		return this.store.isOpen();
	}

	/**
	 * Sugar method for opening a database.
	 *
	 * @param pathOrStore - The filesystem path to the database or a custom store.
	 * @param options - The options for the database.
	 * @returns A new RocksDatabase instance.
	 *
	 * @example
	 * ```ts
	 * const db = RocksDatabase.open('/path/to/database');
	 * ```
	 */
	static open(
		pathOrStore: string | Store,
		options?: RocksDatabaseOptions
	): RocksDatabase {
		return new RocksDatabase(pathOrStore, options).open();
	}

	/**
	 * Opens the database. This function returns immediately if the database is
	 * already open.
	 *
	 * @returns A new RocksDatabase instance.
	 *
	 * @example
	 * ```ts
	 * const db = new RocksDatabase('/path/to/database');
	 * db.open();
	 * ```
	 */
	open(): RocksDatabase {
		const { store } = this;

		if (store.open()) {
			// already open
			return this;
		}

		/**
		 * The encoder initialization precedence is:
		 * 1. encoder.Encoder
		 * 2. encoder.encode()
		 * 3. encoding === `msgpack`
		 * 4. encoding === `ordered-binary`
		 * 5. encoder.writeKey()
		 */
		let EncoderClass: EncoderFunction | undefined = store.encoder?.Encoder;
		if (store.encoding === false) {
			store.encoder = null;
			EncoderClass = undefined;
		} else if (typeof EncoderClass === 'function') {
			store.encoder = null;
		} else if (
			typeof store.encoder?.encode !== 'function' &&
			(!store.encoding || store.encoding === 'msgpack')
		) {
			store.encoding = 'msgpack';
			EncoderClass = MsgpackEncoder;
		}

		if (EncoderClass) {
			const opts: Record<string, any> = {
				copyBuffers: true
			};
			const { sharedStructuresKey } = store;
			if (sharedStructuresKey) {
				opts.getStructures = (): any => {
					const buffer = this.getBinarySync(sharedStructuresKey);
					return buffer && store.decoder?.decode ? store.decoder.decode(buffer) : undefined;
				};
				opts.saveStructures = (structures: any, isCompatible: boolean | ((existingStructures: any) => boolean)) => {
					this.transactionSync((txn: Transaction) => {
						// note: we need to get a fresh copy of the shared structures,
						// so we don't want to use the transaction's getBinarySync()
						const existingStructuresBuffer = this.getBinarySync(sharedStructuresKey);
						const existingStructures = existingStructuresBuffer && store.decoder?.decode
							? store.decoder.decode(existingStructuresBuffer)
							: undefined;
						if (typeof isCompatible == 'function') {
							if (!isCompatible(existingStructures)) {
								return false;
							}
						} else if (existingStructures && existingStructures.length !== isCompatible) {
							return false;
						}
						txn.putSync(sharedStructuresKey, structures);
					});
				};
			}
			store.encoder = new EncoderClass({
				...opts,
				...store.encoder
			});
			store.decoder = store.encoder;
		} else if (typeof store.encoder?.encode === 'function') {
			if (!store.decoder) {
				store.decoder = store.encoder;
			}
		} else if (store.encoding === 'ordered-binary') {
			store.encoder = {
				readKey: orderedBinary.readKey,
				writeKey: orderedBinary.writeKey,
			};
			store.decoder = store.encoder;
		}

		if (typeof store.encoder?.writeKey === 'function' && !store.encoder?.encode) {
			// define a fallback encode method that uses writeKey to encode values
			store.encoder = {
				...store.encoder,
				encode: (value: any, _mode?: number): Buffer => {
					const bytesWritten = store.writeKey(value, store.encodeBuffer, 0);
					return store.encodeBuffer.subarray(0, bytesWritten);
				}
			};
			store.encoder.copyBuffers = true;
		}

		if (store.decoder?.needsStableBuffer !== true) {
			store.decoderCopies = true;
		}

		if (store.decoder?.readKey && !store.decoder.decode) {
			store.decoder.decode = (buffer: Buffer): any => {
				if (store.decoder?.readKey) {
					return store.decoder.readKey(buffer, 0, buffer.length);
				}
				return buffer;
			};
			store.decoderCopies = true;
		}

		return this;
	}

	get encoder() {
		return this.store.encoder;
	}

	get path() {
		return this.store.path;
	}

	/**
	 * Executes all operations in the callback as a single transaction.
	 *
	 * @param callback - A async function that receives the transaction as an argument.
	 * @returns A promise that resolves the `callback` return value.
	 *
	 * @example
	 * ```ts
	 * const db = RocksDatabase.open('/path/to/database');
	 * await db.transaction(async (txn) => {
	 *   await txn.put('key', 'value');
	 * });
	 * ```
	 */
	async transaction(callback: (txn: Transaction) => PromiseLike<any>, options?: TransactionOptions) {
		if (typeof callback !== 'function') {
			throw new TypeError('Callback must be a function');
		}

		const txn = new Transaction(this.store, options);

		try {
			this.emit('begin-transaction');
			const result = await callback(txn);
			await txn.commit();
			return result;
		} catch (error) {
			txn.abort();
			throw error;
		}
	}

	/**
	 * Executes all operations in the callback as a single transaction.
	 *
	 * @param callback - A function that receives the transaction as an
	 * argument. If the callback return promise-like value, it is awaited
	 * before committing the transaction. Otherwise, the callback is treated as
	 * synchronous.
	 * @returns The `callback` return value.
	 *
	 * @example
	 * ```ts
	 * const db = RocksDatabase.open('/path/to/database');
	 * await db.transaction(async (txn) => {
	 *   await txn.put('key', 'value');
	 * });
	 * ```
	 */
	transactionSync<T>(callback: (txn: Transaction) => T | PromiseLike<T>, options?: TransactionOptions): T | PromiseLike<T> {
		if (typeof callback !== 'function') {
			throw new TypeError('Callback must be a function');
		}

		const txn = new Transaction(this.store, options);

		try {
			this.emit('begin-transaction');
			const result = callback(txn);
			let committed = false;

			// despite being 'sync', we need to support async operations
			if (result && typeof result === 'object' && 'then' in result && typeof result.then === 'function') {
				return result.then((value) => {
					if (committed) {
						throw new Error('Transaction already committed');
					}
					committed = true;
					txn.commitSync();
					return value as T;
				});
			}

			txn.commitSync();
			return result;
		} catch (error) {
			txn.abort();
			throw error;
		}
	}

	/**
	 * Attempts to acquire a lock for a given key. If the lock is available,
	 * the function returns `true` and the optional callback is never called.
	 * If the lock is not available, the function returns `false` and the
	 * callback is queued until the lock is released.
	 *
	 * @param key - The key to lock.
	 * @param onUnlocked - A callback to call when the lock is released.
	 *
	 * @example
	 * ```ts
	 * const db = RocksDatabase.open('/path/to/database');
	 * db.tryLock('foo', () => {
	 *   console.log('lock acquired');
	 * });
	 * ```
	 * @returns `true` if the lock was acquired, `false` otherwise.
	 */
	tryLock(key: Key, onUnlocked?: () => void): boolean {
		return this.store.tryLock(key, onUnlocked);
	}

	/**
	 * Releases the lock on the given key and calls any queued `onUnlocked`
	 * callback handlers.
	 *
	 * @param key - The key to unlock.
	 * @returns `true` if the lock was released or `false` if the lock did not
	 * exist.
	 *
	 * @example
	 * ```ts
	 * const db = RocksDatabase.open('/path/to/database');
	 * db.tryLock('foo', () => {});
	 * db.unlock('foo'); // returns `true`
	 * db.unlock('foo'); // already unlocked, returns `false`
	 * ```
	 */
	unlock(key: Key): void {
		return this.store.unlock(key);
	}

	/**
	 * Excecutes a function using a thread-safe lock to ensure mutual
	 * exclusion.
	 *
	 * @param callback - A callback to call when the lock is acquired.
	 * @returns A promise that resolves when the lock is acquired.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * await db.withLock(async (waited) => {
	 *   console.log('lock acquired', waited);
	 * });
	 * ```
	 */
	async withLock(key: Key, callback: () => void | Promise<void>): Promise<void> {
		return this.store.withLock(key, callback);
	}
}
