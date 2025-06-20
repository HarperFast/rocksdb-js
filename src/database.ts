import { Transaction } from './transaction.js';
import { DBI, type DBITransactional } from './dbi.js';
import { Store, type StoreOptions } from './store.js';
import { config, type TransactionOptions, type RocksDatabaseConfig } from './load-binding.js';
import * as orderedBinary from 'ordered-binary';
import { IndexStore } from './index-store.js';
import type { Key } from './encoding.js';

interface RocksDatabaseOptions extends StoreOptions {
	dupSort?: boolean;
	name?: string; // defaults to 'default'
};

/**
 * The main class for interacting with a RocksDB database.
 *
 * Before using this class, you must open the database first.
 *
 * @example
 * ```ts
 * const db = await RocksDatabase.open('/path/to/database');
 * await db.put('key', 'value');
 * const value = await db.get('key');
 * db.close();
 * ```
 */
export class RocksDatabase extends DBI<DBITransactional> {
	// #cache: boolean;
	// #useVersions: boolean;

	constructor(
		path: string,
		options?: RocksDatabaseOptions
	) {
		if (options?.dupSort) {
			super(new IndexStore(path, options));
		} else {
			super(new Store(path, options));
		}

		// this.#cache = options?.cache ?? false; // TODO: better name?
		// this.#useVersions = options?.useVersions ?? false; // TODO: better name?

		// if (this.#dupSort && (this.#cache || this.#useVersions)) {
		// 	throw new Error('The dupSort flag can not be combined with versions or caching');
		// }
	}

	/**
	 * In memory lock mechanism for cache resolution.
	 * @param key
	 * @param version
	 */
	attemptLock(_key: Key, _version: number) {
		//
	}

	async clear(): Promise<void> {
		//
	}

	/**
	 * Closes the database.
	 *
	 * @example
	 * ```ts
	 * const db = await RocksDatabase.open('/path/to/database');
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

	getStats() {
		return {
			free: {},
			root: {},
		};
	}

	getUserSharedBuffer(_key: Key, _defaultBuffer?: Buffer) {
		//
	}

	hasLock(_key: Key, _version: number): boolean {
		return false;
	}

	async ifNoExists(_key: Key): Promise<void> {
		//
	}

	async ifVersion(
		_key: Key,
		_version?: number | null,
		_options?: {
			allowNotFound?: boolean;
			ifLessThan?: number;
		}
	): Promise<void> {
		//
	}

	/**
	 * Initializes the encoder.

	 * Note: ideally would go in the `Store` class, but the "structures"
	 * functionality requires access to the high-level data functions.
	 */
	async #initEncoder(): Promise<void> {
		const { store } = this;

		/**
		 * The encoder initialization precedence is:
		 * 1. encoder.Encoder
		 * 2. encoder.encode()
		 * 3. encoding === `msgpack`
		 * 4. encoding === `ordered-binary`
		 * 5. encoder.writeKey()
		 */
		let EncoderClass = store.encoder?.Encoder;
		if (store.encoding === false || typeof EncoderClass === 'function') {
			store.encoder = null;
		} else if (
			typeof store.encoder?.encode !== 'function' &&
			(!store.encoding || store.encoding === 'msgpack')
		) {
			store.encoding = 'msgpack';
			EncoderClass = await import('msgpackr').then(m => m.Encoder);
		}

		if (EncoderClass) {
			const opts: Record<string, any> = {
				copyBuffers: true
			};
			const { sharedStructuresKey } = store;
			if (sharedStructuresKey) {
				opts.getStructures = (): any => {
					// let lastVersion: number;
					// if (this.useVersions) {
					// 	lastVersion = getLastVersion();
					// }
					const buffer = this.getBinarySync(sharedStructuresKey);
					// if (lastVersion) {
					// 	setLastVersion(lastVersion);
					// }
					return buffer && store.decoder?.decode ? store.decoder.decode(buffer) : undefined;
				};
				opts.saveStructures = (structures: any, isCompatible: boolean | ((existingStructures: any) => boolean)) => {
					this.transactionSync((txn: Transaction) => {
						const existingStructuresBuffer = txn.getBinarySync(sharedStructuresKey);
						const existingStructures = existingStructuresBuffer && store.decoder?.decode ? store.decoder.decode(existingStructuresBuffer) : undefined;
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
			store.decoderCopies = !store.encoder.needsStableBuffer;
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
	}

	isOpen() {
		return this.store.isOpen();
	}

	/**
	 * Sugar method for opening a database.
	 *
	 * @param path - The filesystem path to the database.
	 * @param options - The options for the database.
	 * @returns A new RocksDatabase instance.
	 *
	 * @example
	 * ```ts
	 * const db = await RocksDatabase.open('/path/to/database');
	 * ```
	 */
	static async open(
		path: string,
		options?: RocksDatabaseOptions
	): Promise<RocksDatabase> {
		return new RocksDatabase(path, options).open();
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
	 * await db.open();
	 * ```
	 */
	async open(): Promise<RocksDatabase> {
		if (this.store.open()) {
			// already open
			return this;
		}

		await this.#initEncoder();

		return this;
	}

	/**
	 * Executes all operations in the callback as a single transaction.
	 *
	 * @param callback - A async function that receives the transaction as an argument.
	 * @returns A promise that resolves when the transaction is committed or aborted.
	 *
	 * @example
	 * ```ts
	 * const db = await RocksDatabase.open('/path/to/database');
	 * await db.transaction(async (txn) => {
	 *   await txn.put('key', 'value');
	 * });
	 * ```
	 */
	async transaction(callback: (txn: Transaction) => Promise<any>, options?: TransactionOptions) {
		if (typeof callback !== 'function') {
			throw new TypeError('Callback must be a function');
		}

		const txn = new Transaction(this.store, options);

		try {
			const result = await callback(txn);
			await txn.commit();
			return result;
		} catch (error) {
			txn.abort();
			throw error;
		}
	}

	transactionSync(callback: (txn: Transaction) => void, options?: TransactionOptions) {
		if (typeof callback !== 'function') {
			throw new TypeError('Callback must be a function');
		}

		const txn = new Transaction(this.store, options);

		try {
			const result = callback(txn);
			txn.commitSync();
			return result;
		} catch (error) {
			txn.abort();
			throw error;
		}
	}

	unlock(_key: Key, _version: number): boolean {
		return true;
	}
}
