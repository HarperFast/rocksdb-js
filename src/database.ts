import type { Key } from './types.js';
import { Transaction } from './transaction.js';
import { DBI, type DBITransactional } from './dbi.js';
import { Store, type StoreOptions } from './store.js';

interface RocksDatabaseOptions extends StoreOptions {
	cache?: boolean;
	dupSort?: boolean;
	name?: string; // defaults to 'default'
	parallelism?: number;
	useVersions?: boolean;
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
	// #dupSort: boolean;
	// #useVersions: boolean;

	constructor(
		path: string,
		options?: RocksDatabaseOptions
	) {
		if (!path || typeof path !== 'string') {
			throw new TypeError('Path is required');
		}

		if (options !== undefined && typeof options !== 'object') {
			throw new TypeError('Options must be an object');
		}

		const store = new Store(path, options);

		super(store);

		// this.#cache = options?.cache ?? false; // TODO: better name?
		// this.#dupSort = options?.dupSort ?? false; // TODO: better name?
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
		await this.store.open();
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
	async transaction(callback: (txn: Transaction) => Promise<void>) {
		if (typeof callback !== 'function') {
			throw new TypeError('Callback must be a function');
		}

		const txn = new Transaction(this.store);

		try {
			await callback(txn);
			txn.commit();
		} catch (error) {
			txn.abort();
			throw error;
		}
	}

	unlock(_key: Key, _version: number): boolean {
		//

		return true;
	}
}
