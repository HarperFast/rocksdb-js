import { Database } from './util/load-binding.js';
import { Transaction } from './transaction';

export type RocksDatabaseOptions = {
	path: string;
	useVersions?: boolean;
};

export type Key = string | number | Buffer;

type GetOptions = {
	ifNotTxnId?: number;
	transaction?: Transaction;
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
	transaction?: Transaction;
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

export const IF_EXISTS = Symbol('IF_EXISTS');

/**
 * This class is the public API. It exposes the internal native `Database` class.
 *
 * lmdb-js would call this the environment. It contains multiple databases.
 */
export class RocksDatabase {
	#db: any;

	constructor(optionsOrPath: string | RocksDatabaseOptions, options?: RocksDatabaseOptions) {
		if (!optionsOrPath) {
			throw new Error('Options or path is required');
		}

		let path: string;

		if (typeof optionsOrPath === 'string') {
			path = optionsOrPath;
		} else if (optionsOrPath && typeof optionsOrPath === 'object') {
			options = optionsOrPath;
			path = options.path;
		} else {
			throw new TypeError('Invalid options or path');
		}

		this.#db = new Database();
		this.#db.open(path);
	}

	/**
	 * In memory lock mechanism for cache resolution.
	 * @param key 
	 * @param version 
	 */
	attemptLock(key: Key, version: number) {
		//
	}

	async clear(): Promise<void> {
		//
	}
	
	async close() {
		return this;
	}

	// committed

	doesExist(key: Key, versionOrValue: number | Buffer) {
		//
	}

	async drop(): Promise<void> {
		//
	}

	dropSync() {
		//
	}

	// flushed

	async get(key: Key, options?: GetOptions): Promise<Buffer | undefined> {
		// TODO: Remove async?
		// TODO: Return Promise<any> | any?
		// TODO: Call this.getBinaryFast(key, options) and decode the bytes into a value/object
		return Buffer.from('TODO');
	}

	/**
	 * Calls `getBinaryFast()`. Used by HDBreplication
	 */
	async getBinary(key: Key, options?: GetOptions): Promise<Buffer | undefined> {
		// TODO: singleton buffer (default 64KB) that is reused to hold the data from any immediate returns
		return Buffer.from('TODO');
	}

	async getBinaryFast(key: Key, options?: GetOptions): Promise<Buffer | undefined> {
		return Buffer.from('TODO');
	}

	getEntry(key: Key, options?: GetOptions) {
		const value = this.get(key, options);

		if (value === undefined) {
			return;
		}

		return {
			value,
		};
	}

	getKeys(options?: GetRangeOptions) {
		//
	}

	getRange(options?: GetRangeOptions) {
		//
	}

	getStats() {
		return {
			free: {},
			root: {},
		};
	}

	getUserSharedBuffer(key: Key, defaultBuffer?: Buffer) {
		//
	}

	getValues(key: Key, options?: GetRangeOptions) {
		//
	}

	getValuesCount(key: Key, options?: GetRangeOptions) {
		//
	}

	hasLock(key: Key, version: number): boolean {
		return false;
	}

	async ifNoExists(key: Key): Promise<void> {
		//
	}

	async ifVersion(
		key: Key,
		version?: number | null,
		options?: {
			allowNotFound?: boolean;
			ifLessThan?: number;
		}
	): Promise<void> {
		//
	}

	put(key: Key, value: string | Buffer, options?: PutOptions) {
		//
	}

	remove(key: Key, ifVersionOrValue?: symbol | number | null) {
		// This should be similar to put, except no need to pass in the value
	}

	async transaction(callback: (txn: Transaction) => Promise<void>) {
		const txn = new Transaction();
		try {
			await callback(txn);
			await txn.commit();
		} finally {
			await txn.abort();
		}
	}

	unlock(key: Key, version: number): boolean {
		//

		return true;
	}
}
