// Probably destructure and get all the named functions directly from the bindings directly
// import binding from './util/load-binding.js';


import type { Transaction } from './transaction';

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
	transaction: Transaction;
	noDupData?: boolean;
	noOverwrite?: boolean;
	version?: number;
};
// global buffer for reading values that can be reused without recreating new buffers
const READ_VALUE_BUFFER = Buffer.allocUnsafeSlow(0x10000);
// global buffer for write keys that can be reused without recreating new buffers
const KEY_BUFFER= Buffer.allocUnsafeSlow(0x1000);

export const IF_EXISTS = Symbol('IF_EXISTS');

/**
 * This class is the public API. It exposes the internal native `Database` class.
 *
 * lmdb-js would call this the environment. It contains multiple databases.
 */
export class RocksDatabase {
	#path: string;
	#dbPointer: number; // pointer to RocksDB native C++ instance

	constructor(optionsOrPath: string | RocksDatabaseOptions, options?: RocksDatabaseOptions) {
		if (!optionsOrPath) {
			throw new Error('Options or path is required');
		}

		if (typeof optionsOrPath === 'string') {
			this.#path = optionsOrPath;
		} else if (optionsOrPath && typeof optionsOrPath === 'object') {
			options = optionsOrPath;
			this.#path = options.path;
		} else {
			throw new TypeError('Invalid options or path');
		}

		//TODO: this.#address = binding.db_open(...);
	}
	openStore(options): RocksStore {
		// TODO: get or create the RocksStore instance that wraps a native ColumnFamily instance
		// TODO: if caching is enabled, we will need to do: `new (CachingStore(RocksStore, db))(options)` to extend the RocksStore with caching
	}
	transactionStore: RockStore // TODO: We may want to expose the transaction log as a store itself, that we can read and write from
	startTxn(): Transaction {
		let handle = binding.start_txn(this.#dbPointer);
		return new Transaction(handle);
	}
}

// TODO: Move to a separate module?
/**
 * This is a key-value store, which wraps a RocksDB ColumnFamily 
 */
export class RocksStore {
	#dbPointer: number; // pointer to RocksDB native C++ instance
	#columnFamilyPointer: number; // pointer to RocksDB FamilyColumn C++ instance
	#useVersions: boolean;
	db: RocksDatabase;
	writeKey: (key: Key, buffer: Buffer, start: number) => void;
	/**
	 * In memory lock mechanism for cache resolution.
	 * @param key 
	 * @param version 
	 */
	attemptLock(key: Key, version: number) {
		// TODO: should be copied from/identical to lmdb-js (there is no interaction with LMDB or RocksDB)
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

	get(key: Key, options?: GetOptions): Promise<any> | any {
		// TODO: Call this.getBinaryFast(key, options) and decode the bytes into a value/object
		return Buffer.from('TODO');
	}

	/**
	 * Calls `getBinaryFast()`. Used by HDBreplication
	 */
	async getBinary(key: Key, options?: GetOptions): Promise<Buffer | undefined> {
		return Buffer.from('TODO');
	}

	async getBinaryFast(key: Key, options?: GetOptions): Buffer | undefined | Promise<Buffer | undefined> {
		const keyLength = this.writeKey(key, KEY_BUFFER, 0);
		const result = binding.get_from_cache(this.#columnFamilyPointer, keyLength, options.transaction.transactionPointer);
		// If we were able to retrieve the value form RocksDB using only caching read tier (and the value fits in the global read buffer), we can immediately return synchronously
		if (result >= 0) {
			READ_VALUE_BUFFER.length = result;
			return READ_VALUE_BUFFER;
		}
		// TODO: db_get_from_cache may sometimes be able to definitely say that the key doesn't exist, in which we should immediately return undefined
		return new Promise((resolve, reject) =>
			binding.get_async(this.#columnFamilyPointer, keyLength, options.transaction.transactionPointer, (value: Buffer, error: string) => {
				resolve(value);
			})
		);
	}

	getEntry(key: Key, options?: GetOptions) {
		const value = this.get(key, options);
		if (value === undefined) {
			return;
		}

		if (this.#useVersions) {
			return {
				value,
				// version: getLastVersion(),
			};
		}

		return {
			value,
		};
	}

	getKeys(options?: GetRangeOptions) {
		//
	}

	getRange(options?: GetRangeOptions) {
		// we will want to use the RangeIterable from lmdb-js, as implements all the iteration functions
		let iterable = new RangeIterable();
		iterable.iterate = () => {
			// initialize a RocksDB iterator and provide iteration
		};
	}

	getStats() {
		return {
			free: {},
			root: {},
		};
	}

	getUserSharedBuffer(key: Key, defaultBuffer?: Buffer) {
		// TODO: Should be directly ported over
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
		// ifNoExists and ifVersion probably aren't necessary because in a world with true transactions where the versions
		// can be programmatically checked in a transaction without requiring it to be submitted as an instruction
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

	async prefetch(keys: Key[]) {
		// This is probably unnecessary in RocksDB.
		// This was done in lmdb-js as a mechanism for async gets and to avoid the I/O costs of reading pages that need to be modified for writes.
		// With true async gets, prefetch isn't necessary for that. And it is not clear if writes in rocksdb even needs to do any reads to find tree branches/leaves.
	}


	put(key: Key, value: Buffer, options: PutOptions) {
		// TODO: pass in the address and length of the binary data that was encoded
		const keyLength = this.writeKey(key, KEY_BUFFER, 0);
		const arrayBuffer = value.buffer;
		const pointer = (arrayBuffer.pointer ?? (arrayBuffer.pointer = binding.getBufferAddress(value))) + value.offset;

		// this probably can be a synchronous function, at least if we are using a write policy of WriteCommitted, since this
		// means that writes are relatively cheap and most of the work is done in the commit phase (which will always be async)
		// we might reconsider an async put, if we use a different write policy
		// TODO: implement this in the binding
		binding.put(this.#columnFamilyPointer, keyLength, options.transaction.transactionPointer, valueBuffer.pointer , valueBuffer.length)
	}

	putSync(key: Key, value: string | Buffer, options?: PutOptions) {
		// if put is sync, don't need this, or can just call this.put()
	}


	remove(key: Key, ifVersionOrValue?: symbol | number | null) {
		// This should be similar to put, except no need to pass in the value
	}

	unlock(key: Key, version: number): boolean {
		//

		return true;
	}

}
