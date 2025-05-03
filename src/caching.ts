import { WeakLRUCache } from 'weak-lru-cache';
import { RocksDatabase } from './database.js';

/**
 * This extends the RocksDB class to provide in-memory caching using weak-lru-cache to retain
 * and expire entries. weak-lru-cache actually combines least frequent and least recent usage
 * for entry purging, and then maintains a weak reference to work in conjunction with GC
 * to retain access to entries until they are GC'ed. This class then maintains a shared memory
 * buffer that utilizes a shallow hash-map (like a bloom filter, shallow storage of one entry at
 * each hash slot) with values that are a hash of version of key to verify the freshness on
 * each cache retrieval. This shared buffer can then be invalidated by writes on any thread
 * to ensure correct invalidation.
 * Each entry in the shared memory buffer is one (64-bit) word. The entries are keyed/indexed by
 * a hash of the key. The entry indicates the potential freshness of a value in the cache.
 * An entry with a matching hashed version indicates that the value in the cache is fresh.
 * The size of the cache means that hash collisions will be frequent. And entries can have
 * multiple simultaneous versions when writes are occurring. So we must be careful to not have
 * false positives for freshness, and we have different states of the entries to indicate
 * when caching is not feasible.
 * We hash the key and the version together to get a unique value to check for freshness to
 * avoid false positives (since the entries themselves will frequently be shared by different keys).
 * There are several states for the entries:
 * A leading bit of 0 indicates the entry is a hashed version representing a valid fresh value. A
 * match on this indicates that the value in the cache is fresh. In this state, the entry can also
 * freely be replaced with a new hashed version if another entry is accessed that uses this slot.
 * A leading bit of 1 indicates that the entry cannot be used for caching because there was a recent write.
 * The value of the remaining bits is the timestamp of the last write. We need to wait for the oldest
 * snapshot to pass this timestamp before it can be eligible for caching again.
 * We will periodically check the last snapshot timestamp and update all entries in the cache
 * that are in the state of a recent write, and restore them to the state of allowing caching if
 * the timestamp is old enough.
 * We can interact with the buffer through a Float64 typed array, which is the representation of the version,
 * or through BigInt64, which is necessary for any of the Atomics because for some reason the
 * Atomics won't work with Float64 (I get it for bit operators, but not sure why it doesn't work with
 * the compareExchange).
 * Note that this code assumes little endian platform.
 */

const VERSION_CACHE_SIZE_EXPONENT = 16;
const VERSION_CACHE_SIZE = Math.pow(2, VERSION_CACHE_SIZE_EXPONENT); // 65536
const VERSION_CACHE_SIZE_MASK = VERSION_CACHE_SIZE - 1;
const hashedVersion = new BigInt64Array(1); // this exists for calculating the hashed version from the version number
const versionToHash = new Float64Array(hashedVersion.buffer); // the input to the hash function
// int32 are easier/faster to work with, and we actually just hash the most significant 32-bits (the least significant
// have plenty of entropy already)
const int32ToHash = new Int32Array(hashedVersion.buffer);
const keyAsFloat = new Float64Array(1);
const keyAsInt32 = new Int32Array(keyAsFloat.buffer);
type MaybePromise<T = any> = T | Promise<T>;

const mapGet = Map.prototype.get;
export class CachingRocksDatabase extends RocksDatabase {
	sharedHashedVersions: BigInt64Array;
	sharedRecentWriteVersions: Float64Array;
	cache: WeakLRUCache;
	constructor(dbName, options) {
		super(dbName, options); // whatever super stuff
		// create a cache map for this store
		this.cache = new WeakLRUCache(options.cache);
		// get (or create) the shared buffer for the version/hash cache that is shared across all threads
		let sharedAB = this.getUserSharedBuffer('cache', new SharedArrayBuffer(VERSION_CACHE_SIZE * Float64Array.BYTES_PER_ELEMENT));
		this.sharedHashedVersions = new BigInt64Array(sharedAB);
		this.sharedRecentWriteVersions = new Float64Array(sharedAB);
	}

	get isCaching() {
		return true;
	}

	/**
	 * Hashes a given key and returns the value. It also takes a version number and hashes the key and version
	 * and puts that into the hashed version buffer to be used for the entry values, which is used for slot
	 * verification. It ideally is influenced by different bits than the returned value that is used for
	 * the key. We only modify the most significant 31-bits of the version word because we assume the least
	 * significant already have plenty of separate uniqueness/entropy.
	 * Supports hashing for various key types such as numbers, strings, arrays, and symbols.
	 *
	 * @param {any} key The key to be hashed. Can be a number, string, array, or symbol.
	 * @param {number} [version=0] Optional version number used in the hashing process (last version continues to be hashed if not provided)
	 * @return {number} A 16-bit integer hash generated for the given key
	 * @throws {Error} If an unsupported key type is provided.
	 */
	hashKeyVersion(key: any, version?: number): number {
		if (version !== undefined) {
			// we use the version number as the input to the hash function (which is a float)
			versionToHash[0] = version;
		}
		if (typeof key === 'number') {
			if (key >> 0 === key) {
				// We have a 32-bit integer, which makes it simple.
				// Hash the number against the version with a simple XOR, and make sure to keep the sign bit as a zero
				// this will leave the hashedVersion in a state of having a hashed version (hashed with the key)
				int32ToHash[1] = (int32ToHash[1] ^ key) & 0x7fffffff;
				return key & VERSION_CACHE_SIZE_MASK;
			} else {
				// a floating point representation, work with the constituent 32-bit ints for ease/speed with bitwise operators
				keyAsFloat[0] = key;
				let int0 = keyAsInt32[0]; // least significant int
				int32ToHash[1] = (int32ToHash[1] ^ int0 ^ keyAsFloat[1]) & 0x7fffffff;
				return (int0 >> 16 ^ int0) & VERSION_CACHE_SIZE_MASK; // with floats, the last few bits may not vary, so xor both 16-bit parts
			}
		} else if (typeof key === 'string') {
			// Use FNV-1 & FNV-1a hash algorithm for strings; they preserve different parts of the key, so any collisions in one seem to not coincide with a collision in the other
			let fnv1 = 2166136261; // FNV offset basis
			let fnv1a = 2166136261; // FNV offset basis
			for (let i = 0; i < key.length; i++) {
				let code = key.charCodeAt(i);
				// we very carefully ensure that we _only_ use int32 numbers so that v8 doesn't convert to floats, which is much slower
				fnv1 = Math.imul(fnv1, 435) ^ code;
				fnv1a = Math.imul(fnv1a ^ code, 435);
			}
			// Ensure we stay within 31-bit bounds with a leading zero
			int32ToHash[1] = (int32ToHash[1] ^ fnv1) & 0x7fffffff;
			return fnv1a & VERSION_CACHE_SIZE_MASK;
		} else if (Array.isArray(key)) {
			// hash the array by hashing each element in the array, and XORing the result together
			return key.reduce((a, b) => this.hashKeyVersion(a) ^ this.hashKeyVersion(b), 0);
		} else if (typeof key === 'symbol') {
			// hash the symbol by hashing the string representation of the symbol
			return this.hashKeyVersion(key.toString());
		}
		else {
			throw new Error('Unsupported key type');
		}
	}

	getEntry(id: any, options: any): MaybePromise<Entry> {
		// check if we have this in our cache
		let entry = this.cache.get(id);
		let hashIndex: number;
		if (entry) {
			// first we hash the key so we can check the shared buffer of versions to see if we have a fresh version
			hashIndex = this.hashKeyVersion(id, entry.version ?? 0);
			// the hashed version in the cache to see if it matches
			if (hashedVersion[0] === this.sharedHashedVersions[hashIndex]) {
				// it matches, so we can return the cached value
				return entry;
			}
		}
		let entryResult = super.getEntry(id, options);

		return when(entryResult, (entry) => {
			let existingHashedVersion = this.sharedHashedVersions[hashIndex];
			if (existingHashedVersion >= 0) {
				// there are no recent writes for this slot, this slot can be used for caching
				this.hashKeyVersion(id, entry.version ?? 0); // recalculate the hashed version
				// Atomically and conditionally add the hashed version to the shared buffer.
				// If the hashed version has changed since we last checked (since a write could have taken place
				// since we last checked) the atomic operation should fail
				Atomics.compareExchange(this.sharedHashedVersions, hashIndex, existingHashedVersion, hashedVersion[0]);
				// Note we could verify that exchanged value matches existingHashedVersion before adding to cache, but that
				// is not necessary because it doesn't produce any false positives, and is extremely rare
				this.cache.setValue(id, entry.value, entry.size >> 10);
			}
			return entry;
		});
	}

	putSync(id, value, options) {
		let hashIndex = this.hashKeyVersion(id);
		// we set the shared buffer to indicate that there was a recent write, with the current timestamp
		// so this can't be used for caching until the timestamp is old enough. Note that we negate this
		// so that the leading/sign bit (1) indicates the state of a recent write
		// Do we need to use Atomics.store here? (that would require converting to bigint, so preferably not)
		this.sharedRecentWriteVersions[hashIndex] = -Date.now();
		return super.putSync(id, value, options);
	}

	/**
	 * Refresh the cache after writes have completed. This needs to be called periodically on one of the threads
	 * (maybe once every 10 seconds) to check if any of the entries in the cache are in the state of a
	 * being ready to be used for caching again. This only needs to run on one thread.
	 */
	revalidateAfterWrites() {
		// we need to check the oldest snapshot timestamp and update all entries in the cache
		// that are in the state of a recent write, and restore them to the state of allowing caching if
		// the timestamp is old enough.
		// there is a buffer of 10 seconds to allow for the transaction to be committed, TODO: We need to actually
		// record the longest transaction length, so we can accurately calculate how much time to force between oldest
		// snapshot and the most recent write
		const TRANSACTION_OVERLAP_BUFFER_TIME = 10000;
		// Note that we get the oldest snapshot and then negate it because the leading bit for
		// the recent writes is 1, so we need to make sure that the timestamp is negative
		// TODO: Implement getOldestSnapshotTimestamp, using db.GetProperty(kOldestSnapshotTime, value);
		let oldestSnapshotTimestamp = TRANSACTION_OVERLAP_BUFFER_TIME - this.getOldestSnapshotTimestamp();
		let sharedRecentWriteVersions = this.sharedRecentWriteVersions;
		for (let i = 0; i < VERSION_CACHE_SIZE; i++) {
			if (sharedRecentWriteVersions[i] > oldestSnapshotTimestamp) {
				// this entry is now clear of any contention from snapshots the state of a recent write, so we can update it
				// clear the entry and make it available for caching again
				sharedRecentWriteVersions[i] = 0;
			}
		}
	}
}

// convenience function for handling MaybePromise
function when(awaitable: MaybePromise, callback: (value: any) => any, errback?: (error: Error) => any): MaybePromise {
	if (awaitable && awaitable.then) {
		return errback ?
			awaitable.then(callback, errback) :
			awaitable.then(callback);
	}
	return callback(awaitable);
}