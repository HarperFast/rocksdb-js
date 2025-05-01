import { WeakLRUCache } from 'weak-lru-cache';

/**
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
 * We can interact with the buffer through float64, which is the representation of the version,
 * or through BigInt64, which is necessary for any of the Atomics because for some reason the
 * Atomics won't work with float64 (I get it for bit operators, but not sure why it doesn't work with
 * the compareExchange).
 */

const VERSION_CACHE_SIZE_EXPONENT = 16;
const VERSION_CACHE_SIZE = Math.pow(2, VERSION_CACHE_SIZE_EXPONENT); // 65536
const VERSION_CACHE_SIZE_MASK = VERSION_CACHE_SIZE - 1;
const hashedVersion = new BigInt64Array(1); // this exists for calculating the hashed version from the version number
const versionToHash = new Float64Array(hashedVersion.buffer); // the input to the hash function
// uint32 are easier/faster to work with, and we actually just hash the most significant 32-bits (least significant
// have plenty of entropy already)
const uint32ToHash = new Uint32Array(hashedVersion.buffer);
type MaybePromise<T = any> = T | Promise<T>;

const mapGet = Map.prototype.get;
export const CachingStore = (Store, env) => {
	return class RocksDBStore extends Store {
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

		hashKeyVersion(key: any, version = 0): number {
			versionToHash[0] = version;
			if (typeof key === 'number') {
				// hash the number against the version with a simple XOR, and make sure to keep the sign bit as a zero
				// this will leave the hashedVersion in a state of having a hashed version (hashed with the key)
				uint32ToHash[1] = (uint32ToHash[1] ^ (key / VERSION_CACHE_SIZE)) & 0x7fffffff;
				return key & VERSION_CACHE_SIZE_MASK;
			} else if (typeof key === 'string') {
				// TODO: hash the string and all other types
			}
		}

		getEntry(id: any, options: any): MaybePromise<Entry> {
			// check if we have this in our cache
			let entry = this.cache.get(id);
			let hashIndex: number;
			if (entry) {
				// first we hash the key so we can check the shared buffer of versions to see if we have a fresh version
				hashIndex = this.hashKeyVersion(id, entry.version);
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
					this.hashKeyVersion(id, entry.version); // recalculate the hashed version
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
			// so that the leading/sign bit indicates the state of a recent write
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
};

// convenience function for handling MaybePromise
function when(awaitable: MaybePromise, callback: (value: any) => any, errback?: (error: Error) => any): MaybePromise {
	if (awaitable && awaitable.then) {
		return errback ?
			awaitable.then(callback, errback) :
			awaitable.then(callback);
	}
	return callback(awaitable);
}
