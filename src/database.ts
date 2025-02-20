import { NativeDatabase } from './util/load-binding.js';
import { RocksStore, type RocksStoreOptions } from './store.js';

export type RocksDatabaseOptions = {
	path: string;
	useVersions?: boolean;
};

export const IF_EXISTS = Symbol('IF_EXISTS');

/**
 * This class is the public API. It exposes the internal native `Database` class.
 *
 * lmdb-js would call this the environment. It contains multiple "databases" (namespaced stores?).
 */
export class RocksDatabase extends RocksStore {
	constructor(
		optionsOrPath: string | RocksDatabaseOptions,
		options?: RocksDatabaseOptions
	) {
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

		super(new NativeDatabase(path));
	}

	close() {
		this.db.close();
	}

	static async open(
		optionsOrPath: string | RocksDatabaseOptions,
		options?: RocksDatabaseOptions
	): Promise<RocksDatabase> {
		const db = new RocksDatabase(optionsOrPath, options);
		await db.open();
		return db;
	}

	async open(): Promise<RocksDatabase> {
		await this.init();
		return this;
	}

	async openStore(options?: RocksStoreOptions): Promise<RocksStore> {
		// TODO: dbName?

		const store = new RocksStore(this.db, options);
		return store.init();
	}
}
