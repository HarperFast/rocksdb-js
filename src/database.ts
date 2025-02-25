import { Database, NativeDatabase } from './util/load-binding.js';
import { RocksStore, type RocksStoreOptions } from './store.js';

export interface RocksDatabaseOptions extends RocksStoreOptions {
	path?: string;
	useVersions?: boolean;
}

/**
 * This class is the public API. It exposes the internal native `Database` class.
 *
 * lmdb-js would call this the environment. It contains multiple "databases" (namespaced stores?).
 */
export class RocksDatabase extends RocksStore {
	#parallelism?: number;

	constructor(
		optionsOrPath: string | RocksDatabaseOptions,
		options?: RocksDatabaseOptions
	) {
		if (!optionsOrPath) {
			throw new Error('Options or path is required');
		}

		let path: string | undefined;

		if (typeof optionsOrPath === 'string') {
			path = optionsOrPath;
		} else if (optionsOrPath && typeof optionsOrPath === 'object') {
			options = optionsOrPath;
			path = options.path;
		}
		if (!path) {
			throw new TypeError('Invalid options or path');
		}

		super(new NativeDatabase(path), options);

		this.#parallelism = options?.parallelism;
	}

	close() {
		this.db.close();
	}

	static async open(
		optionsOrPath: string | RocksDatabaseOptions,
		options?: RocksDatabaseOptions
	): Promise<RocksDatabase> {
		const db = new RocksDatabase(optionsOrPath, options);
		return db.open();
	}

	async open(): Promise<RocksDatabase> {
		if (this.db.opened) {
			return this;
		}

		this.db.open({
			parallelism: this.#parallelism
		});

		// init the root store
		await this.init();

		return this;
	}

	// async openStore(options?: RocksStoreOptions): Promise<RocksStore> {
	// 	// TODO: dbName?

	// 	const store = new RocksStore(options);
	// 	return store.open();
	// }
}
