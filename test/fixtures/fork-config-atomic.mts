// config() settings are process-global, so each mode starts from a fresh process.
import { getWriteBufferManagerStats, RocksDatabase } from '../../src/index.ts';

const [dbPath, mode] = process.argv.slice(2);
if (!dbPath || (mode !== 'beforeOpen' && mode !== 'live')) {
	console.error('Usage: fork-config-atomic.mts <dbPath> <beforeOpen|live>');
	process.exit(1);
}

const MiB = 1024 * 1024;
const result: Record<string, unknown> = {};

function rejectedWith(options: object): string {
	try {
		RocksDatabase.config(options as any);
	} catch (error) {
		return `${(error as Error).name}: ${(error as Error).message}`;
	}
	return 'not rejected';
}

function wbm() {
	const { allowStall, bufferSize, costToCache, enabled, flushPolicy } =
		getWriteBufferManagerStats();
	return { allowStall, bufferSize, costToCache, enabled, flushPolicy };
}

if (mode === 'beforeOpen') {
	result.lifecycleRejected = rejectedWith({
		blockCacheSize: 1 * MiB,
		writeBufferManagerSize: 8 * MiB,
		writeBufferManagerCostToCache: true,
		writeBufferManagerAllowStall: true,
		lifecycleWaitSeconds: -1,
	});
	result.vtRejected = rejectedWith({
		blockCacheSize: 1 * MiB,
		writeBufferManagerSize: 8 * MiB,
		verificationTableEntries: -1,
	});
	result.getterRejected = rejectedWith({
		blockCacheSize: 1 * MiB,
		writeBufferManagerSize: 8 * MiB,
		writeBufferManagerAllowStall: true,
		get verificationTableEntries() {
			throw new Error('getter threw');
		},
	});
	result.settings = wbm();

	const db = RocksDatabase.open(dbPath);
	db.putSync('k', 'v');
	result.blockCacheCapacity = db.getStats()['rocksdb.block-cache-capacity'];
	result.afterOpen = wbm();
	db.close();
} else {
	RocksDatabase.config({ writeBufferManagerSize: 16 * MiB });
	const db = RocksDatabase.open(dbPath, { verificationTable: true });
	db.putSync('k', 'v');
	result.created = wbm();
	result.stallRejected = rejectedWith({
		writeBufferManagerAllowStall: true,
		lifecycleWaitSeconds: -1,
	});
	result.vtRejected = rejectedWith({
		writeBufferManagerSize: 32 * MiB,
		writeBufferManagerAllowStall: true,
		writeBufferManagerFlushPolicy: 'largest',
		verificationTableEntries: 1024,
	});
	result.costToCacheRejected = rejectedWith({
		writeBufferManagerSize: 32 * MiB,
		writeBufferManagerAllowStall: true,
		writeBufferManagerCostToCache: true,
	});
	result.afterRejected = wbm();

	RocksDatabase.config({
		writeBufferManagerSize: 32 * MiB,
		writeBufferManagerAllowStall: true,
		writeBufferManagerFlushPolicy: 'largest',
	});
	result.applied = wbm();
	RocksDatabase.config({ writeBufferManagerAllowStall: false });
	db.close();
}

console.log(`CONFIG ${JSON.stringify(result)}`);
