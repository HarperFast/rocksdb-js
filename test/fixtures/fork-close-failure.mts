import { RocksDatabase, registryStatus, shutdown } from '../../src/index.ts';

const path = process.argv[2];
const db = RocksDatabase.open(path);
db.putSync('key', 'value');
db.close();

const startedAt = Date.now();
try {
	RocksDatabase.open(path);
	throw new Error('Expected the failed automatic close to quarantine the path');
} catch (error) {
	if (!String(error).includes('previous close failed: Injected database close failure'))
		throw error;
}
if (Date.now() - startedAt >= 1_000)
	throw new Error('Open waited instead of reporting the failed automatic close immediately');

delete process.env.ROCKSDB_JS_CLOSE_FAILURE;
shutdown();
if (registryStatus().some((entry) => entry.path === path))
	throw new Error('Shutdown retry did not clear the quarantined automatic close');
