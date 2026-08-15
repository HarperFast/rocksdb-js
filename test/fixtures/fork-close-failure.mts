import { RocksDatabase, registryStatus, shutdown } from '../../src/index.ts';

const path = process.argv[2];
const db = RocksDatabase.open(path);
db.putSync('key', 'value');
try {
	db.close();
	throw new Error('Expected close to surface the injected native failure');
} catch (error) {
	if (!String(error).includes('Injected database close failure')) throw error;
}
if (
	registryStatus().find((entry) => entry.path === path)?.closeError !==
	'Injected database close failure'
)
	throw new Error('Registry status did not expose the quarantined close failure');

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
const reopened = RocksDatabase.open(path);
if (reopened.getSync('key') !== 'value')
	throw new Error('Shutdown recovery did not preserve the database');
reopened.destroy();
