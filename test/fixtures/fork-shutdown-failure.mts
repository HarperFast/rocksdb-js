import { RocksDatabase, registryStatus, shutdown } from '../../src/index.ts';

const path = process.argv[2];
const db = RocksDatabase.open(path);
db.putSync('key', 'value');

try {
	shutdown();
	throw new Error('Expected shutdown to surface the injected close failure');
} catch (error) {
	if (!String(error).includes('Injected database close failure')) throw error;
}

const startedAt = Date.now();
try {
	RocksDatabase.open(path, { readOnly: true });
	throw new Error('Expected the failed path to remain quarantined across open modes');
} catch (error) {
	if (!String(error).includes('previous close failed: Injected database close failure'))
		throw error;
}
if (Date.now() - startedAt >= 1_000)
	throw new Error('Cross-mode open waited instead of reporting the quarantined path immediately');

delete process.env.ROCKSDB_JS_CLOSE_FAILURE;
shutdown();
if (registryStatus().some((entry) => entry.path === path))
	throw new Error('Shutdown retry did not clear the quarantined descriptor');
