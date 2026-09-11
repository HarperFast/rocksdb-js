import { RocksDatabase, registryStatus, shutdown } from '../../src/index.ts';

const path = process.argv[2];
const db = RocksDatabase.open(path, { disableWAL: true });
db.putSync('key', 'unflushed');

try {
	db.close();
	throw new Error('Expected close to surface the injected flush failure');
} catch (error) {
	if (!String(error).includes('Injected database close flush failure')) throw error;
}
if (db.isOpen()) throw new Error('A flush-failed database still reports itself open');
if (!registryStatus().some((entry) => entry.path === path && entry.closeError))
	throw new Error('Flush failure did not quarantine the descriptor');
try {
	RocksDatabase.open(path);
	throw new Error('Expected the flush failure to block reopen');
} catch (error) {
	if (!String(error).includes('previous close failed')) throw error;
}

shutdown();
const reopened = RocksDatabase.open(path);
if (reopened.getSync('key') !== 'unflushed')
	throw new Error('Shutdown retry did not preserve the unflushed write');
reopened.destroy();
