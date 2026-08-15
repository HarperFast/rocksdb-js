import { RocksDatabase } from '../../src/index.ts';

const path = process.argv[2];
const closeFailure = process.env.ROCKSDB_JS_CLOSE_FAILURE === '1';
const db = RocksDatabase.open(path);
db.putSync('key', 'value');

let destroyError: unknown;
try {
	db.destroy();
} catch (error) {
	destroyError = error;
}
const expectedError = closeFailure
	? 'Injected database close failure'
	: 'Injected database destruction failure';
if (!String(destroyError).includes(expectedError))
	throw new Error(`Expected injected destroy failure, received: ${String(destroyError)}`);

if (closeFailure) {
	const startedAt = Date.now();
	try {
		RocksDatabase.open(path);
		throw new Error('Expected the failed descriptor to remain quarantined');
	} catch (error) {
		if (!String(error).includes(`previous close failed: ${expectedError}`)) throw error;
	}
	if (Date.now() - startedAt >= 1_000)
		throw new Error('Opening a quarantined descriptor waited instead of failing immediately');
	try {
		db.destroy();
		throw new Error('Expected repeated destroy to report the previous close failure');
	} catch (error) {
		if (!String(error).includes(`previous close failed: ${expectedError}`)) throw error;
	}
	process.exit(0);
}

const reopened = RocksDatabase.open(path);
if (reopened.getSync('key') !== 'value')
	throw new Error('Reopen after failed destruction did not preserve the database');
reopened.close();
