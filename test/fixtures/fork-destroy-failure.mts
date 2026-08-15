import { RocksDatabase } from '../../src/index.ts';

const path = process.argv[2];
const db = RocksDatabase.open(path);
db.putSync('key', 'value');

let destroyError: unknown;
try {
	db.destroy();
} catch (error) {
	destroyError = error;
}
if (!String(destroyError).includes('Injected database destruction failure'))
	throw new Error(`Expected injected destroy failure, received: ${String(destroyError)}`);

const reopened = RocksDatabase.open(path);
if (reopened.getSync('key') !== 'value')
	throw new Error('Reopen after failed destruction did not preserve the database');
reopened.close();
