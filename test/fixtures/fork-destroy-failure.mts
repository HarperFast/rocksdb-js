import { RocksDatabase } from '../../src/index.ts';

const path = process.argv[2];
const closeFailure = process.env.ROCKSDB_JS_CLOSE_FAILURE === '1';
const db = RocksDatabase.open(path);
db.putSync('key', 'value');
let resolveCloseFailure: (args: unknown[]) => void;
const closeFailureEvent = new Promise<unknown[]>((resolve) => (resolveCloseFailure = resolve));
RocksDatabase.on('database:closeFailed', (...args) => resolveCloseFailure(args));

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
	const args = await Promise.race([
		closeFailureEvent,
		new Promise<never>((_, reject) =>
			setTimeout(() => reject(new Error('Destroy did not emit database:closeFailed')), 1_000)
		),
	]);
	if (args[0] !== path || args[1] !== expectedError) {
		throw new Error(`Unexpected database:closeFailed arguments: ${JSON.stringify(args)}`);
	}
	const startedAt = Date.now();
	try {
		RocksDatabase.open(path);
		throw new Error('Expected the failed descriptor to remain quarantined');
	} catch (error) {
		if (!String(error).includes(`previous close failed: ${expectedError}`)) throw error;
	}
	if (Date.now() - startedAt >= 1_000)
		throw new Error('Opening a quarantined descriptor waited instead of failing immediately');
	delete process.env.ROCKSDB_JS_CLOSE_FAILURE;
	db.destroy();
	process.exit(0);
}

const reopened = RocksDatabase.open(path);
if (reopened.getSync('key') !== 'value')
	throw new Error('Reopen after failed destruction did not preserve the database');
reopened.close();
