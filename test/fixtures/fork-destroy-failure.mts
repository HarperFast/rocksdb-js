import { RocksDatabase } from '../../src/index.ts';
import { existsSync } from 'node:fs';

const path = process.argv[2];
const closeFailure = process.env.ROCKSDB_JS_CLOSE_FAILURE === '1';
const flushFailure = process.env.ROCKSDB_JS_CLOSE_FLUSH_FAILURE === '1';
// No WAL: these writes survive a failed destroy only if destroy flushed them.
const db = RocksDatabase.open(path, { disableWAL: true });
const named = RocksDatabase.open(path, { name: 'named', disableWAL: true });
db.putSync('key', 'value');
named.putSync('named-key', 'named-value');
let resolveCloseFailure: (args: unknown[]) => void;
const closeFailureEvent = new Promise<unknown[]>((resolve) => (resolveCloseFailure = resolve));
RocksDatabase.on('database:closeFailed', (...args) => resolveCloseFailure(args));
const nextCloseFailure = () =>
	Promise.race([
		closeFailureEvent,
		new Promise<never>((_, reject) =>
			setTimeout(() => reject(new Error('Destroy did not emit database:closeFailed')), 1_000)
		),
	]);

let destroyError: unknown;
try {
	db.destroy();
} catch (error) {
	destroyError = error;
}

if (flushFailure) {
	// A failed destroy flush does not stop the deletion; it is reported.
	if (destroyError) throw new Error(`Destroy threw on a flush failure: ${String(destroyError)}`);
	if (existsSync(path)) throw new Error('Destroy left the database behind after a flush failure');
	const args = await nextCloseFailure();
	if (!String(args[1]).includes('Failed to flush database during destroy: '))
		throw new Error(`Unexpected database:closeFailed arguments: ${JSON.stringify(args)}`);
	process.exit(0);
}

const expectedError = closeFailure
	? 'Injected database close failure'
	: 'Injected database destruction failure';
if (!String(destroyError).includes(expectedError))
	throw new Error(`Expected injected destroy failure, received: ${String(destroyError)}`);

if (closeFailure) {
	const args = await nextCloseFailure();
	if (args[0] !== path || args[1] !== expectedError) {
		throw new Error(`Unexpected database:closeFailed arguments: ${JSON.stringify(args)}`);
	}
	const startedAt = Date.now();
	const recovery = new RocksDatabase(path);
	try {
		recovery.open();
		throw new Error('Expected the failed descriptor to remain quarantined');
	} catch (error) {
		if (!String(error).includes(`previous close failed: ${expectedError}`)) throw error;
	}
	if (Date.now() - startedAt >= 1_000)
		throw new Error('Opening a quarantined descriptor waited instead of failing immediately');
	recovery.destroy();
	process.exit(0);
}

const reopened = RocksDatabase.open(path);
const reopenedNamed = RocksDatabase.open(path, { name: 'named' });
if (reopened.getSync('key') !== 'value')
	throw new Error('Reopen after failed destruction lost the unflushed default-family write');
if (reopenedNamed.getSync('named-key') !== 'named-value')
	throw new Error('Reopen after failed destruction lost the unflushed named-family write');
reopenedNamed.close();
reopened.close();
