import { RocksDatabase, Transaction, TransactionLoggedError } from '../../src/index.ts';
import { constants, forceTryAgainForTesting } from '../../src/load-binding.ts';
import { readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';

// The conflict-before-log protocol under one commit execution mode
// (ROCKSDB_JS_COMMIT_THREAD, read once per process) and one OCC validation
// policy (RocksDatabase.config before the first open).
//   argv: <dbPath> <async|sync> <parallel|serial>
const [dbPath, mode, policy] = process.argv.slice(2);
if (!['async', 'sync'].includes(mode) || !['parallel', 'serial'].includes(policy)) {
	throw new Error('usage: fork-conflict-before-log.mts <dbPath> <async|sync> <parallel|serial>');
}

function assert(condition: unknown, message: string): asserts condition {
	if (!condition) {
		throw new Error(`fixture assertion failed: ${message}`);
	}
}

function logBytes(name: string): number {
	const dir = join(dbPath, 'transaction_logs', name);
	let size = 0;
	for (const file of readdirSync(dir)) {
		size += statSync(join(dir, file)).size;
	}
	return size;
}

const { TRANSACTION_LOG_FILE_HEADER_SIZE, TRANSACTION_LOG_ENTRY_HEADER_SIZE } = constants;
const oneEntry = (payload: number) =>
	TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + payload;

RocksDatabase.config({ occValidation: policy as 'parallel' | 'serial' });
const db = RocksDatabase.open(dbPath, { verificationTable: true });
const log = db.useLog('audit');

// 1. An ordinary conflict is refused before any log byte.
await db.put('k', 'initial');
const durableAtStart: boolean[] = [];
let handle: Transaction | undefined;
const body = async (txn: Transaction, attempt: number) => {
	handle = txn;
	durableAtStart.push(txn.hasDurableLog);
	if (mode === 'sync') {
		txn.getSync('k');
	} else {
		await txn.get('k');
	}
	if (attempt === 1) {
		db.putSync('k', 'conflict');
	}
	txn.putSync('k', 'committed');
	log.addEntry(Buffer.from('entry'), txn.id);
};
if (mode === 'async') {
	await db.transaction(body, { coordinatedRetry: true, maxRetries: 5 });
} else {
	await db.transactionSync(body, { retryOnBusy: true, maxRetries: 5 });
}
assert(durableAtStart.length === 2, `expected two attempts, saw ${durableAtStart.length}`);
assert(
	durableAtStart.every((value) => value === false),
	'hasDurableLog must stay false before the log write'
);
assert(handle!.hasDurableLog === true, 'hasDurableLog must be true once the commit landed');
assert(db.getSync('k') === 'committed', 'the retry must have committed');
assert(logBytes('audit') === oneEntry(5), `one log entry expected, got ${logBytes('audit')} bytes`);

// 2. A batch left durable by a failure past the log write can neither be aborted nor dropped.
const logged = new Transaction(db.store);
logged.putSync('j', 'logged');
log.addEntry(Buffer.from('j'), logged.id);
forceTryAgainForTesting(1);
let code: string | undefined;
try {
	if (mode === 'async') {
		await logged.commit();
	} else {
		logged.commitSync();
	}
} catch (error) {
	code = (error as { code?: string }).code;
}
assert(code === 'ERR_TRY_AGAIN', `expected ERR_TRY_AGAIN, got ${code}`);
assert(logged.hasDurableLog === true, 'a stranded commit past the log write is durable');
let refused: unknown;
try {
	logged.abort();
} catch (error) {
	refused = error;
}
assert(refused instanceof TransactionLoggedError, 'abort must be refused');
logged.putSync('j', 'logged');
if (mode === 'async') {
	await logged.commit();
} else {
	logged.commitSync();
}
assert(db.getSync('j') === 'logged', 'the logged handle must still commit');
assert(
	logBytes('audit') === oneEntry(5) + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 1,
	'the logged batch is written once'
);
assert([...log.query({ start: 0 })].length === 2, 'both entries are committed-visible');

db.close();
process.stdout.write(
	`${JSON.stringify({ mode, policy, commitThread: process.env.ROCKSDB_JS_COMMIT_THREAD ?? '' })}\n`
);
