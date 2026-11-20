import { RocksDatabase, registryStatus, shutdown, Transaction } from '../../src/index.ts';

// ROCKSDB_JS_CLOSE_FLUSH_FAILURE=1 makes the first shutdown() quarantine the descriptor before its
// closables sweep. The commit's completion runs after that and must still close its transaction
// rather than leave it registered, with its snapshot, until a retry.
const path = process.argv[2];
if (Number(process.env.ROCKSDB_JS_CLOSE_FLUSH_FAILURE) !== 1) {
	throw new Error('ROCKSDB_JS_CLOSE_FLUSH_FAILURE must be 1');
}

const db = RocksDatabase.open(path);
db.putSync('existing', 'value');

const txn = new Transaction(db.store);
txn.putSync('k', 'committed');
const settled = txn.commit().catch((error) => {
	// The after-commit notification can find the database closed; the write is checked below.
	if (!/Database (not open|is closing)/.test(String(error))) throw error;
});
// Shut down once the commit has executed, while its completion is still queued behind this turn.
const sleepCell = new Int32Array(new SharedArrayBuffer(4));
const deadline = Date.now() + 10_000;
while (db.getSync('k') !== 'committed') {
	if (Date.now() > deadline) throw new Error('the commit never landed');
	Atomics.wait(sleepCell, 0, 0, 2);
}

try {
	shutdown();
	throw new Error('expected shutdown() to report the injected flush failure');
} catch (error) {
	if (!String(error).includes('Injected database close flush failure')) throw error;
}
const quarantined = registryStatus().find((entry) => entry.path === path);
if (!quarantined?.closeError) throw new Error('the flush failure did not quarantine the database');
if (quarantined.transactions !== 1) {
	throw new Error(`expected the commit's transaction registered: ${JSON.stringify(quarantined)}`);
}

await settled;
const after = registryStatus().find((entry) => entry.path === path);
if (after?.transactions !== 0) {
	throw new Error(`the commit's transaction stayed registered: ${JSON.stringify(after)}`);
}

shutdown();
const reopened = RocksDatabase.open(path);
try {
	if (reopened.getSync('existing') !== 'value') throw new Error('existing data did not survive');
	if (reopened.getSync('k') !== 'committed') throw new Error('the commit did not survive');
} finally {
	reopened.close();
}
console.log(JSON.stringify({ ok: true }));
