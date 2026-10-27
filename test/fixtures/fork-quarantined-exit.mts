import { RocksDatabase, registryStatus } from '../../src/index.ts';

// A close-time flush failure quarantines the descriptor: it stays in the
// process-global registry, still holding an open rocksdb::DB, so an explicit
// shutdown()/destroy() can retry the close. This fixture never issues that
// retry -- the quarantine survives to process exit, which is the one case the
// registry cannot defer.
//
// The registry singleton is a namespace-scope static, so anything still in its
// map is destroyed from an atexit handler. Closing a RocksDB database there
// runs DBImpl::CancelAllBackgroundWork() after RocksDB's own function-local
// statics (the PeriodicTaskScheduler timer and its port::Mutex) have already
// been destroyed, and RocksDB's PthreadCall aborts the process:
//
//   pthread lock: Invalid argument
//
// The module's env-cleanup hook therefore releases whatever Shutdown() left
// behind (DBRegistry::Teardown), while RocksDB is still usable. The harness
// asserts a zero exit code and no signal, which is what fails without it.
//
// ROCKSDB_JS_CLOSE_FLUSH_FAILURE is set to 2: one failure for the explicit
// close below, one for the exit-time shutdown retry.
//
// With `iterators`, open iterators (a database range, and a pending
// transaction holding busy and idle pooled ones) survive to exit too: the
// failed close never reaches its closables sweep, so Teardown must close them
// before releasing the descriptor, or their finalizers reset and detach
// against a destroyed database during env teardown.
const path = process.argv[2];
const db = RocksDatabase.open(path, { disableWAL: true });
db.putSync('key', 'unflushed');
const retained: unknown[] = [];
if (process.argv[3] === 'iterators') {
	const { Transaction } = await import('../../src/transaction.ts');
	const range = db.getRange()[Symbol.iterator]();
	range.next();
	const txn = new Transaction(db.store);
	retained.push(range, txn, txn.getRange().asArray);
	const busy = txn.getRange()[Symbol.iterator]();
	busy.next();
	retained.push(busy);
}

try {
	db.close();
	throw new Error('Expected close to surface the injected flush failure');
} catch (error) {
	if (!String(error).includes('Injected database close flush failure')) throw error;
}

if (!registryStatus().some((entry) => entry.path === path && entry.closeError))
	throw new Error('Flush failure did not quarantine the descriptor');
