import { RocksDatabase } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { Worker } from 'node:worker_threads';

// DBHandle::close() deliberately skips releasing `logRefs` on a foreign thread
// (they are this env's napi_refs -- AGENTS.md invariant 18's recycled-thread-id
// hazard), so a cross-env shutdown() leaves this handle's TransactionLog cache
// populated. Reopening the handle must therefore drop that cache: the cached
// TransactionLogHandle holds a weak_ptr to the previous lifecycle's store, which
// shutdown() unregistered, and only its write path re-resolves -- every read
// accessor reports empty instead of reading the reopened log.
const path = process.argv[2];
const db = RocksDatabase.open(path);
const log = db.useLog('foo');
await db.transaction(async (txn) => {
	log.addEntry(Buffer.from('hello'), txn.id);
});
const sizeBefore = log.getLogFileSize();
if (sizeBefore === 0) throw new Error('Expected the transaction log to have entries');

const worker = new Worker(createWorkerBootstrapScript('./test/workers/shutdown-retry-worker.mts'), {
	eval: true,
});
function nextMessage(): Promise<any> {
	return new Promise((resolve, reject) => {
		worker.once('message', resolve);
		worker.once('error', reject);
	});
}
await nextMessage(); // worker started, about to call shutdown()
const result = await nextMessage();
if (!result.shutdown) throw new Error(`Foreign shutdown failed: ${JSON.stringify(result)}`);
await worker.terminate();

if (db.isOpen()) throw new Error('Expected the foreign shutdown to close this handle');
// `log` is still referenced here on purpose: useLog()'s cache entry is a weak
// napi_ref, so letting it be collected would hide the stale-cache bug.
db.open();

const reopenedLog = db.useLog('foo');
// The symptom: a stale cache entry's store weak_ptr is expired, so every read
// accessor reports an empty log rather than the reopened one.
const sizeAfter = reopenedLog.getLogFileSize();
if (sizeAfter !== sizeBefore) {
	throw new Error(`Reopened log reports ${sizeAfter} bytes, expected ${sizeBefore}`);
}
const entries = [...reopenedLog.query({ start: 0 })];
if (entries.length !== 1) throw new Error(`Expected 1 log entry, read ${entries.length}`);
if (reopenedLog === log) {
	throw new Error('useLog() returned the closed lifecycle’s cached TransactionLog');
}
db.destroy();
