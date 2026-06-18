import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

// W opens a FRESH handle each round (signaled by M via "nextRound") so the
// DBDescriptor refcount is always 2 when the commit starts (M holds the other
// ref). After W closes its handle the refcount drops to 1; when M then closes
// its handle the refcount hits 0 and triggers DBDescriptor::close() →
// T->close() on M's JS thread (PATH A). The async commit's complete callback
// fires T->close() on W's JS thread (PATH B). Without the closed-gate fix both
// paths race to delete this->txn (HarperFast/harper#1370, close-vs-commit).
//
// Round coordination ("nextRound" / "committing" / "done"):
//
//   M ──nextRound──► W opens DB, starts commit, closes handle
//   M ◄─committing── W
//   M closes its handle (triggers descriptor->close() / PATH A)
//   W awaits commit (PATH B fires in complete callback)
//   W ──done──────► M
//   (M loops: opens fresh DB handle, then sends nextRound for the next round)
//
// The explicit handshake prevents W from reopening the DB before M's close has
// fully erased the registry entry, which would trigger the pre-existing
// DBRegistry race (HarperFast/rocksdb-js PR #664, not the bug under test).

let counter = 0;

parentPort?.on('message', async (message: { stop?: boolean; nextRound?: boolean }) => {
	if (message.stop) {
		parentPort?.postMessage({ stopped: true });
		return;
	}

	if (!message.nextRound) return;

	try {
		// Fresh handle: shares descriptor with M's handle (refcount = 2)
		const db = RocksDatabase.open(workerData.path);

		// Queue the async commit on the threadpool
		const commitPromise = db.transaction((txn) => {
			txn.put('committer', counter++);
		});

		// Drop W's descriptor ref (refcount → 1, descriptor still alive via M)
		// T->dbHandle->descriptor is now null; PATH B's transactionRemove is skipped
		db.close();

		// Signal M: W's handle is closed; M closing now triggers descriptor->close()
		parentPort?.postMessage({ committing: true });

		// PATH B: complete callback fires on this thread once execute finishes
		await commitPromise;
	} catch {
		// Expected — descriptor may be closing when the commit resolves
	}

	parentPort?.postMessage({ done: true });
});

parentPort?.postMessage({ ready: true });
