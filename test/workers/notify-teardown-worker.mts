import { RocksDatabase } from '../../src/index.js';
import { parentPort, workerData } from 'node:worker_threads';

// Open the SAME path as the parent so all envs share one DBDescriptor (and its
// event emitter) — the cross-env precondition for the harper#1370 crash.
const db = RocksDatabase.open(workerData.path);

if (workerData.role === 'committer') {
	// Continuously fire async transaction commits. Each completes on this env's
	// libuv threadpool and calls descriptor->notify('committed'), iterating every
	// listener — including the listener workers living in other (soon-dying) envs.
	let stop = false;
	let counter = 0;
	parentPort?.on('message', (message: { stop?: boolean }) => {
		if (message.stop) {
			stop = true;
		}
	});
	const spin = async () => {
		// `stop` is checked only between fully-awaited commits, so there is never
		// an in-flight commit when we report stopped — that keeps this worker's
		// teardown clear of the unrelated close-vs-commit transaction double free.
		if (stop) {
			parentPort?.postMessage({ stopped: true });
			return;
		}
		try {
			await db.transaction((txn) => {
				txn.put('committer', counter++);
			});
		} catch {
			// the db may be closing as the run winds down; ignore
		}
		setImmediate(spin);
	};
	setImmediate(spin);
} else {
	// listener role: register a per-db 'committed' listener — a threadsafe
	// function bound to THIS worker env, stored on the shared descriptor's
	// emitter. The committer's notify iterates it; the parent terminates this
	// worker mid-notify. No transactions here, so teardown only exercises the
	// emitter cleanup path (not the transaction-close path).
	db.addListener('committed', () => {});
}

parentPort?.postMessage({ ready: true });
