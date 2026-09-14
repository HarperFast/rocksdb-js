import { RocksDatabase } from '../../src/index.ts';
import {
	getCommitGateCountersForTesting,
	setCommitHoldForTesting,
} from '../../src/load-binding.ts';
import type { Transaction } from '../../src/transaction.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { parentPort, workerData } from 'node:worker_threads';

// Command-driven worker for test/drop-commit-gate.test.ts. Each command runs on
// this worker's own JS thread against handles it opens on the shared database
// path, so the parent can arrange a commit from one env racing a drop from
// another and observe the ordering through the binding's process-global
// commit-gate counters.

type ErrorInfo = { code?: string; message: string };

const { path, pessimistic } = workerData as { path: string; pessimistic?: boolean };
const handles = new Map<string, RocksDatabase>();

function open(name: string): RocksDatabase {
	let db = handles.get(name);
	if (!db) {
		db = RocksDatabase.open(path, { name, pessimistic });
		handles.set(name, db);
	}
	return db;
}

function describeError(err: unknown): ErrorInfo {
	const e = err as Error & { code?: string };
	return { code: e?.code, message: e?.message ?? String(err) };
}

async function waitForCounter(field: 'commitsAdmitted' | 'dropsBegun', min: number): Promise<void> {
	while (getCommitGateCountersForTesting()[field] < min) {
		await delay(1);
	}
}

parentPort?.on('message', async (msg: Record<string, any>) => {
	try {
		switch (msg.type) {
			case 'commit': {
				// Async commit: dispatched to the shared commit lane (or the libuv pool in
				// legacy mode) where the gate admits it; reports once it settles.
				const db = open(msg.family);
				const promise = db.transaction((txn: Transaction) => {
					txn.putSync(msg.key, msg.value ?? 1);
				});
				parentPort?.postMessage({ type: 'commit-started', id: msg.id });
				let error: ErrorInfo | undefined;
				try {
					await promise;
				} catch (err) {
					error = describeError(err);
				}
				parentPort?.postMessage({ type: 'commit-settled', id: msg.id, error });
				break;
			}
			case 'late-commit-when-drop-begins': {
				// Waits until a drop has closed the gate, then commits synchronously on
				// this thread (a sync commit bypasses the commit lane, so it cannot queue
				// behind the held commit) and finally lets the held commit go.
				await waitForCounter('dropsBegun', msg.minDropsBegun);
				const during: Record<string, unknown> = {};
				if (msg.unrelatedFamily) {
					const unrelated = open(msg.unrelatedFamily);
					unrelated.putSync('during', 'ok');
					during.unrelatedPut = unrelated.getSync('during');
				}
				const before = getCommitGateCountersForTesting();
				let error: ErrorInfo | undefined;
				try {
					open(msg.family).transactionSync((txn: Transaction) => {
						txn.putSync('late', 1);
					});
				} catch (err) {
					error = describeError(err);
				}
				const after = getCommitGateCountersForTesting();
				if (msg.releaseHold) {
					setCommitHoldForTesting(false);
				}
				parentPort?.postMessage({
					type: 'late-commit-done',
					id: msg.id,
					error,
					before,
					after,
					during,
				});
				break;
			}
			case 'open': {
				open(msg.family);
				parentPort?.postMessage({ type: 'opened', id: msg.id });
				break;
			}
			case 'drop': {
				let error: ErrorInfo | undefined;
				try {
					open(msg.family).dropSync();
				} catch (err) {
					error = describeError(err);
				}
				parentPort?.postMessage({ type: 'dropped', id: msg.id, error });
				break;
			}
			case 'close': {
				for (const db of handles.values()) {
					db.close();
				}
				handles.clear();
				parentPort?.postMessage({ type: 'closed', id: msg.id });
				break;
			}
			default:
				parentPort?.postMessage({
					type: 'error',
					id: msg.id,
					message: `unknown command ${msg.type}`,
				});
		}
	} catch (err) {
		parentPort?.postMessage({ type: 'error', id: msg.id, message: describeError(err).message });
	}
});

parentPort?.postMessage({ type: 'ready' });
