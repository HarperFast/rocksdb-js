import { registryStatus, RocksDatabase, Transaction } from '../../src/index.ts';
import assert from 'node:assert/strict';
import { rmSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';

const mode = process.argv[2];
const dbPath = process.argv[3];

if (
	(mode !== 'async-get' && mode !== 'iterator' && mode !== 'routed-iterator') ||
	!dbPath ||
	!globalThis.gc
) {
	console.error(
		'Usage: node --expose-gc transaction-orphan-dependents.mts <async-get|iterator|routed-iterator> <dbPath>'
	);
	process.exit(1);
}

function transactionCount(): number {
	return registryStatus().find((entry) => entry.path === dbPath)?.transactions ?? 0;
}

async function forceCollection(rounds = 10): Promise<number> {
	let longestGcMs = 0;
	for (let i = 0; i < rounds; i++) {
		const started = Date.now();
		globalThis.gc!();
		longestGcMs = Math.max(longestGcMs, Date.now() - started);
		await delay(20);
	}
	return longestGcMs;
}

async function waitForTransactionClose(): Promise<void> {
	const deadline = Date.now() + 3000;
	while (transactionCount() !== 0 && Date.now() < deadline) {
		globalThis.gc!();
		await delay(20);
	}
	assert.equal(transactionCount(), 0);
}

async function testDelayedAsyncGet(db: RocksDatabase): Promise<void> {
	await db.put('key', 'value');
	await db.flush();
	db.close();
	db.open();

	const pendingRead = (() => {
		const txn = new Transaction(db.store);
		return txn.getBinary('key');
	})();
	assert.ok(pendingRead instanceof Promise, 'expected a cold-cache async read');

	const longestGcMs = await forceCollection();
	assert.ok(longestGcMs < 3000, `transaction finalizer blocked for ${longestGcMs}ms`);
	await assert.rejects(pendingRead, /Database closed during transaction get operation/);
	await waitForTransactionClose();
}

async function testLiveIterator(db: RocksDatabase, routed: boolean): Promise<void> {
	for (const key of ['a', 'b', 'c']) {
		await db.put(key, `value-${key}`);
	}

	const iterator = (() => {
		const txn = new Transaction(db.store);
		const iterator = (routed ? db.getRange({ transaction: txn }) : txn.getRange())[
			Symbol.iterator
		]();
		assert.deepEqual(iterator.next(), { done: false, value: { key: 'a', value: 'value-a' } });
		return iterator;
	})();

	await forceCollection();
	assert.equal(transactionCount(), 1);
	assert.deepEqual(iterator.next(), { done: false, value: { key: 'b', value: 'value-b' } });
	iterator.return?.();
	await waitForTransactionClose();
}

const db = RocksDatabase.open(dbPath);
try {
	if (mode === 'async-get') {
		await testDelayedAsyncGet(db);
	} else {
		await testLiveIterator(db, mode === 'routed-iterator');
	}
} finally {
	db.close();
	if (!process.env.KEEP_FILES) {
		rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 100 });
	}
}
