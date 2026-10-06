import { RocksDatabase } from '../../src/index.ts';
import { isTransactionCommitExecuteDelayedForTesting } from '../../src/load-binding.ts';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';

const db = RocksDatabase.open(process.argv[2], { transactionLogMaxSize: 2000 });
const log = db.useLog('foo');
const pendingPayloadSize = Number(process.argv[3]);
const write = (index: number) =>
	db.transaction((txn) => {
		const payload = Buffer.alloc(index === 3 ? pendingPayloadSize : 534, 1);
		payload.writeUInt32BE(index);
		log.addEntry(payload, txn.id);
	});

try {
	for (let i = 0; i < 3; i++) await write(i);
	const pending = write(3);
	const deadline = Date.now() + 5000;
	while (!isTransactionCommitExecuteDelayedForTesting()) {
		assert(Date.now() < deadline, 'commit did not reach the execute delay');
		await delay(1);
	}

	const timestamp = [...log.query({ start: 0, readUncommitted: true })].at(-1)!.timestamp;
	const ahead = log.query({ start: timestamp, exactStart: true });
	assert.deepEqual([...ahead], []);
	assert.deepEqual([...ahead], []);
	await pending;
	for (let i = 4; i < 7; i++) await write(i);
	assert.deepEqual(
		[...log.query({ start: 0 })].map(({ data }) => data.readUInt32BE()),
		[0, 1, 2, 3, 4, 5, 6]
	);
	assert.deepEqual(
		[...ahead].map(({ data }) => data.readUInt32BE()),
		[3, 4, 5, 6]
	);
} finally {
	db.close();
}
