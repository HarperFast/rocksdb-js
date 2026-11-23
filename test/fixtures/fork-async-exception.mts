// Usage: node fork-async-exception.mts <dir> <stage> [op]
// Prints the { op, code, message } of each rejection as one JSON array.
import { backups, RocksDatabase, validateTransactionLogStore } from '../../src/index.ts';
import { forceAsyncExceptionForTesting } from '../../src/load-binding.ts';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';

const [dir, stageArg, only] = process.argv.slice(2);
const stage = Number(stageArg) as 1 | 2 | 3 | 5;
mkdirSync(dir, { recursive: true });

const coldPath = join(dir, 'cold');
const seed = RocksDatabase.open(coldPath);
for (let i = 0; i < 10; i++) {
	seed.putSync(`key-${i}`, `value-${i}`);
}
await seed.flush();
seed.close();
// Without a block cache every read of a flushed key misses memory and takes the async path.
const cold = RocksDatabase.open(coldPath, { noBlockCache: true });

const db = RocksDatabase.open(join(dir, 'db'));
db.putSync('key', 'value');
const log = db.useLog('log');
await db.transaction(async (txn) => {
	log.addEntry(Buffer.from('entry'), txn.id);
});
const backupDir = join(dir, 'backups');
await db.backup(backupDir);
await db.backup(backupDir);
const dropped = RocksDatabase.open(join(dir, 'dropped'));

const secondary = RocksDatabase.open(join(dir, 'db'), { secondaryPath: join(dir, 'secondary') });

let sequence = 0;
let backupId = 1;
const ops: [string, () => unknown][] = [
	[
		'get',
		() => {
			const result = cold.get('key-1');
			if (!(result instanceof Promise)) throw new Error('get did not take the async path');
			return result;
		},
	],
	['getMany', () => cold.getMany(['key-1', 'key-2'])],
	['transaction get', () => cold.transaction((txn) => txn.get('key-3'))],
	['transaction getMany', () => cold.transaction((txn) => txn.getMany(['key-4', 'key-5']))],
	['flush', () => db.flush()],
	['compact', () => db.compact()],
	['syncWal', () => db.syncWal()],
	['clear', () => db.clear()],
	['drop', () => dropped.drop()],
	['catchUpWithPrimary', () => secondary.catchUpWithPrimary()],
	['backup', () => db.backup(backupDir)],
	['backup stream', () => db.backup(new WritableStream<Uint8Array>())],
	['createCheckpoint', () => db.createCheckpoint(join(dir, `checkpoint-${sequence++}`))],
	['backups.list', () => backups.list(backupDir)],
	['backups.verify', () => backups.verify(backupDir, 1)],
	['backups.purge', () => backups.purge(backupDir, 100)],
	['backups.delete', () => backups.delete(backupDir, backupId++)],
	['backups.restore', () => backups.restore(backupDir, join(dir, `restore-${sequence++}`))],
	[
		'validateTransactionLogStore',
		() => validateTransactionLogStore(join(db.path, 'transaction_logs', 'log')),
	],
];
if (stage === 1 || stage === 5) {
	// The commit path's execute and completion are not wrapped.
	ops.push(['commit', () => db.transaction((txn) => txn.put('committed', 'value'))]);
}

const results: { op: string; code?: string; message?: string }[] = [];
for (const [op, run] of ops) {
	if (only && op !== only) {
		continue;
	}
	forceAsyncExceptionForTesting(stage, 1);
	let error: (Error & { code?: string }) | undefined;
	try {
		await run();
	} catch (caught) {
		error = caught as Error;
	} finally {
		forceAsyncExceptionForTesting(0, 0);
	}
	results.push({ op, code: error?.code, message: error?.message });
	await run();
}

// Closing only once the loop is idle proves no failed operation left it referenced.
process.once('beforeExit', () => {
	secondary.close();
	dropped.close();
	cold.close();
	db.close();
	console.log(JSON.stringify(results));
});
