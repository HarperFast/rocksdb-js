// Usage: node fork-async-exception.mts <dir> <stage> [op]
// Makes one call of every async operation (or only `op`) throw std::bad_alloc at `stage` (1 = state
// allocation, 2 = execute, 3 = complete), retries it, then closes every database. Prints one JSON
// array of { op, code, message } for the rejections. Run in a child because an exception that
// escapes kills the process, and a registration it strands makes close() wait forever.
import { backups, RocksDatabase, validateTransactionLogStore } from '../../src/index.ts';
import { forceAsyncExceptionForTesting } from '../../src/load-binding.ts';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';

const [dir, stageArg, only] = process.argv.slice(2);
const stage = Number(stageArg) as 1 | 2 | 3;
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

const secondary = RocksDatabase.open(join(dir, 'db'), { secondaryPath: join(dir, 'secondary') });

let sequence = 0;
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
	['catchUpWithPrimary', () => secondary.catchUpWithPrimary()],
	['backup', () => db.backup(backupDir)],
	['backup stream', () => db.backup(new WritableStream<Uint8Array>())],
	['createCheckpoint', () => db.createCheckpoint(join(dir, `checkpoint-${sequence++}`))],
	['backups.list', () => backups.list(backupDir)],
	['backups.verify', () => backups.verify(backupDir, 1)],
	['backups.purge', () => backups.purge(backupDir, 100)],
	['backups.restore', () => backups.restore(backupDir, join(dir, `restore-${sequence++}`))],
	[
		'validateTransactionLogStore',
		() => validateTransactionLogStore(join(db.path, 'transaction_logs', 'log')),
	],
];
if (stage === 1) {
	// Only the state allocation is guarded on the commit path.
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

secondary.close();
cold.close();
db.close();
console.log(JSON.stringify(results));
