// Usage: node fork-purge-claim-alloc.mts <dir> [control]
// A checkpoint's descriptor pin makes close() skip its purge; the checkpoint's completion retries it
// from a noexcept destructor. Fails that retry's close-claim allocation, then prints the registry
// entries left after it and after a reopen and close.
import { RocksDatabase, registryStatus } from '../../src/index.ts';
import { forceAsyncExceptionForTesting } from '../../src/load-binding.ts';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';

const [dir, mode] = process.argv.slice(2);
mkdirSync(dir, { recursive: true });
const path = join(dir, 'db');

const db = RocksDatabase.open(path);
db.putSync('key', 'value');
// The completion cannot run before close(): nothing yields in between.
const checkpoint = db.createCheckpoint(join(dir, 'checkpoint'));
db.close();
if (mode !== 'control') {
	forceAsyncExceptionForTesting(4, 1);
}
let checkpointError: string | undefined;
try {
	await checkpoint;
} catch (error) {
	checkpointError = (error as Error).message;
} finally {
	forceAsyncExceptionForTesting(0, 0);
}
const afterFailedPurge = registryStatus().map(({ refCount }) => ({ refCount }));

const reopened = RocksDatabase.open(path);
const value = reopened.getSync('key');
reopened.close();

console.log(
	JSON.stringify({
		checkpointError,
		afterFailedPurge,
		value,
		afterReopenClose: registryStatus().length,
	})
);
