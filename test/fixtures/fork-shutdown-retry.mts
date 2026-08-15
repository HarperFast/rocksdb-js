import { RocksDatabase } from '../../src/index.ts';
import { createWorkerBootstrapScript } from '../lib/worker-bootstrap.ts';
import { Worker } from 'node:worker_threads';

const path = process.argv[2];
const db = RocksDatabase.open(path);
db.putSync('key', 'value');
try {
	db.close();
	throw new Error('Expected the initial close to fail');
} catch (error) {
	if (!String(error).includes('Injected database close failure')) throw error;
}

delete process.env.ROCKSDB_JS_CLOSE_FAILURE;
process.env.ROCKSDB_JS_CLOSE_RETRY_DELAY_MS = '1000';
const worker = new Worker(createWorkerBootstrapScript('./test/workers/shutdown-retry-worker.mts'), {
	eval: true,
});
function nextMessage(): Promise<any> {
	return new Promise((resolve, reject) => {
		worker.once('message', resolve);
		worker.once('error', reject);
	});
}
const started = await nextMessage();
const shutdownResult = nextMessage();
const reopened = RocksDatabase.open(path);
const elapsed = Date.now() - started;
if (elapsed < 500) throw new Error(`Open did not wait for the shutdown retry (${elapsed}ms)`);
if (reopened.getSync('key') !== 'value') throw new Error('Shutdown retry did not preserve data');
const result = await shutdownResult;
if (!result.shutdown) throw new Error(`Shutdown retry failed: ${JSON.stringify(result)}`);
delete process.env.ROCKSDB_JS_CLOSE_RETRY_DELAY_MS;
reopened.destroy();
await worker.terminate();
