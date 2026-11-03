import { RocksDatabase, getRegistryStatus, shutdown } from '../../src/index.ts';
import { writeSync } from 'node:fs';
import { Worker } from 'node:worker_threads';

// Node runs no env cleanup hook for the main thread when the process ends via
// process.exit(), so the binding's last-env shutdown never runs there. Each mode
// writes with the WAL disabled -- data only a close-time flush persists -- and
// then exits; the harness reopens the path and checks what survived.
const [mode, path] = process.argv.slice(2);
const workerSource = new URL('../../src/index.ts', import.meta.url).href;

const openAndWrite = `const db = RocksDatabase.open(workerData.path, { disableWAL: true });
	db.putSync('worker', 'value');`;

function startWorker(script: string): Worker {
	return new Worker(
		`
		const { parentPort, workerData } = require('node:worker_threads');
		import(workerData.src).then(({ RocksDatabase, shutdown }) => {
			${script}
		});
		`,
		{ eval: true, workerData: { path, src: workerSource } }
	);
}

switch (mode) {
	case 'exit': {
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('main', 'value');
		process.exit(0);
	}
	case 'natural': {
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('main', 'value');
		break;
	}
	case 'uncaught': {
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('main', 'value');
		throw new Error('uncaught on purpose');
	}
	case 'opt-out': {
		RocksDatabase.config({ shutdownOnExit: false });
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('main', 'value');
		process.exit(0);
	}
	case 'prepend': {
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('main', 'value');
		process.prependListener('exit', () => db.putSync('late', 'value'));
		process.exit(0);
	}
	case 'fenced': {
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('main', 'value');
		process.on('exit', () => {
			writeSync(1, `${JSON.stringify({ open: db.isOpen() })}\n`);
			try {
				RocksDatabase.open(`${path}-late`);
				writeSync(1, 'reopened\n');
			} catch (error) {
				writeSync(1, `${(error as Error).message}\n`);
			}
		});
		process.exit(0);
	}
	case 'caller-shutdown': {
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('main', 'value');
		process.on('exit', () => shutdown());
		process.prependListener('exit', () => shutdown());
		process.exit(0);
	}
	case 'quarantined': {
		// Run with ROCKSDB_JS_CLOSE_FLUSH_FAILURE=2: the exit-time retry fails
		// too, leaving the descriptor quarantined at exit.
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('main', 'value');
		try {
			db.close();
		} catch {}
		if (!getRegistryStatus().some((entry) => entry.closeError)) {
			throw new Error('Flush failure did not quarantine the descriptor');
		}
		process.on('exit', () => writeSync(1, 'later listener ran\n'));
		process.exit(0);
	}
	case 'compact': {
		// Run with ROCKSDB_JS_COMPACT_DELAY_MS, which parks the compaction until
		// its cancel token is armed, so it is in flight at exit.
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('main', 'value');
		db.compact().catch(() => {});
		setTimeout(() => {
			if (!db.isOpen()) throw new Error('Database closed before exit');
			process.exit(0);
		}, 100);
		break;
	}
	case 'commit': {
		// Run with ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS, which holds the admitted
		// commit before RocksDB applies it, so it is in flight at exit.
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('main', 'value');
		void db.transaction((txn) => {
			txn.putSync('commit', 'value');
		});
		setTimeout(() => {
			if (!db.isOpen()) throw new Error('Database closed before exit');
			process.exit(0);
		}, 50);
		break;
	}
	case 'concurrent-close': {
		// Run with ROCKSDB_JS_CLOSE_FLUSH_FAILURE=2 and
		// ROCKSDB_JS_CLOSE_RETRY_DELAY_MS: a worker's shutdown() is still retrying
		// the failed close when the main thread exits, so the main thread's
		// shutdown times out on it: the retry quarantines the descriptor after the
		// main thread's sweep, and a database opened meanwhile is never claimed.
		RocksDatabase.config({ lifecycleWaitSeconds: 1 });
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('main', 'value');
		try {
			db.close();
		} catch {}
		startWorker(`try { shutdown(); } catch {}`);
		const poll = setInterval(() => {
			if (getRegistryStatus().some((entry) => entry.closeRetrying)) {
				clearInterval(poll);
				RocksDatabase.open(`${path}-late`, { disableWAL: true }).putSync('late', 'value');
				process.exit(0);
			}
		}, 5);
		break;
	}
	case 'worker-churn': {
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('main', 'value');
		let ready = 0;
		for (let i = 0; i < 4; i++) {
			const worker = startWorker(`
				let n = 0;
				parentPort.postMessage('ready');
				for (;;) {
					try {
						const db = RocksDatabase.open(workerData.path + '-churn-' + (n++ % 3), { disableWAL: true });
						db.putSync('k', n);
						db.close();
					} catch {}
				}
			`);
			worker.once('message', () => {
				if (++ready === 4) setTimeout(() => process.exit(0), 20);
			});
		}
		break;
	}
	case 'worker': {
		const db = RocksDatabase.open(path, { disableWAL: true });
		const worker = startWorker(
			`${openAndWrite} parentPort.postMessage('ready'); setInterval(() => {}, 1000);`
		);
		worker.once('message', () => {
			db.putSync('main', 'value');
			process.exit(0);
		});
		worker.once('error', (error) => {
			console.error(error);
			process.exit(2);
		});
		break;
	}
	case 'worker-exit': {
		const db = RocksDatabase.open(path, { disableWAL: true });
		const worker = startWorker(`${openAndWrite} process.exit(0);`);
		worker.once('exit', () => {
			if (!db.isOpen()) throw new Error('Worker exit closed the main thread database');
			db.putSync('main', 'value');
		});
		break;
	}
	case 'verify': {
		const db = RocksDatabase.open(path);
		const read = (key: string) => db.getSync(key) ?? null;
		console.log(
			JSON.stringify({
				main: read('main'),
				worker: read('worker'),
				late: read('late'),
				commit: read('commit'),
			})
		);
		db.close();
		break;
	}
	default:
		throw new Error(`Unknown mode: ${mode}`);
}
