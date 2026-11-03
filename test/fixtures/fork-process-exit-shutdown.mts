import { RocksDatabase, getRegistryStatus, shutdown } from '../../src/index.ts';
import { writeSync } from 'node:fs';
import { Worker } from 'node:worker_threads';

// Node runs no env cleanup hook for the main thread when the process ends via
// process.exit(), so the binding's last-env shutdown never runs there. Each mode
// writes with the WAL disabled -- data only a close-time flush persists -- and
// then exits; the harness reopens the path and checks what survived.
const [mode, path] = process.argv.slice(2);
const workerSource = new URL('../../src/index.ts', import.meta.url).href;

function startWorker(script: string): Worker {
	return new Worker(
		`
		const { parentPort, workerData } = require('node:worker_threads');
		import(workerData.src).then(({ RocksDatabase }) => {
			const db = RocksDatabase.open(workerData.path, { disableWAL: true });
			db.putSync('worker', 'value');
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
		// Listeners added after rocksdb-js loads run after its exit shutdown;
		// prependListener runs first, while the database is still open.
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('main', 'value');
		process.prependListener('exit', () => db.putSync('late', 'value'));
		process.exit(0);
	}
	case 'fenced': {
		// A later exit listener finds the databases closed, and the open fence
		// rejects a reopen rather than leaving it unflushed.
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
		// The listener the README used to ask callers to add, on both sides of ours.
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('main', 'value');
		process.on('exit', () => shutdown());
		process.prependListener('exit', () => shutdown());
		process.exit(0);
	}
	case 'quarantined': {
		// Run with ROCKSDB_JS_CLOSE_FLUSH_FAILURE=2: the explicit close and the
		// exit-time retry both fail, so the descriptor is still quarantined when
		// the process exits. The exit path must release it before static
		// destruction, and leave later listeners and the exit code alone.
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
		// Many small flushed files, then process.exit() while a manual compaction
		// is running on a libuv thread.
		const db = RocksDatabase.open(path, { disableWAL: true });
		const value = 'x'.repeat(1024);
		for (let file = 0; file < 8; file++) {
			for (let i = 0; i < 2000; i++) db.putSync(`${file}-${i}`, value);
			db.flushSync();
		}
		void db.compact();
		db.putSync('main', 'value');
		setImmediate(() => process.exit(0));
		break;
	}
	case 'commit': {
		// process.exit() with an async commit dispatched but not settled.
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('main', 'value');
		void db.transaction((txn) => {
			txn.putSync('commit', 'value');
		});
		queueMicrotask(() => process.exit(0));
		break;
	}
	case 'worker': {
		// A worker holding the same database is still running when the main
		// thread calls process.exit(); Node stops it after the exit listeners.
		const db = RocksDatabase.open(path, { disableWAL: true });
		const worker = startWorker(`parentPort.postMessage('ready'); setInterval(() => {}, 1000);`);
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
		// A worker's own process.exit() ends only the worker; the main thread's
		// database stays open.
		const db = RocksDatabase.open(path, { disableWAL: true });
		const worker = startWorker(`process.exit(0);`);
		worker.once('exit', () => {
			if (!db.isOpen()) throw new Error('Worker exit closed the main thread database');
			db.putSync('main', 'value');
		});
		break;
	}
	case 'verify': {
		const db = RocksDatabase.open(path);
		const read = (key: string) => db.getSync(key) ?? null;
		console.log(JSON.stringify({ main: read('main'), worker: read('worker'), late: read('late') }));
		db.close();
		break;
	}
	default:
		throw new Error(`Unknown mode: ${mode}`);
}
