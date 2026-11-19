// Synchronous Database calls from several workers on one database (`--layout shared`) or on a
// database each (`--layout separate`). Prints one JSON line; nsPerOp is wall time / total ops.
//
//   node benchmark/shared-db-sync.mts --op get|put|remove|count --workers 4 --layout shared
//
// `--dist` selects another build (for example a base checkout) to compare interleaved rounds.
import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

if (isMainThread) {
	const { values } = parseArgs({
		options: {
			dist: { type: 'string', default: resolve('dist/index.mjs') },
			op: { type: 'string', default: 'get' },
			workers: { type: 'string', default: '4' },
			layout: { type: 'string', default: 'shared' },
			ops: { type: 'string', default: '400000' },
			dir: { type: 'string', default: resolve('benchmark/data') },
		},
	});
	const dist = pathToFileURL(resolve(values.dist!)).href;
	const { RocksDatabase } = await import(dist);
	const workers = Number(values.workers);
	const ops = Number(values.ops);
	mkdirSync(values.dir!, { recursive: true });
	const root = mkdtempSync(join(values.dir!, 'shared-db-sync-'));
	const paths: string[] = [];
	const holders: any[] = [];
	for (let id = 0; id < workers; id++) {
		const path = values.layout === 'shared' ? join(root, 'db') : join(root, `db-${id}`);
		paths.push(path);
		if (values.layout !== 'shared' || id === 0) {
			const db = RocksDatabase.open(path, { disableWAL: true });
			for (let k = 0; k < 100; k++) db.putSync(`key-${k}`, `value-${k}-${'x'.repeat(40)}`);
			holders.push(db);
		}
	}
	const states = paths.map((path, id) => {
		const worker = new Worker(new URL(import.meta.url), {
			workerData: { dist, path, op: values.op, ops, id },
		});
		const ready = Promise.withResolvers<void>();
		const done = Promise.withResolvers<number>();
		worker.on('error', (e) => {
			ready.reject(e);
			done.reject(e);
		});
		worker.on('message', (m) => {
			if (m === 'ready') ready.resolve();
			else done.resolve(m);
		});
		const exited = new Promise((resolve) => worker.once('exit', resolve));
		return { worker, ready: ready.promise, done: done.promise, exited };
	});
	await Promise.all(states.map((s) => s.ready));
	const start = performance.now();
	for (const s of states) s.worker.postMessage('go');
	await Promise.all(states.map((s) => s.done));
	const elapsed = performance.now() - start;
	// Each worker closes its database after reporting, then exits.
	await Promise.all(states.map((s) => s.exited));
	for (const db of holders) db.close();
	rmSync(root, { recursive: true, force: true });
	console.log(
		JSON.stringify({
			op: values.op,
			workers,
			layout: values.layout,
			nsPerOp: (elapsed * 1e6) / (ops * workers),
		})
	);
} else {
	const { dist, path, op, ops, id } = workerData;
	const { RocksDatabase } = await import(dist);
	const db = RocksDatabase.open(path, { disableWAL: true });
	const keys = Array.from({ length: 100 }, (_, k) => `key-${k}`);
	const ownKeys = Array.from({ length: 100 }, (_, k) => `w${id}-${k}`);
	const value = 'v'.repeat(50);
	const run = (n: number) => {
		let sink = 0;
		switch (op) {
			case 'get':
				for (let i = 0; i < n; i++) sink += db.getSync(keys[i % 100]) ? 1 : 0;
				break;
			case 'put':
				for (let i = 0; i < n; i++) db.putSync(ownKeys[i % 100], value);
				break;
			case 'remove':
				for (let i = 0; i < n; i++) db.removeSync(ownKeys[i % 100]);
				break;
			case 'count':
				for (let i = 0; i < n; i++) sink += db.getKeysCount({ start: 'key-10', end: 'key-12' });
				break;
			default:
				throw new Error(`unknown op ${op}`);
		}
		return sink;
	};
	run(Math.min(ops, 50000));
	parentPort!.postMessage('ready');
	parentPort!.once('message', () => {
		run(ops);
		parentPort!.postMessage(0);
		db.close();
	});
}
