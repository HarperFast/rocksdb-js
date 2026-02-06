import { fork } from 'node:child_process';
import { dirname, join } from 'node:path';
import { RocksDatabase, shutdown } from '../../src/index.js';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

if (process.argv.length < 3) {
	throw new Error('Missing database path');
}

const dbPath = process.argv[2];
const dbs: RocksDatabase[] = [];

const parentPid = Number.parseInt(process.env.PARENT_PID || '0', 10);
if (parentPid) {
	console.log(`parent pid: ${parentPid}`);
	const timeout = setTimeout(() => {
		console.log('parent timed out');
		process.exit(1);
	}, 10000);

	while (true) {
		try {
			process.kill(parentPid, 0);
		} catch {
			console.log('parent is dead');
			break;
		}
		console.log('parent is still alive');
		await delay(100);
	}

	clearTimeout(timeout);
}

for (let i = 0; i < 20; i++) {
	console.log(`open db${i}`);
	const db = RocksDatabase.open(join(dbPath, `db${i}`));
	dbs.push(db);
	if (!process.env.DO_FORK) {
		db.putSync(`test1-${i}`, `value1-${i}`);
	}
}

process.on('exit', () => {
	console.log('shutdown start');
	shutdown();
	console.log('shutdown end');
});

if (process.env.DO_FORK) {
	delete process.env.DO_FORK;
	const __dirname = dirname(fileURLToPath(import.meta.url));
	console.log('forking')
	fork(join(__dirname, 'fork-shutdown.mts'), [dbPath], {
		env: {
			...process.env,
			PARENT_PID: process.pid.toString(),
		},
		stdio: 'inherit',
	});
} else {
	let j = 0;
	for (const db of dbs) {
		if (db.getSync(`test1-${j}`) !== `value1-${j}`) {
			console.log(`db${j} get ${db.getSync(`test1-${j}`)} expected ${j}`);
			process.exit(1);
		}
		j++;
	}
}

process.exit(0);
