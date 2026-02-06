import { fork } from 'node:child_process';
import { dirname, join } from 'node:path';
import { RocksDatabase, shutdown } from '../../src/index.js';
import { fileURLToPath } from 'node:url';

if (process.argv.length < 3) {
	throw new Error('Missing database path');
}

const dbPath = process.argv[2];
const dbs: RocksDatabase[] = [];

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
	fork(join(__dirname, 'fork-shutdown.mts'), [dbPath], { stdio: 'inherit' });
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
