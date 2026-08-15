import { RocksDatabase, registryStatus, shutdown } from '../../src/index.ts';
import { setTimeout as delay } from 'node:timers/promises';

const path = process.argv[2];
let db: RocksDatabase | undefined = RocksDatabase.open(path);
db.putSync('key', 'value');

let resolveCloseFailure!: (args: unknown[]) => void;
const closeFailure = new Promise<unknown[]>((resolve) => {
	resolveCloseFailure = resolve;
});
RocksDatabase.on('database:closeFailed', (...args) => resolveCloseFailure(args));

db = undefined;
for (let attempt = 0; attempt < 40; attempt++) {
	global.gc!();
	await delay(25);
}

const args = await Promise.race([
	closeFailure,
	delay(1_000).then(() => {
		throw new Error('Automatic close failure did not emit database:closeFailed');
	}),
]);
if (args[0] !== path || args[1] !== 'Injected database close failure') {
	throw new Error(`Unexpected database:closeFailed arguments: ${JSON.stringify(args)}`);
}
if (
	registryStatus().find((entry) => entry.path === path)?.closeError !==
	'Injected database close failure'
) {
	throw new Error('Automatic close failure was not quarantined');
}

delete process.env.ROCKSDB_JS_CLOSE_FAILURE;
shutdown();
