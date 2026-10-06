import { RocksDatabase, backups } from '../../src/index.ts';

// Runs in a child process so the patched global cannot leak into other tests.
const [dbPath, missingDir] = process.argv.slice(2);
const realCreate = Object.create;
const sentinel = { sentinel: 'object-create-threw' };

async function rejectionOf(promise: Promise<unknown>): Promise<unknown> {
	return promise.then(
		() => {
			throw new Error('Expected a rejection');
		},
		(error: unknown) => error
	);
}

const db = RocksDatabase.open(dbPath);
try {
	Object.create = () => {
		throw sentinel;
	};
	const rejected = await rejectionOf(backups.list(missingDir));
	if (rejected !== sentinel) throw new Error(`Expected the thrown value, got ${String(rejected)}`);

	let thrown: unknown;
	try {
		db.transactionSync((txn) => {
			txn.setTimestamp(-1);
		});
	} catch (error) {
		thrown = error;
	}
	if (thrown !== sentinel) throw new Error(`Expected the sync thrown value, got ${String(thrown)}`);
} finally {
	Object.create = realCreate;
}

try {
	Object.create = 0 as unknown as typeof Object.create;
	const rejected = await rejectionOf(backups.list(missingDir));
	if (!(rejected instanceof Error)) throw new Error(`Expected an Error, got ${String(rejected)}`);
} finally {
	Object.create = realCreate;
}

db.destroy();
console.log('settled');
