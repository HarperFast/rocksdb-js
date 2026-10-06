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

function thrownBy(fn: () => unknown): unknown {
	try {
		fn();
	} catch (error) {
		return error;
	}
	throw new Error('Expected a throw');
}

const db = RocksDatabase.open(dbPath);
try {
	// A thrown value leaves a JS exception pending, so the builder must hand back exactly that value.
	Object.create = () => {
		throw sentinel;
	};
	const rejected = await rejectionOf(backups.list(missingDir));
	if (rejected !== sentinel) throw new Error(`Expected the thrown value, got ${String(rejected)}`);
	Object.create = realCreate;

	// A non-callable factory fails without a JS exception, so the builder must synthesize one.
	Object.create = 0 as unknown as typeof Object.create;
	const syncThrown = thrownBy(() =>
		db.transactionSync((txn) => {
			txn.setTimestamp(-1);
		})
	);
	if (!(syncThrown instanceof Error))
		throw new Error(`Expected a sync Error, got ${String(syncThrown)}`);
	const asyncRejected = await rejectionOf(backups.list(missingDir));
	if (!(asyncRejected instanceof Error))
		throw new Error(`Expected an Error, got ${String(asyncRejected)}`);
} finally {
	Object.create = realCreate;
	db.destroy();
}

console.log('settled');
