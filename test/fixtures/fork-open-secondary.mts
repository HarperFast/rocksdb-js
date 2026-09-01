import { RocksDatabase } from '../../src/index.ts';

// argv: <dbPath> <lockedSecondaryPath> <ownSecondaryPath>
// The parent holds a secondary open on <lockedSecondaryPath>; opening it from
// this process must be refused by the workspace kernel lock, while a workspace
// of our own must work.
const [dbPath, lockedSecondaryPath, ownSecondaryPath] = process.argv.slice(2);

const locked = new RocksDatabase(dbPath, { secondaryPath: lockedSecondaryPath });
try {
	locked.open();
	console.error('Expected the locked workspace open to be refused!');
	process.exit(1);
} catch (err) {
	if (!/locked by another secondary instance/.test((err as Error).message)) {
		console.error('Unexpected error for locked workspace:', (err as Error).message);
		process.exit(1);
	}
} finally {
	locked.close();
}

const own = new RocksDatabase(dbPath, { secondaryPath: ownSecondaryPath });
try {
	own.open();
	if (own.getSync('foo') !== 'bar') {
		console.error('Expected to read foo=bar through own workspace');
		process.exit(1);
	}
	console.log('Success');
} finally {
	own.close();
}
