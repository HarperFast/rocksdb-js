import { RocksDatabase, Transaction } from '../../src/index.ts';
import { RETRY_NOW } from '../../src/transaction.ts';

const dbPath = process.argv[2];

if (!dbPath) {
	console.error('Usage: fork-park-timeout.mts <dbPath>');
	process.exit(1);
}

function valueWithVersion(version: number): Buffer {
	const value = Buffer.alloc(16);
	value.writeDoubleBE(version, 0);
	return value;
}

const db = new RocksDatabase(dbPath, { encoding: false, verificationTable: true });
const keepAlive = setInterval(() => {}, 1000);

try {
	db.open();
	const key = Buffer.from('park-timeout-abandoned-holder');
	const initialVersion = 1.6e12;
	await db.put(key, valueWithVersion(initialVersion));
	db.populateVersion(key, initialVersion);
	if (!db.verifyVersion(key, initialVersion)) {
		throw new Error('failed to populate the verification-table version');
	}

	const holder = new Transaction(db.store, { coordinatedRetry: true });
	holder.putSync(key, valueWithVersion(2.1e12));

	const transaction = new Transaction(db.store, { coordinatedRetry: true });
	await transaction.get(key);
	await db.put(key, valueWithVersion(2.2e12));
	transaction.putSync(key, valueWithVersion(2.3e12));

	const start = performance.now();
	const result = await transaction.commit();
	const elapsed = performance.now() - start;

	if (result !== RETRY_NOW || elapsed < 40 || elapsed >= 4000) {
		throw new Error(`unexpected park result=${String(result)} elapsed=${elapsed}`);
	}
} finally {
	clearInterval(keepAlive);
	db.close();
}
