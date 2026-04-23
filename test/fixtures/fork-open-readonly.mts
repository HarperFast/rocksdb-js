import { RocksDatabase } from '../../src/index.js';

let db: RocksDatabase | undefined;

try {
	// open in read-write mode, this should fail
	db = RocksDatabase.open(process.argv[2]);

	console.error('Expected database to fail in read-write mode');
	process.exit(1);
} catch {
	// good, continue
	db = RocksDatabase.open(process.argv[2], { readOnly: true });
	const value = db.getSync('foo');
	if (value !== 'bar') {
		console.error('Expected value to be bar, got', value);
		process.exit(1);
	}
} finally {
	db?.close();
}

console.log('Success');
process.exit(0);
