import { RocksDatabase } from '../../src/index.js';

let db: RocksDatabase | undefined;

try {
	// open in write mode, this should fail
	console.log('Opening database in write mode...');
	db = RocksDatabase.open(process.argv[2]);

	console.error('Expected database to fail in write mode!');
	process.exit(1);
} catch {
	// good, continue
	console.log('Opening database in read-only mode...');
	db = RocksDatabase.open(process.argv[2], { readOnly: true });
	console.log('Getting value...');
	const value = db.getSync('foo');
	console.log('Value is', value);
	if (value !== 'bar') {
		console.error('Expected value to be bar, got', value);
		process.exit(1);
	}
	console.log('Success');
} finally {
	db?.close();
}
