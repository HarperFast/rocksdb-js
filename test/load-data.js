import { RocksDatabase } from '../dist/index.js';
let db = new RocksDatabase('/tmp/load-test');
(async () => {
	await db.open();
	console.log('Database opened successfully', db.get('key1'));
	await db.transaction(async (txn) => {
		db.put('key1', 'value1', {transaction: txn});
	});

})();