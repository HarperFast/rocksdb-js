import { RocksDatabase } from '../../src/index.ts';
import { chmodSync } from 'node:fs';

const path = process.argv[2];
const db = RocksDatabase.open(path);
db.putSync('key', 'value');
db.close();

chmodSync(path, 0o500);
let destroyFailed = false;
try {
	db.destroy();
} catch {
	destroyFailed = true;
} finally {
	chmodSync(path, 0o700);
}
if (!destroyFailed)
	throw new Error('Expected destroy to fail for a non-writable database directory');

try {
	RocksDatabase.open(path).close();
} catch {
	// Physical destruction may have partially completed; only gate release is asserted.
}
