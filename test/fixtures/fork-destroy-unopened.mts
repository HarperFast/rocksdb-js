import { RocksDatabase } from '../../src/index.ts';
import { existsSync } from 'node:fs';

// Runs with its own throwaway CWD, containing a sentinel file: destroy() on a
// database that was never opened must not resolve "no path" into the process
// working directory and delete it. Exits 0 only if the call was refused AND the
// CWD survived.
const db = new RocksDatabase(process.argv[2]);
let refused = false;
try {
	db.destroy();
} catch {
	refused = true;
}

if (!existsSync('sentinel.txt')) {
	console.error('destroy() deleted the working directory');
	process.exit(2);
}
if (!refused) {
	console.error('destroy() on a never-opened database was not refused');
	process.exit(1);
}
console.log('refused, cwd intact');
