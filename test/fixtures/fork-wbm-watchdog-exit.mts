import { RocksDatabase } from '../../src/index.ts';

const dbPath = process.argv[2];
const listenerOrder = process.argv[3];

if (!dbPath || (listenerOrder !== 'before' && listenerOrder !== 'after')) {
	process.exit(1);
}

const addListener = () => RocksDatabase.on('log.warn', () => {});
if (listenerOrder === 'before') {
	addListener();
}

RocksDatabase.config({
	writeBufferManagerSize: 16 * 1024 * 1024,
	writeBufferManagerAllowStall: true,
});
const db = RocksDatabase.open(dbPath);

if (listenerOrder === 'after') {
	addListener();
}

process.exit(db.isOpen() ? 0 : 1);
