// Regenerates `store/`: a transaction-log store written, and partly purged, by rocksdb-js 2.13.0,
// the last release without a purge watermark. Not run by the tests.
//
//   mkdir /tmp/rjs213 && cd /tmp/rjs213 && npm init -y && npm i @harperfast/rocksdb-js@2.13.0
//   node <repo>/test/fixtures/txnlog-2.13.0/generate.mjs /tmp/rjs213
//
// Segments (two 100-byte entries each, keyed by `setTimestamp()`):
//   1: 1000, AHEAD_1  deleted by 2.13.0's purgeLogs, so nothing on disk records its keys
//   2: 2000, AHEAD_2  survives: an old segment holding a key ahead of the clock
//   3: 3000           current
import { cpSync, mkdtempSync, readdirSync, rmSync, utimesSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const AHEAD_1 = Date.UTC(2100, 0, 1);
const AHEAD_2 = AHEAD_1 + 1000;

const installDir = process.argv[2];
const require = createRequire(join(installDir, 'package.json'));
const { RocksDatabase } = require('@harperfast/rocksdb-js');
const out = join(dirname(fileURLToPath(import.meta.url)), 'store');
const dbPath = mkdtempSync(join(tmpdir(), 'txnlog-2.13.0-'));

const db = RocksDatabase.open(dbPath, { transactionLogMaxSize: 256 });
const log = db.useLog('legacy');
for (const key of [1000, AHEAD_1, 2000, AHEAD_2, 3000]) {
	await db.transaction(async (txn) => {
		txn.setTimestamp(key);
		txn.putSync(`key-${key}`, key);
		log.addEntry(Buffer.alloc(100, 'x'), txn.id);
	});
}
// The flush is what writes txn.state, without which no segment is purgeable.
db.flushSync();
const storePath = join(dbPath, 'transaction_logs', 'legacy');
const old = new Date(Date.now() - 2 * 60 * 60 * 1000);
utimesSync(join(storePath, '1.txnlog'), old, old);
db.purgeLogs({ before: Date.now() - 60 * 60 * 1000 });
db.close();

rmSync(out, { recursive: true, force: true });
cpSync(storePath, out, { recursive: true });
rmSync(dbPath, { recursive: true, force: true });
console.log(readdirSync(out));
