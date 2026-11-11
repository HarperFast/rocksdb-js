// A caller's synchronous boot purge over a log 2.13.0 wrote: adopt a baseline, commit a floor, sync
// the WAL, purge, then SIGKILL before any close or flush. The parent reopens and checks both.
import { RocksDatabase } from '../../src/index.ts';
import { utimesSync, writeSync } from 'node:fs';
import { join } from 'node:path';

const dbPath = process.argv[2];
const db = RocksDatabase.open(dbPath);
const log = db.useLog('legacy');
if (!log.setPurgeBaseline(3000).known) throw new Error('The baseline was not adopted');
db.putSync('floor', 4242);
db.syncWalSync();
const old = new Date(Date.now() - 2 * 60 * 60 * 1000);
utimesSync(join(dbPath, 'transaction_logs', 'legacy', '2.txnlog'), old, old);
const purged = db.purgeLogs({ name: 'legacy', before: Date.now() - 60 * 60 * 1000 });
if (purged.length !== 1) throw new Error(`Expected one purged segment, got ${purged.length}`);
writeSync(1, 'ready\n');
process.kill(process.pid, 'SIGKILL');
