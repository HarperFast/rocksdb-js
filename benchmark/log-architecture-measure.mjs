import { RocksDatabase } from '../dist/index.mjs';
/**
 * Log-architecture measurements for the stage-1 ruling (design §3.4). Run
 * after `pnpm build`: node benchmark/log-architecture-measure.mjs
 * M1 marginal log-append cost/commit; M2 sequential scan rate; M3 indexed
 * seek-by-timestamp latency.
 */
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const root = join(tmpdir(), `rocksdbjs-log-arch-${process.pid}`);
rmSync(root, { recursive: true, force: true });
mkdirSync(root, { recursive: true });

const payload = Buffer.alloc(128, 'x');

function hrms() {
	return Number(process.hrtime.bigint()) / 1e6;
}

function commitBatch(db, log, entriesPerTxn, txns) {
	const start = hrms();
	for (let i = 0; i < txns; i++) {
		db.transactionSync((txn) => {
			txn.putSync(`k${i & 1023}`, payload);
			for (let e = 0; e < entriesPerTxn; e++) {
				log.addEntry(payload, txn.id);
			}
		});
	}
	return ((hrms() - start) / txns) * 1000; // µs per commit
}

try {
	{
		const db = RocksDatabase.open(join(root, 'm1'), {
			encoding: 'binary',
			transactionLogsPath: join(root, 'm1-logs'),
		});
		const log = db.useLog('local');
		const WARM = 2000;
		const N = 20000;
		commitBatch(db, log, 0, WARM);
		commitBatch(db, log, 1, WARM);
		const noLog = commitBatch(db, log, 0, N);
		const oneEntry = commitBatch(db, log, 1, N);
		const fourEntry = commitBatch(db, log, 4, N);
		console.log(
			`M1 append path: no-log=${noLog.toFixed(2)}µs one-entry=${oneEntry.toFixed(2)}µs ` +
				`(marginal full path +${(oneEntry - noLog).toFixed(2)}µs, ` +
				`per extra entry +${((fourEntry - oneEntry) / 3).toFixed(2)}µs)`
		);
		db.close();
	}

	{
		const db = RocksDatabase.open(join(root, 'm2'), {
			encoding: 'binary',
			transactionLogsPath: join(root, 'm2-logs'),
		});
		const log = db.useLog('local');
		const ENTRIES = 200000;
		const PER_TXN = 10;
		for (let i = 0; i < ENTRIES / PER_TXN; i++) {
			db.transactionSync((txn) => {
				for (let e = 0; e < PER_TXN; e++) {
					log.addEntry(payload, txn.id);
				}
			});
		}

		let count = 0;
		const drainStart = hrms();
		for (const _entry of log.query({ start: 1 })) count++;
		const drainMs = hrms() - drainStart;
		console.log(
			`M2 scan: ${count} entries in ${drainMs.toFixed(1)}ms ` +
				`(${Math.round(count / (drainMs / 1000)).toLocaleString()} entries/s, ` +
				`${(((count * (128 + 13)) / 1e6 / drainMs) * 1000).toFixed(0)} MB/s)`
		);

		let mid = 0;
		let scanned = 0;
		for (const entry of log.query({ start: 1 })) {
			if (++scanned >= count / 2) {
				mid = entry.timestamp;
				break;
			}
		}
		const coldStart = hrms();
		for (const _entry of log.query({ start: mid })) break;
		const coldMs = hrms() - coldStart;
		const warmStart = hrms();
		for (const _entry of log.query({ start: mid })) break;
		const warmMs = hrms() - warmStart;
		console.log(
			`M3 index seek: cold=${(coldMs * 1000).toFixed(0)}µs warm=${(warmMs * 1000).toFixed(1)}µs`
		);
		db.close();
	}
} finally {
	rmSync(root, { recursive: true, force: true });
}
