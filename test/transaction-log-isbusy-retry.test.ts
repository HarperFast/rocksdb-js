// Regression test for HarperFast/rocksdb-js#668.
//
// The write-ahead log is written once per transaction; an IsBusy commit retry
// re-runs the transaction body to re-drive the RocksDB commit, but must not
// rewrite the WAL. Before the fix, the retry re-staged the log entries, so
// writeBatch() ran a second time and wrote the records again at a fresh
// position. The first copy was orphaned (commitFinished is gated on !IsBusy, so
// it was never finalized), which pinned the committed-read watermark at it
// forever and silently truncated committed reads (HarperFast/harper-pro#426).
//
// This test forces exactly one IsBusy retry on a logged transaction via the
// db.transaction() retry loop and asserts the log holds a single copy of the
// record (not one per attempt) and that the committed read still sees it.

import { constants } from '../src/load-binding.js';
import { dbRunner, generateDBPath } from './lib/util.js';
import { readdir, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const { TRANSACTION_LOG_FILE_HEADER_SIZE, TRANSACTION_LOG_ENTRY_HEADER_SIZE } = constants;

async function transactionLogBytes(logDir: string): Promise<number> {
	let size = 0;
	for (const file of await readdir(logDir).catch(() => [])) {
		const info = await stat(join(logDir, file)).catch(() => undefined);
		if (info?.isFile()) {
			size += info.size;
		}
	}
	return size;
}

describe('#668 IsBusy retry does not rewrite the WAL', () => {
	it('writes the log entry once across an IsBusy retry and keeps committed reads intact', () =>
		dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
			const key = 'k';
			const payload = Buffer.alloc(128, 'z');
			const log = db.useLog('repl');

			let attempts = 0;
			await db.put(key, 'initial');

			await db.transaction(
				async (txn, attempt) => {
					attempts = attempt;
					// Establish the snapshot before injecting the conflict.
					await txn.get(key);
					// Inject an external write only on the first attempt so attempt 1
					// hits an optimistic conflict (IsBusy) and the retry then commits.
					if (attempt === 1) {
						await db.put(key, 'conflict');
					}
					await txn.put(key, 'committed');
					log.addEntry(payload, txn.id);
				},
				{ retryOnBusy: true, maxRetries: 5 }
			);

			// The retry must actually have happened, otherwise the test proves nothing.
			expect(attempts).toBeGreaterThanOrEqual(2);

			// The transaction ultimately committed its value.
			expect(await db.get(key)).toBe('committed');

			// The WAL holds exactly one copy of the record — not one per attempt.
			const logDir = join(dbPath, 'transaction_logs', 'repl');
			const bytes = await transactionLogBytes(logDir);
			expect(bytes).toBe(
				TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + payload.length
			);

			// And a committed read from the start reaches it — the watermark advanced to head
			// rather than being pinned short of it (the #668 failure mode would truncate here).
			const entries = [...db.useLog('repl').query({ start: 0 })];
			expect(entries.length).toBe(1);
			expect(Buffer.from(entries[0].data).equals(payload)).toBe(true);
		}));
});
