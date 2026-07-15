// Regression test for HarperFast/harper#1695 / #1696, verified at the rocksdb-js layer.
//
// An optimistic commit returns TryAgain (not Busy) when the transaction's snapshot is
// stranded outside the memtable window — the sequence history the conflict check needs was
// flushed away. Recommitting the *same* transaction re-checks the same lost history and fails
// forever, so before this fix TryAgain was not retried at all (db.transaction() abandoned it)
// and, worse, the commitFinished visibility gate keyed off `!IsBusy`, so a failed TryAgain
// commit still published its log entries — the change-feed entry became visible while the
// record was rolled back.
//
// The fix treats TryAgain like Busy: the native layer resets the transaction onto a fresh
// snapshot (committedPosition survives, so the WAL stays write-once, #668) and the retry
// re-runs the body to validate against current state. commitFinished now fires only on a real
// commit, so the entry is published exactly once, after the data lands — no premature publish,
// no phantom entry if the retry is ultimately abandoned.
//
// This forces exactly that stranded-snapshot TryAgain and asserts the retry converges on the
// *same* native transaction (harper#1696 replayed onto a fresh transaction at the JS layer; the
// native reset makes that unnecessary), writing the log entry once and keeping committed reads
// intact.

import { constants, forceTryAgainForTesting } from '../src/load-binding.js';
import { Transaction } from '../src/transaction.js';
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

// Record the commit-error code of every failed commit attempt so the test can assert the
// failure was actually TryAgain (a Busy conflict would otherwise pass silently and prove nothing).
function spyOnCommitCodes() {
	const original = Transaction.prototype.commit;
	const codes: (string | undefined)[] = [];
	Transaction.prototype.commit = function (this: Transaction, ...args: []) {
		return original.apply(this, args).then(undefined, (err: { code?: string }) => {
			codes.push(err?.code);
			throw err;
		});
	};
	return { codes, restore: () => (Transaction.prototype.commit = original) };
}

describe('#1695 ERR_TRY_AGAIN retry resets onto a fresh snapshot and converges', () => {
	it('converges on the same transaction after a stranded snapshot, WAL written once', () =>
		dbRunner({ dbOptions: [{ path: generateDBPath() }] }, async ({ db, dbPath }) => {
			const key = 'k';
			const payload = Buffer.alloc(128, 'z');
			const log = db.useLog('repl');
			await db.put(key, 'initial');

			// Strand exactly the first commit with a real TryAgain (rolled back, so no data lands).
			// A genuine memtable flush produces the same status, but staging one deterministically
			// through the public API hinges on OCC memtable-history eviction and is flaky; the seam
			// exercises the native reset + JS retry path directly. Disarmed in the finally.
			forceTryAgainForTesting(1);
			const { codes, restore } = spyOnCommitCodes();
			const txnIds = new Set<number>();
			let attempts = 0;
			try {
				await db.transaction(
					async (txn, attempt) => {
						attempts = attempt;
						txnIds.add(txn.id);
						if (attempt === 2) {
							// Entering the retry: attempt 1 wrote its log batch, then failed TryAgain and
							// rolled back. The entry must NOT be visible yet — commitFinished fires only on a
							// real commit, so the watermark did not advance on the failed attempt. (Under the
							// old `!IsBusy` gate this query would already return the prematurely-published
							// entry, ahead of its never-committed data.)
							expect([...db.useLog('repl').query({ start: 0 })].length).toBe(0);
						}
						// The retry re-runs on the reset transaction's fresh snapshot; the put re-tracks
						// the key at the current sequence, so the commit validates cleanly.
						await txn.put(key, 'committed');
						log.addEntry(payload, txn.id);
					},
					{ retryOnBusy: true, maxRetries: 5 }
				);
			} finally {
				restore();
				forceTryAgainForTesting(0);
			}

			// Premise: the first attempt actually failed with TryAgain (not Busy), else the test
			// would prove nothing about the TryAgain path.
			expect(codes).toContain('ERR_TRY_AGAIN');
			// A retry actually happened.
			expect(attempts).toBeGreaterThanOrEqual(2);
			// The reset reuses one native transaction across attempts (stable id) — the whole point
			// of aligning with the Busy reset path instead of building a fresh transaction.
			expect(txnIds.size).toBe(1);

			// The transaction ultimately committed its value.
			expect(await db.get(key)).toBe('committed');

			// The WAL holds exactly one copy — committedPosition survived the reset, so the retry
			// did not rewrite the batch.
			const logDir = join(dbPath, 'transaction_logs', 'repl');
			const bytes = await transactionLogBytes(logDir);
			expect(bytes).toBe(
				TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + payload.length
			);

			// A committed read from the start reaches the entry — the watermark advanced to head on
			// the successful commit, not on the earlier failed TryAgain attempt.
			const entries = [...db.useLog('repl').query({ start: 0 })];
			expect(entries.length).toBe(1);
			expect(Buffer.from(entries[0].data).equals(payload)).toBe(true);
		}));
});
