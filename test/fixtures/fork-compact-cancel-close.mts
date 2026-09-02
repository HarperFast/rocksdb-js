import { RocksDatabase } from '../../src/index.ts';
import { setTimeout as delay } from 'node:timers/promises';

// The self-close half of the manual-compaction cancellation contract (see
// DBHandle::compactCancelRequested). Unlike the two fork-compact-cancel-*
// fixtures, nothing foreign is involved: this process owns the database and
// closes it while its own async compact() is running.
//
// That ordering is what the descriptor-wide token cannot cover.
// DBRegistry::CloseDB reaches DBHandle::close() -- which cancels async work and
// then waits for it *without a timeout* -- before it reaches
// PurgeIfUnreferenced/beginClose(), the only site that arms the descriptor
// token. An async compact() checks the handle's cancelled flag once at entry
// and then blocks inside RocksDB, which reads CompactRangeOptions::canceled and
// nothing else, so a close that only sets the flag parks the JS thread for the
// compaction's full duration (minutes to hours on a large column family).
//
// The seam parks the compaction until its token is armed, so "close returned
// promptly" is a direct assertion that close armed a token the compaction was
// actually given. Fails if the async compact goes back to the descriptor token,
// or if arming moves after the async-work drain.
const path = process.argv[2];
const db = RocksDatabase.open(path);
for (let i = 0; i < 20; i++) {
	db.putSync(`key-${i}`, i);
}
db.flushSync();

// Start the compaction and let the libuv worker park it in the seam before
// closing; otherwise the execute callback's own opened()/isCancelled() check
// rejects it at entry and the token is never exercised.
const compacting = db.compact();
let compactSettled = false;
const outcome = compacting.then(
	() => {
		compactSettled = true;
		return null;
	},
	(error: unknown) => {
		compactSettled = true;
		return error;
	}
);
await delay(250);
if (compactSettled) throw new Error('Compaction settled before close; seam not active');

const started = Date.now();
db.close();
const closeElapsed = Date.now() - started;

const compactError = await outcome;

if (closeElapsed >= 2000)
	throw new Error(
		`db.close() blocked ${closeElapsed}ms on its own in-flight compact() -- ` +
			'the async compaction is not being handed a cancel token that close() arms'
	);
if (!compactError)
	throw new Error('Expected the in-flight compact() to be cancelled by close(), but it resolved');
if (String(compactError).includes('Database closed during compact operation'))
	throw new Error(
		'Compaction was rejected at entry rather than cancelled mid-flight; the fixture ' +
			'no longer exercises the cancel token'
	);
if (!/cancel|paused|incomplete/i.test(String(compactError))) throw compactError;

// Clearing the token on reopen is covered by test/lifecycle.test.ts's
// close/open cycle, which awaits a compact() on the reopened instance; a token
// left armed cancels it. Re-asserting it here would mean parking a second
// compaction in the seam for its full bound.
db.destroy();
