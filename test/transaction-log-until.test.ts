import {
	CorruptFrameError,
	Transaction,
	TransactionLogUntilUnreachedError,
	type RocksDatabase,
	type TransactionLog,
	type TransactionLogIterator,
} from '../src/index.ts';
import { constants, forceTryAgainForTesting } from '../src/load-binding.ts';
import { dbRunner } from './lib/util.ts';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { describe, expect, it, vi } from 'vitest';

const {
	TRANSACTION_LOG_FILE_HEADER_SIZE: FILE_HEADER,
	TRANSACTION_LOG_ENTRY_HEADER_SIZE: ENTRY_HEADER,
} = constants;

function add(db: RocksDatabase, log: TransactionLog, text: string, timestamp?: number) {
	db.transactionSync((txn) => {
		if (timestamp !== undefined) txn.setTimestamp(timestamp);
		log.addEntry(Buffer.from(text), txn.id);
	});
}

function addAll(db: RocksDatabase, log: TransactionLog, count: number, size = 1) {
	for (let i = 0; i < count; i++) add(db, log, `${i}`.padEnd(size, 'x'));
}

function texts(iterator: TransactionLogIterator) {
	return Array.from(iterator, (entry) => entry.data.toString());
}

describe('Transaction log iterator handoff', () => {
	it('compares logs natively with equals() across cwd changes, aliases, subclasses and closed stores', () =>
		dbRunner({ skipOpen: true }, ({ dbPath }) => {
			const fixture = fileURLToPath(
				new URL('./fixtures/fork-transaction-log-until-identity.mts', import.meta.url)
			);
			const args = process.versions.deno ? ['run', '--allow-all'] : [];
			const child = spawnSync(process.execPath, [...args, fixture, dbPath], {
				encoding: 'utf8',
				timeout: 20_000,
			});
			expect(child.error).toBeUndefined();
			expect(child.status, child.stdout + child.stderr).toBe(0);
		}));

	it.each([
		{ size: 500 - FILE_HEADER - ENTRY_HEADER, reopen: false },
		{ size: 600, reopen: false },
		{ size: 500 - FILE_HEADER - ENTRY_HEADER, reopen: true },
		{ size: 600, reopen: true },
	])(
		'meets a live reader at an unwritten segment after a $size-byte entry (reopen: $reopen)',
		({ size, reopen }) =>
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 500 }] }, ({ db }) => {
				let log = db.useLog('foo');
				const payload = 'x'.repeat(size);
				add(db, log, payload);
				if (reopen) {
					db.close();
					db.open();
					log = db.useLog('foo');
				}
				expect(log.getStats().lastCommittedPosition).toEqual({ sequence: 2, offset: 0 });
				const live = log.query();
				const replay = log.query({ start: 0, until: live });
				expect(texts(replay)).toEqual([payload]);
				expect(live.peek().done).toBe(true);
				add(db, log, 'later');
				expect(live.peek().value?.data.toString()).toBe('later');
				expect(replay.next().done).toBe(true);
				expect(texts(live)).toEqual(['later']);
				expect(texts(replay)).toEqual(['later']);
			})
	);

	it('meets a live reader after retention empties the log, then tails new entries', () =>
		dbRunner({ dbOptions: [{ transactionLogMaxSize: 500 }] }, ({ db }) => {
			const log = db.useLog('foo');
			db.transactionSync((txn) => {
				txn.putSync('flush-key', true);
				log.addEntry(Buffer.alloc(600, 'x'), txn.id);
			});
			db.flushSync();
			expect(db.purgeLogs({ name: 'foo', before: Date.now() + 1000 })).toHaveLength(1);
			const live = log.query();
			const replay = log.query({ start: 0, until: live });
			expect(replay.next().done).toBe(true);
			add(db, log, 'later');
			expect(texts(live)).toEqual(['later']);
			expect(texts(replay)).toEqual(['later']);
		}));

	it('retries its original unmapped starting position without replaying older segments', () =>
		dbRunner({ dbOptions: [{ transactionLogMaxSize: 500 }] }, ({ db }) => {
			const log = db.useLog('foo');
			addAll(db, log, 2, 300);
			log.query();
			log._logBuffers.clear();
			log._currentLogBuffer = undefined;
			const original = log._getMemoryMapOfFile.bind(log);
			const map = vi
				.spyOn(log, '_getMemoryMapOfFile')
				.mockImplementation((id) => (id === 2 ? undefined : original(id)));
			try {
				const live = log.query();
				const replay = log.query({ start: 0, until: live });
				expect(replay.next().value?.data.toString()).toBe('0'.padEnd(300, 'x'));
				expect(() => replay.next()).toThrow(TransactionLogUntilUnreachedError);
				map.mockRestore();
				expect(texts(replay)).toEqual(['1'.padEnd(300, 'x')]);
				add(db, log, 'later');
				expect(texts(live)).toEqual(['later']);
				expect(texts(replay)).toEqual(['later']);
			} finally {
				map.mockRestore();
			}
		}));

	it('refuses committed catch-up to an uncommitted leader, then resumes after commit', () =>
		dbRunner(async ({ db }) => {
			const log = db.useLog('foo');
			add(db, log, 'committed');
			const txn = new Transaction(db.store);
			let committed = false;
			try {
				txn.putSync('pending-key', 'pending');
				log.addEntry(Buffer.from('pending'), txn.id);
				forceTryAgainForTesting(1);
				await expect(txn.commit()).rejects.toMatchObject({ code: 'ERR_TRY_AGAIN' });
				expect(txn.logWritten()).toBe(true);
				const committedLive = log.query({ start: 0 });
				expect(texts(committedLive)).toEqual(['committed']);
				const uncommittedReplay = log.query({
					start: 0,
					readUncommitted: true,
					until: committedLive,
				});
				expect(texts(uncommittedReplay)).toEqual(['committed']);
				const live = log.query({ start: 0, readUncommitted: true });
				expect(texts(live)).toEqual(['committed', 'pending']);
				const replay = log.query({ start: 0, until: live });
				expect(replay.next().value?.data.toString()).toBe('committed');
				expect(() => replay.peek()).toThrow(TransactionLogUntilUnreachedError);
				txn.putSync('pending-key', 'pending');
				await txn.commit();
				committed = true;
				expect(texts(replay)).toEqual(['pending']);
				expect(texts(committedLive)).toEqual(['pending']);
				expect(texts(uncommittedReplay)).toEqual(['pending']);
			} finally {
				forceTryAgainForTesting(0);
				if (!committed) txn.abort();
			}
		}));

	it('does not rewind its bound when a missing start is followed by an unmappable segment', () =>
		dbRunner({ dbOptions: [{ transactionLogMaxSize: 500 }] }, ({ db }) => {
			const log = db.useLog('foo');
			addAll(db, log, 2, 300);
			log.query();
			log._logBuffers.clear();
			log._currentLogBuffer = undefined;
			const originalMap = log._getMemoryMapOfFile.bind(log);
			const originalSize = log.getLogFileSize.bind(log);
			const map = vi
				.spyOn(log, '_getMemoryMapOfFile')
				.mockImplementation((id) => (id >= 2 ? undefined : originalMap(id)));
			const size = vi
				.spyOn(log, 'getLogFileSize')
				.mockImplementation((id) => (id === 2 ? 0 : originalSize(id)));
			try {
				const live = log.query();
				add(db, log, 'later'.padEnd(300));
				expect(live.next().done).toBe(true);
				const replay = log.query({ start: 0, until: live });
				expect(replay.next().value?.data.toString()).toBe('0'.padEnd(300, 'x'));
				expect(() => replay.next()).toThrow(TransactionLogUntilUnreachedError);
			} finally {
				map.mockRestore();
				size.mockRestore();
			}
		}));

	it('keeps an initially empty handoff at the first appended entry', () =>
		dbRunner(({ db }) => {
			const log = db.useLog('foo');
			const leader = log.query();
			const follower = log.query({ start: 0, until: leader });
			expect(follower.peek().done).toBe(true);
			add(db, log, 'first');
			expect(follower.next().done).toBe(true);
			expect(leader.peek().value?.data.toString()).toBe('first');
			expect(follower.next().done).toBe(true);
			leader.next();
			expect(texts(follower)).toEqual(['first']);
		}));

	it.each([false, true])(
		'does not probe a missing segment beyond the bound (filtered: %s)',
		(filtered) =>
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 500 }] }, ({ db }) => {
				const log = db.useLog('foo');
				add(db, log, 'before'.padEnd(600, 'x'));
				const live = log.query();
				add(db, log, 'excluded'.padEnd(600, 'x'));
				add(db, log, 'survivor');
				const originalMap = log._getMemoryMapOfFile.bind(log);
				const originalSize = log.getLogFileSize.bind(log);
				const map = vi
					.spyOn(log, '_getMemoryMapOfFile')
					.mockImplementation((id) => (id === 2 ? undefined : originalMap(id)));
				const size = vi
					.spyOn(log, 'getLogFileSize')
					.mockImplementation((id) => (id === 2 ? 0 : originalSize(id)));
				try {
					const replay = log.query({ start: 0, until: live, end: filtered ? 1 : undefined });
					expect(texts(replay)).toEqual(filtered ? [] : ['before'.padEnd(600, 'x')]);
					expect(replay.peek().done).toBe(true);
					expect(map.mock.calls.some(([id]) => id === 2)).toBe(false);
				} finally {
					map.mockRestore();
					size.mockRestore();
				}
			})
	);

	it('stops before until and resumes as it advances', () =>
		dbRunner(({ db }) => {
			const log = db.useLog('foo');
			addAll(db, log, 5);
			const leader = log.query({ start: 0 });
			const follower = log.query({ start: 0, until: leader });
			expect(texts(follower)).toEqual([]);
			leader.next();
			leader.next();
			expect(texts(follower)).toEqual(['0', '1']);
			expect(texts(follower)).toEqual([]);
			expect(texts(leader)).toEqual(['2', '3', '4']);
			expect(texts(follower)).toEqual(['2', '3', '4']);
		}));

	it('keeps a peek unread, including when the follower also peeks', () =>
		dbRunner(({ db }) => {
			const log = db.useLog('foo');
			addAll(db, log, 3);
			const leader = log.query({ start: 0 });
			const follower = log.query({ start: 0, until: leader });
			leader.next();
			const peeked = leader.peek();
			expect(peeked.value?.data.toString()).toBe('1');
			expect(leader.peek()).toBe(peeked);
			expect(follower.peek().value?.data.toString()).toBe('0');
			expect(texts(follower)).toEqual(['0']);
			expect(leader.next()).toBe(peeked);
			expect(texts(follower)).toEqual(['1']);
			expect(leader.next().value?.data.toString()).toBe('2');
		}));

	it('does not cache done and leaves a later lower-timestamp commit to the live reader', () =>
		dbRunner(({ db }) => {
			const log = db.useLog('foo');
			addAll(db, log, 2);
			const leader = log.query();
			const follower = log.query({ start: 0, until: leader });
			expect(leader.peek().done).toBe(true);
			expect(texts(follower)).toEqual(['0', '1']);
			add(db, log, 'late', 1);
			expect(texts(follower)).toEqual([]);
			expect(leader.peek().value?.data.toString()).toBe('late');
			expect(texts(follower)).toEqual([]);
			expect(texts(leader)).toEqual(['late']);
			expect(texts(follower)).toEqual(['late']);
		}));

	it('positions a filtered peek at the returned frame, across rotation', () =>
		dbRunner({ dbOptions: [{ transactionLogMaxSize: 500 }] }, ({ db }) => {
			const log = db.useLog('foo');
			add(db, log, 'first'.padEnd(300), 10);
			add(db, log, 'filtered'.padEnd(300), 30);
			add(db, log, 'peeked'.padEnd(300), 20);
			const leader = log.query({ start: 0, end: 25 });
			leader.next();
			expect(leader.peek().value?.data.toString().trim()).toBe('peeked');
			const follower = log.query({ start: 0, until: leader });
			expect(texts(follower).map((text) => text.trim())).toEqual(['first', 'filtered']);
			leader.next();
			expect(texts(follower).map((text) => text.trim())).toEqual(['peeked']);
		}));

	it('meets until across multiple segments', () =>
		dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, ({ db }) => {
			const log = db.useLog('foo');
			addAll(db, log, 20, 200);
			expect(log.getLogFileSize(2)).toBeGreaterThan(FILE_HEADER);
			const leader = log.query({ start: 0 });
			const follower = log.query({ start: 0, until: leader });
			const led = Array.from({ length: 12 }, () => leader.next().value!.data.toString());
			expect(texts(follower)).toEqual(led);
			const rest = texts(leader);
			expect(rest).toHaveLength(8);
			expect(texts(follower)).toEqual(rest);
		}));

	// In these filtered cases `witness` is bounded by the filtered reader, so what it returns is
	// exactly where the filtered reader's physical cursor stopped.
	it('stops a filtered scan at a moving until, a peek and a close', () =>
		dbRunner(({ db }) => {
			const log = db.useLog('foo');
			addAll(db, log, 6);
			const leader = log.query({ start: 0 });
			const filtered = log.query({ start: 0, end: 1, until: leader });
			const witness = log.query({ start: 0, until: filtered });
			leader.next();
			leader.next();
			expect(filtered.next().done).toBe(true);
			expect(texts(witness)).toEqual(['0', '1']);
			expect(leader.peek().value?.data.toString()).toBe('2');
			expect(filtered.next().done).toBe(true);
			expect(texts(witness)).toEqual([]);
			leader.next();
			leader.next();
			expect(filtered.next().done).toBe(true);
			expect(texts(witness)).toEqual(['2', '3']);
			leader.close();
			expect(filtered.next().done).toBe(true);
			expect(texts(witness)).toEqual(['4', '5']);
		}));

	it('stops a filtered scan at until in a later segment', () =>
		dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, ({ db }) => {
			const log = db.useLog('foo');
			addAll(db, log, 20, 200);
			expect(log.getLogFileSize(3)).toBeGreaterThan(FILE_HEADER);
			const leader = log.query({ start: 0 });
			const filtered = log.query({ start: 0, end: 1, until: leader });
			const witness = log.query({ start: 0, until: filtered });
			// mid-segment: a bound at a segment's end would also stop a scan left on a stale limit
			const led = Array.from({ length: 10 }, () => leader.next().value!.data.toString());
			expect(filtered.next().done).toBe(true);
			expect(texts(witness)).toEqual(led);
			const rest = texts(leader);
			expect(rest).toHaveLength(10);
			expect(filtered.next().done).toBe(true);
			expect(texts(witness)).toEqual(rest);
		}));

	it('stops immediately when starting in a segment past until', () =>
		dbRunner({ dbOptions: [{ transactionLogMaxSize: 500 }] }, ({ db }) => {
			const log = db.useLog('foo');
			for (let i = 0; i < 4; i++) add(db, log, `${i}`.padEnd(600, 'x'), 10 + i);
			const leader = log.query({ start: 0 });
			leader.next();
			const follower = log.query({ start: 13, until: leader });
			expect(follower.next().done).toBe(true);
			expect(texts(leader).map((text) => text[0])).toEqual(['1', '2', '3']);
			expect(texts(follower).map((text) => text[0])).toEqual(['3']);
		}));

	it.each([false, true])(
		're-applies until after a filtered scan outruns its size snapshot (rotated: %s)',
		(rotated) =>
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, ({ db }) => {
				const log = db.useLog('foo');
				addAll(db, log, 2, 100);
				const leader = log.query({ start: 0 });
				const filtered = log.query({ start: 0, end: 1, until: leader });
				const witness = log.query({ start: 0, until: filtered });
				leader.next();
				expect(filtered.next().done).toBe(true);
				addAll(db, log, 3, 100);
				if (rotated) {
					add(db, log, 'rotates'.padEnd(800, 'x'));
					expect(log.getStats().lastCommittedPosition?.sequence).toBe(2);
				}
				leader.next();
				leader.next();
				expect(filtered.next().done).toBe(true);
				expect(texts(witness).map((text) => text[0])).toEqual(['0', '1', '0']);
				leader.next();
				expect(filtered.next().done).toBe(true);
				expect(texts(witness).map((text) => text[0])).toEqual(['1']);
			})
	);

	it.each([false, true])(
		'covers a grown segment before meeting until at the next header (uncommitted: %s)',
		(readUncommitted) =>
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, ({ db }) => {
				const log = db.useLog('foo');
				addAll(db, log, 2, 100);
				const leader = log.query({ start: 0 });
				const filtered = log.query({ start: 0, end: 1, until: leader, readUncommitted });
				const witness = log.query({ start: 0, until: filtered });
				leader.next();
				expect(filtered.next().done).toBe(true);
				addAll(db, log, 2, 100);
				add(db, log, 'rotates'.padEnd(800, 'x'));
				expect(log.getStats().lastCommittedPosition?.sequence).toBe(2);
				for (let i = 0; i < 3; i++) leader.next();
				expect(leader.peek().value?.data.toString()[0]).toBe('r');
				expect(filtered.next().done).toBe(true);
				expect(texts(witness).map((text) => text[0])).toEqual(['0', '1', '0', '1']);
			})
	);

	it('applies until to filtered scans in both committed and uncommitted directions', () =>
		dbRunner(async ({ db }) => {
			const log = db.useLog('foo');
			add(db, log, 'committed');
			const txn = new Transaction(db.store);
			let committed = false;
			try {
				txn.putSync('pending-key', 'pending');
				log.addEntry(Buffer.from('pending'), txn.id);
				forceTryAgainForTesting(1);
				await expect(txn.commit()).rejects.toMatchObject({ code: 'ERR_TRY_AGAIN' });
				const committedLive = log.query({ start: 0 });
				expect(texts(committedLive)).toEqual(['committed']);
				const uncommitted = log.query({
					start: 0,
					end: 1,
					readUncommitted: true,
					until: committedLive,
				});
				expect(uncommitted.next().done).toBe(true);
				const witness = log.query({ start: 0, readUncommitted: true, until: uncommitted });
				expect(texts(witness)).toEqual(['committed']);
				const live = log.query({ start: 0, readUncommitted: true });
				expect(texts(live)).toEqual(['committed', 'pending']);
				const filtered = log.query({ start: 0, end: 1, until: live });
				expect(() => filtered.next()).toThrow(TransactionLogUntilUnreachedError);
				txn.putSync('pending-key', 'pending');
				await txn.commit();
				committed = true;
				expect(filtered.next().done).toBe(true);
			} finally {
				forceTryAgainForTesting(0);
				if (!committed) txn.abort();
			}
		}));

	it('closes permanently and releases followers, including a pending peek', () =>
		dbRunner(({ db }) => {
			const log = db.useLog('foo');
			addAll(db, log, 3);
			const leader = log.query({ start: 0 });
			const follower = log.query({ start: 0, until: leader });
			leader.peek();
			expect(texts(follower)).toEqual([]);
			leader.close();
			leader.close();
			expect(leader.next().done).toBe(true);
			expect(leader.peek().done).toBe(true);
			expect(texts(follower)).toEqual(['0', '1', '2']);
			add(db, log, 'later');
			expect(leader.peek().done).toBe(true);
			expect(texts(log.query({ start: 0, until: leader }))).toEqual(['0', '1', '2', 'later']);
		}));

	it('preserves for-of resumption and observes peek and close inside the loop', () =>
		dbRunner(({ db }) => {
			const log = db.useLog('foo');
			addAll(db, log, 4);
			const iterator = log.query({ start: 0 });
			for (const entry of iterator) {
				expect(entry.data.toString()).toBe('0');
				break;
			}
			const found: string[] = [];
			for (const entry of iterator) {
				found.push(entry.data.toString());
				if (found.length === 1) expect(iterator.peek().value?.data.toString()).toBe('2');
				else iterator.close();
			}
			expect(found).toEqual(['1', '2']);
		}));

	it('rejects every supplied invalid until and iterators from other logs', () =>
		dbRunner(({ db }) => {
			const log = db.useLog('foo');
			const leader = log.query();
			expect(() => Reflect.construct(leader.constructor, [])).toThrow(TypeError);
			for (const until of [
				null,
				false,
				0,
				'',
				{},
				Object.create(Object.getPrototypeOf(leader)),
				[][Symbol.iterator](),
				db.useLog('bar').query(),
			]) {
				expect(() => log.query({ until: until as TransactionLogIterator })).toThrow(TypeError);
			}
		}));

	it('accepts separate handles to the same log', () =>
		dbRunner(({ db }, { db: other }) => {
			const log = db.useLog('foo');
			addAll(db, log, 2);
			const leader = other.useLog('foo').query({ start: 0 });
			leader.next();
			expect(texts(log.query({ start: 0, until: leader }))).toEqual(['0']);
		}));

	it('rejects the same log name in a different database', () =>
		dbRunner(async ({ db }) => {
			const log = db.useLog('foo');
			await dbRunner(({ db: other }) => {
				expect(() => log.query({ until: other.useLog('foo').query() })).toThrow(TypeError);
			});
		}));

	it.each([false, true])(
		'stops immediately when starting past until (exactStart: %s)',
		(exactStart) =>
			dbRunner(({ db }) => {
				const log = db.useLog('foo');
				addAll(db, log, 2);
				const leader = log.query({ start: 0 });
				expect(texts(log.query({ start: Date.now() + 1000, exactStart, until: leader }))).toEqual(
					[]
				);
				expect(texts(leader)).toEqual(['0', '1']);
			})
	);

	it.each([false, true])(
		'throws short of until on an unmappable segment (filtered: %s)',
		(filtered) =>
			dbRunner({ dbOptions: [{ transactionLogMaxSize: 500 }] }, ({ db }) => {
				const log = db.useLog('foo');
				addAll(db, log, 4, 300);
				const leader = log.query({ start: 0 });
				leader.next();
				leader.next();
				leader.next();
				log._logBuffers.clear();
				log._currentLogBuffer = undefined;
				const original = log._getMemoryMapOfFile.bind(log);
				const map = vi
					.spyOn(log, '_getMemoryMapOfFile')
					.mockImplementation((id) => (id === 2 ? undefined : original(id)));
				try {
					const follower = log.query({ start: 0, end: filtered ? 1 : undefined, until: leader });
					if (!filtered) expect(follower.next().value?.data[0]).toBe(48);
					expect(() => follower.peek()).toThrow(/short of the iterator/);
					expect(() => follower.next()).toThrow(/short of the iterator/);
					map.mockRestore();
					expect(texts(follower)).toEqual(
						filtered ? [] : ['1'.padEnd(300, 'x'), '2'.padEnd(300, 'x')]
					);
				} finally {
					map.mockRestore();
				}
			})
	);

	it('does not jump an exact-start miss past until', () =>
		dbRunner(({ db }) => {
			const log = db.useLog('foo');
			add(db, log, 'before', 10);
			add(db, log, 'after', 30);
			const leader = log.query({ start: 0 });
			leader.next();
			const follower = log.query({ start: 20, exactStart: true, until: leader });
			expect(follower.next().done).toBe(true);
			expect(texts(log.query({ start: 0, until: follower }))).toEqual(['before']);
		}));

	it('keeps the exact-start skip when its target is at the bound', () =>
		dbRunner(({ db }) => {
			const log = db.useLog('foo');
			add(db, log, 'before', 10);
			for (let i = 0; i < 200; i++) add(db, log, 'after', 30 + i);
			const live = log.query();
			const read = vi.spyOn(DataView.prototype, 'getFloat64');
			try {
				expect(texts(log.query({ start: 20, exactStart: true, until: live }))).toEqual([]);
				expect(read.mock.calls.length).toBeLessThan(10);
			} finally {
				read.mockRestore();
			}
		}));

	it('takes an exact-start skip on a later pull after until passes its target', () =>
		dbRunner(({ db }) => {
			const log = db.useLog('foo');
			add(db, log, 'before', 10);
			for (let i = 0; i < 200; i++) add(db, log, 'after', 30 + i);
			const leader = log.query({ start: 0 });
			for (let i = 0; i < 6; i++) leader.next();
			const follower = log.query({ start: 20, exactStart: true, until: leader });
			expect(follower.next().done).toBe(true);
			const witness = log.query({ start: 0, until: follower });
			expect(texts(witness)).toHaveLength(6);
			expect(texts(leader)).toHaveLength(195);
			const read = vi.spyOn(DataView.prototype, 'getFloat64');
			try {
				expect(texts(follower)).toEqual([]);
				expect(read.mock.calls.length).toBeLessThan(10);
			} finally {
				read.mockRestore();
			}
			expect(texts(witness)).toHaveLength(195);
		}));

	describe('with a damaged frame between the reader and until', () => {
		// The copy is what a follower maps; `leader` keeps reading the intact file, so the two
		// disagree about framing the way they would after corruption the leader never read.
		function damaged(
			log: TransactionLog,
			damage: (copy: Buffer, frame: (i: number) => number) => void
		) {
			const real = log._getMemoryMapOfFile(1)!;
			const copy = Buffer.from(new ArrayBuffer(real.length));
			real.copy(copy);
			Object.defineProperty(copy, 'readableExtent', { value: real.readableExtent });
			damage(copy, (i) => FILE_HEADER + i * (ENTRY_HEADER + 1));
			log._logBuffers.clear();
			log._currentLogBuffer = undefined;
			return vi.spyOn(log, '_getMemoryMapOfFile').mockReturnValue(copy as typeof real);
		}

		it.each([false, true])('reads a frame straddling until whole (filtered: %s)', (filtered) =>
			dbRunner(({ db }) => {
				const log = db.useLog('foo');
				addAll(db, log, 12);
				const leader = log.query({ start: 0 });
				for (let i = 0; i < 4; i++) leader.next();
				const spanned = 4 * (ENTRY_HEADER + 1) - ENTRY_HEADER;
				const map = damaged(log, (copy, frame) => copy.writeUInt32BE(spanned, frame(2) + 8));
				try {
					const follower = log.query({ start: 0, end: filtered ? 1 : undefined, until: leader });
					const lengths = Array.from(follower, (entry) => entry.data.length);
					expect(lengths).toEqual(filtered ? [] : [1, 1, spanned]);
					expect(follower.next().done).toBe(true);
				} finally {
					map.mockRestore();
				}
			})
		);

		it.each([false, true])(
			'stops at until when a corrupt frame resyncs past it (filtered: %s)',
			(filtered) =>
				dbRunner(({ db }) => {
					const log = db.useLog('foo');
					addAll(db, log, 12);
					const leader = log.query({ start: 0 });
					for (let i = 0; i < 4; i++) leader.next();
					const map = damaged(log, (copy, frame) => {
						for (let i = 2; i < 6; i++) copy.writeUInt32BE(0, frame(i) + 8);
					});
					try {
						const follower = log.query({ start: 0, end: filtered ? 1 : undefined, until: leader });
						if (!filtered)
							expect([follower.next(), follower.next()].map((r) => r.done)).toEqual([false, false]);
						expect(() => follower.next()).toThrow(CorruptFrameError);
						expect(follower.next().done).toBe(true);
						expect(follower.peek().done).toBe(true);
					} finally {
						map.mockRestore();
					}
				})
		);
	});

	it('checks until before parsing an aborted or corrupt frame, then preserves recovery', () =>
		dbRunner(({ db }) => {
			const log = db.useLog('foo');
			addAll(db, log, 12);
			const real = log._getMemoryMapOfFile(1)!;
			const copy = Buffer.from(new ArrayBuffer(real.length));
			real.copy(copy);
			Object.defineProperty(copy, 'readableExtent', { value: real.readableExtent });
			const frame = (i: number) => FILE_HEADER + i * (ENTRY_HEADER + 1);
			copy[frame(1) + ENTRY_HEADER - 1] |= constants.TRANSACTION_LOG_ENTRY_ABORTED_FLAG;
			copy.writeUInt32BE(0, frame(2) + 8);
			log.query();
			log._logBuffers.clear();
			log._currentLogBuffer = undefined;
			const map = vi.spyOn(log, '_getMemoryMapOfFile').mockReturnValue(copy as typeof real);
			try {
				const leader = log.query({ start: 0 });
				const follower = log.query({ start: 0, until: leader });
				leader.next();
				expect(texts(follower)).toEqual(['0']);
				expect(() => leader.peek()).toThrow(CorruptFrameError);
				expect(() => follower.peek()).toThrow(CorruptFrameError);
				expect(leader.peek().value?.data.toString()).toBe('3');
				expect(follower.next().done).toBe(true);
				expect(texts(leader)).toEqual(['3', '4', '5', '6', '7', '8', '9', '10', '11']);
				expect(texts(follower)).toEqual(['3', '4', '5', '6', '7', '8', '9', '10', '11']);
			} finally {
				map.mockRestore();
			}
		}));
});
