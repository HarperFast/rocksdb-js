import {
	CorruptFrameError,
	type RocksDatabase,
	type TransactionLog,
	type TransactionLogIterator,
} from '../src/index.ts';
import { constants } from '../src/load-binding.ts';
import { dbRunner } from './lib/util.ts';
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
			for (const until of [
				null,
				false,
				0,
				'',
				{},
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
			// If the miss skipped to the tail, this new follower could read 'after'.
			expect(texts(log.query({ start: 0, until: follower }))).toEqual(['before']);
		}));

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
