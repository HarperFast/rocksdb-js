import { type BackgroundErrorInfo, RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import { chmodSync, rmSync } from 'node:fs';
import { afterEach, describe, expect, it } from 'vitest';

// HarperFast/rocksdb-js#730: a filesystem-level write failure latches a RocksDB
// background error and the database refuses all writes until recovery. Before
// this fix there was no way to observe that state or recover in-process — only a
// restart cleared it. These tests exercise the new `db.backgroundError`
// observability and `db.resume()` recovery surface.
//
// The read-only-directory reproduction is skipped on Windows and when the
// process bypasses directory permission bits (root, e.g. inside some
// containers) — the same guard the existing permission-based file-lock tests
// use. GitHub-hosted CI runners are non-root, so it runs there.

describe('background error', () => {
	const cleanup: (() => void)[] = [];
	afterEach(() => {
		while (cleanup.length) {
			try {
				cleanup.pop()!();
			} catch {
				/* best effort */
			}
		}
	});

	function open(path: string): RocksDatabase {
		const db = new RocksDatabase(path);
		db.open();
		cleanup.push(() => {
			try {
				chmodSync(path, 0o755); // ensure teardown can remove the dir
			} catch {
				/* best effort */
			}
			db.close();
			if (!process.env.KEEP_FILES) {
				rmSync(path, { force: true, recursive: true, maxRetries: 3 });
			}
		});
		return db;
	}

	const skipReadOnlyDir = process.platform === 'win32' || process.getuid?.() === 0;

	it('reports no background error on a healthy database', () => {
		const db = open(generateDBPath());
		db.putSync('foo', 'bar');
		expect(db.backgroundError).toBeNull();
		// resume() is a safe no-op when there is nothing to recover (it still
		// exercises the DB::Resume() success path that clears the mirror).
		expect(() => db.resume()).not.toThrow();
		expect(db.backgroundError).toBeNull();
		expect(db.getSync('foo')).toBe('bar');
	});

	it.skipIf(skipReadOnlyDir)(
		'latches an observable, read-only background error when a write fails at the filesystem level',
		() => {
			const path = generateDBPath();
			const db = open(path);

			// Seed one healthy flush so the DB is fully initialized while writable.
			db.putSync('a', '1');
			db.flushSync();
			expect(db.backgroundError).toBeNull();

			// Make the directory read-only so the next flush cannot create its files.
			// RocksDB latches a background error and the database goes read-only.
			chmodSync(path, 0o555);
			db.putSync('b', '2');
			expect(() => db.flushSync()).toThrow();

			const err = db.backgroundError as BackgroundErrorInfo;
			expect(err).not.toBeNull();
			expect(typeof err.message).toBe('string');
			expect(err.message.length).toBeGreaterThan(0);
			// Hard (2) or worse: the database is read-only.
			expect(err.severity).toBeGreaterThanOrEqual(2);
			expect(['hard', 'fatal', 'unrecoverable']).toContain(err.severityName);
			if (err.reason !== undefined) {
				expect(typeof err.reasonName).toBe('string');
			}

			// A latched database refuses further writes — this is the read-only
			// state that, before this fix, a consumer had no way to observe.
			expect(() => db.putSync('c', '3')).toThrow();
		}
	);

	it.skipIf(skipReadOnlyDir)(
		'resume() reports failure and keeps the latch when the underlying condition has not cleared',
		() => {
			const path = generateDBPath();
			const db = open(path);

			db.putSync('a', '1');
			db.flushSync();

			chmodSync(path, 0o555);
			db.putSync('b', '2');
			expect(() => db.flushSync()).toThrow();
			expect(db.backgroundError).not.toBeNull();

			// The directory is still read-only, so recovery cannot succeed. resume()
			// must surface the failure rather than silently clear the mirror — the
			// invariant that keeps a consumer from believing a broken database is
			// healthy. Either it throws (and the latch remains) or, if RocksDB does
			// recover, backgroundError is genuinely null; it must never be "resumed
			// OK but still latched".
			let resumeThrew = false;
			try {
				db.resume();
			} catch {
				resumeThrew = true;
			}
			if (resumeThrew) {
				expect(db.backgroundError).not.toBeNull();
			} else {
				expect(db.backgroundError).toBeNull();
			}
		}
	);
});
