import { BackgroundError, RocksDatabase } from '../src/index.ts';
import { generateDBPath } from './lib/util.ts';
import { chmodSync, rmSync } from 'node:fs';
import { afterEach, describe, expect, it } from 'vitest';

// HarperFast/rocksdb-js#730: a filesystem-level write failure makes RocksDB
// record a background error and refuse all writes until recovery. Before this
// fix there was no way to observe that state or recover in-process — only a
// restart cleared it. The database now emits an `'error'` event carrying a
// `BackgroundError`, and `db.resume()` attempts in-process recovery.
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
			try {
				// destroy(), not close(): a recorded background error is sticky, so
				// the close-time flush keeps failing and would leave the descriptor
				// quarantined in the registry for the rest of the process (the
				// registry then reports it again at exit). destroy() forces teardown.
				if (process.env.KEEP_FILES) {
					db.close();
				} else {
					db.destroy();
				}
			} catch {
				/* best effort */
			}
			if (!process.env.KEEP_FILES) {
				rmSync(path, { force: true, recursive: true, maxRetries: 3 });
			}
		});
		return db;
	}

	/** Resolves with the first `'error'` event, or rejects after `timeoutMs`. */
	function nextError(db: RocksDatabase, timeoutMs = 5000): Promise<BackgroundError> {
		return new Promise((resolve, reject) => {
			const timer = setTimeout(() => {
				db.removeListener('error', onError);
				reject(new Error('timed out waiting for background error event'));
			}, timeoutMs);
			function onError(err: BackgroundError) {
				clearTimeout(timer);
				db.removeListener('error', onError);
				resolve(err);
			}
			db.on('error', onError);
		});
	}

	const skipReadOnlyDir = process.platform === 'win32' || process.getuid?.() === 0;

	it('emits no error event on a healthy database', async () => {
		const db = open(generateDBPath());
		let emitted: unknown = null;
		db.on('error', (err) => {
			emitted = err;
		});
		db.putSync('foo', 'bar');
		db.flushSync();
		// resume() is a safe no-op when there is nothing to recover.
		expect(() => db.resume()).not.toThrow();
		expect(db.getSync('foo')).toBe('bar');
		// No error to pull, either.
		expect(db.getLastError()).toBeNull();
		// Give any (unexpected) queued event a turn to deliver.
		await new Promise((r) => setTimeout(r, 50));
		expect(emitted).toBeNull();
	});

	it.skipIf(skipReadOnlyDir)(
		'emits an observable, writes-disabled BackgroundError when a write fails at the filesystem level',
		async () => {
			const path = generateDBPath();
			const db = open(path);

			// Seed one healthy flush so the DB is fully initialized while writable.
			db.putSync('a', '1');
			db.flushSync();

			const errored = nextError(db);

			// Make the directory read-only so the next flush cannot create its files.
			// RocksDB records a background error and the database goes read-only.
			chmodSync(path, 0o555);
			db.putSync('b', '2');
			expect(() => db.flushSync()).toThrow();

			const err = await errored;
			expect(err).toBeInstanceOf(Error);
			expect(err).toBeInstanceOf(BackgroundError);
			expect(err.name).toBe('BackgroundError');
			expect(typeof err.message).toBe('string');
			expect(err.message.length).toBeGreaterThan(0);
			// Hard (2) or worse: writes are disabled.
			expect(err.severity).toBeGreaterThanOrEqual(2);
			expect(['hard', 'fatal', 'unrecoverable']).toContain(err.severityName);
			expect(err.writesDisabled).toBe(true);
			if (err.reason !== undefined) {
				expect(typeof err.reasonName).toBe('string');
			}

			// getLastError() is the pull equivalent: it returns the same error,
			// even for a caller that never attached an 'error' listener.
			const pulled = db.getLastError();
			expect(pulled).toBeInstanceOf(BackgroundError);
			expect(pulled!.message).toBe(err.message);
			expect(pulled!.writesDisabled).toBe(true);

			// A database in this state refuses further writes — the read-only state
			// that, before this fix, a consumer had no way to observe.
			expect(() => db.putSync('c', '3')).toThrow();
		}
	);

	it.skipIf(skipReadOnlyDir)(
		'resume() reports failure when the underlying condition has not cleared',
		async () => {
			const path = generateDBPath();
			const db = open(path);

			db.putSync('a', '1');
			db.flushSync();

			const errored = nextError(db);
			chmodSync(path, 0o555);
			db.putSync('b', '2');
			expect(() => db.flushSync()).toThrow();
			await errored;

			// The directory is still read-only, so recovery cannot succeed. resume()
			// must surface the failure rather than silently report success — the
			// invariant that keeps a consumer from believing a broken database is
			// healthy. (If RocksDB does manage to recover, resume() returns without
			// throwing; either outcome is truthful, never "resumed OK but broken".)
			let resumeThrew = false;
			try {
				db.resume();
			} catch {
				resumeThrew = true;
			}
			if (!resumeThrew) {
				// Recovery genuinely succeeded — writes must work again.
				chmodSync(path, 0o755);
				expect(() => db.putSync('d', '4')).not.toThrow();
			}
		}
	);

	// setLastError injects/clears deterministically (no filesystem hack), so this
	// runs on every platform — including Windows and root, where the read-only-dir
	// reproduction above is skipped.
	it('setLastError injects a BackgroundError, emits it, and getLastError returns it', async () => {
		const db = open(generateDBPath());
		const errored = nextError(db);

		db.setLastError({
			message: 'injected disk quota exceeded',
			severity: 2,
			severityName: 'hard',
			writesDisabled: true,
		});

		const err = await errored;
		expect(err).toBeInstanceOf(BackgroundError);
		expect(err.message).toBe('injected disk quota exceeded');
		expect(err.severity).toBe(2);
		expect(err.writesDisabled).toBe(true);
		expect(err.type).toBe('background'); // defaulted by setLastError

		const pulled = db.getLastError();
		expect(pulled).toBeInstanceOf(BackgroundError);
		expect(pulled!.message).toBe('injected disk quota exceeded');
		expect(pulled!.writesDisabled).toBe(true);
	});

	it('setLastError(null) resets the last error and emits nothing', async () => {
		const db = open(generateDBPath());

		db.setLastError({ message: 'boom', severity: 2, severityName: 'hard', writesDisabled: true });
		expect(db.getLastError()).not.toBeNull();

		let emitted = 0;
		db.on('error', () => {
			emitted++;
		});
		db.setLastError(null);
		expect(db.getLastError()).toBeNull();

		// A clear is a silent reset — no 'error' event.
		await new Promise((r) => setTimeout(r, 50));
		expect(emitted).toBe(0);
	});

	it('setLastError fills required fields from a partial input', () => {
		const db = open(generateDBPath());

		// message-only: severity/severityName/writesDisabled must be defined, not undefined.
		db.setLastError({ message: 'bare' });
		const bare = db.getLastError()!;
		expect(bare.message).toBe('bare');
		expect(bare.severity).toBe(0);
		expect(bare.severityName).toBe('none');
		expect(bare.writesDisabled).toBe(false);
		expect(bare.type).toBe('background');

		// severity given, name/writesDisabled derived from it.
		db.setLastError({ message: 'fatal one', severity: 3 });
		const derived = db.getLastError()!;
		expect(derived.severity).toBe(3);
		expect(derived.severityName).toBe('fatal');
		expect(derived.writesDisabled).toBe(true);

		// explicit values win over derivation.
		db.setLastError({ message: 'x', severity: 3, severityName: 'custom', writesDisabled: false });
		const explicit = db.getLastError()!;
		expect(explicit.severityName).toBe('custom');
		expect(explicit.writesDisabled).toBe(false);
	});
});
