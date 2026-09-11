import type { RocksDatabase } from '../src/index.ts';
import { dbRunner } from './lib/util.ts';
import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';

const COUNT = 20_000;
const key = (i: number) => `k${String(i).padStart(6, '0')}`;

/**
 * Writes enough keys to fill many data blocks, flushes them into a single SST,
 * closes the database, and flips 128 bytes starting at `offset` (a fraction of
 * the file size). The database is reopened before returning.
 */
function writeAndCorrupt(db: RocksDatabase, dbPath: string, offset: number): void {
	db.open();
	for (let i = 0; i < COUNT; i++) {
		db.putSync(key(i), 'x'.repeat(40));
	}
	db.flushSync();
	db.close();

	const ssts = readdirSync(dbPath).filter((file) => file.endsWith('.sst'));
	expect(ssts).toHaveLength(1);
	const path = join(dbPath, ssts[0]);
	const bytes = readFileSync(path);
	const start = Math.max(64, Math.floor(bytes.length * offset));
	for (let i = start; i < start + 128; i++) {
		bytes[i] ^= 0xff;
	}
	writeFileSync(path, bytes);

	db.open();
}

function expectCorruption(err: unknown): void {
	expect(err).toBeInstanceOf(Error);
	expect((err as Error & { code?: string }).code).toBe('ERR_CORRUPTION');
	expect((err as Error).message).toMatch(/Iterator failed: Corruption/);
}

describe('Iterator status', () => {
	it('should throw when a synchronous range reaches a corrupted block', () =>
		dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
			writeAndCorrupt(db, dbPath, 0);
			let seen = 0;
			let error: unknown;
			try {
				for (const _ of db.getRange()) {
					seen++;
				}
			} catch (err) {
				error = err;
			}
			expect(
				error,
				`the range ended after ${seen} of ${COUNT} entries without an error`
			).toBeDefined();
			expectCorruption(error);
		}));

	it('should reject when an asynchronous range reaches a corrupted block', () =>
		dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
			writeAndCorrupt(db, dbPath, 0);
			let seen = 0;
			let error: unknown;
			try {
				for await (const _ of db.getRange()) {
					seen++;
				}
			} catch (err) {
				error = err;
			}
			expect(
				error,
				`the range ended after ${seen} of ${COUNT} entries without an error`
			).toBeDefined();
			expectCorruption(error);
		}));

	it('should yield the entries before a corrupted block, then throw', () =>
		dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
			writeAndCorrupt(db, dbPath, 0.5);
			let seen = 0;
			let error: unknown;
			try {
				for (const _ of db.getRange()) {
					seen++;
				}
			} catch (err) {
				error = err;
			}
			expect(seen).toBeGreaterThan(0);
			expect(seen).toBeLessThan(COUNT);
			expectCorruption(error);
		}));

	it('should release the native iterator when a step fails', () =>
		dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
			writeAndCorrupt(db, dbPath, 0);
			const iterator = db.getRange()[Symbol.iterator]() as Iterator<unknown> & {
				iterator: { return?: () => void };
			};
			const release = vi.spyOn(iterator.iterator, 'return');
			expect(() => iterator.next()).toThrow(/Iterator failed: Corruption/);
			expect(release).toHaveBeenCalledOnce();
		}));
});
