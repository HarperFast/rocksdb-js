import { RocksDatabase } from '../src/index.js';
import { dbRunner, generateDBPath } from './lib/util.js';
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { gunzipSync } from 'node:zlib';
import * as tar from 'tar';
import { afterEach, describe, expect, it } from 'vitest';

const tempPaths: string[] = [];

function tempPath(): string {
	const p = generateDBPath();
	tempPaths.push(p);
	return p;
}

async function writeAll(db: RocksDatabase, count: number, prefix = 'value'): Promise<void> {
	for (let i = 0; i < count; ++i) {
		await db.put(`key-${i}`, `${prefix}-${i}`);
	}
	await db.flush();
}

/** Runs a stream backup, collecting the archive bytes into a Buffer. */
async function streamBackupToBuffer(
	db: RocksDatabase,
	beforeWrite?: () => Promise<void>
): Promise<Buffer> {
	const chunks: Buffer[] = [];
	const stream = new WritableStream<Uint8Array>({
		async write(chunk) {
			if (beforeWrite) {
				await beforeWrite();
			}
			chunks.push(Buffer.from(chunk));
		},
	});
	await db.backup(stream);
	return Buffer.concat(chunks);
}

/** Extracts a tar archive into a fresh directory and opens it as a database. */
function extractAndOpen(archive: Buffer): RocksDatabase {
	const tarPath = `${tempPath()}.tar`;
	writeFileSync(tarPath, archive);

	const dir = tempPath();
	mkdirSync(dir, { recursive: true });
	tar.extract({ file: tarPath, cwd: dir, sync: true });

	const db = new RocksDatabase(dir);
	db.open();
	return db;
}

describe('Streaming backups', () => {
	afterEach(() => {
		for (const p of tempPaths) {
			rmSync(p, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
			rmSync(`${p}.tar`, { force: true, maxRetries: 3, retryDelay: 500 });
		}
		tempPaths.length = 0;
	});

	it('streams a backup that extracts and reopens with all data intact', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 200);

			const archive = await streamBackupToBuffer(db);
			expect(archive.length).toBeGreaterThan(0);

			const restored = extractAndOpen(archive);
			try {
				for (let i = 0; i < 200; ++i) {
					expect(await restored.get(`key-${i}`)).toBe(`value-${i}`);
				}
			} finally {
				restored.close();
			}
		}));

	it('produces a valid tar archive containing the live DB files', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 50);
			const archive = await streamBackupToBuffer(db);

			const names: string[] = [];
			await new Promise<void>((resolve, reject) => {
				const parser = new tar.Parser();
				parser.on('entry', (entry) => {
					names.push(entry.path);
					entry.resume(); // drain
				});
				parser.on('end', () => resolve());
				parser.on('error', reject);
				parser.end(archive);
			});

			// A live RocksDB snapshot always has CURRENT and a MANIFEST.
			expect(names).toContain('CURRENT');
			expect(names.some((n) => n.startsWith('MANIFEST-'))).toBe(true);
		}));

	it('honors backpressure from a slow consumer and still round-trips', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 100);

			// Delay every write so the native producer must block on each ack.
			const archive = await streamBackupToBuffer(db, () => new Promise((r) => setTimeout(r, 1)));

			const restored = extractAndOpen(archive);
			try {
				expect(await restored.get('key-0')).toBe('value-0');
				expect(await restored.get('key-99')).toBe('value-99');
			} finally {
				restored.close();
			}
		}));

	it('gzips the archive when { gzip: true } and round-trips after gunzip', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 200);

			const chunks: Buffer[] = [];
			const stream = new WritableStream<Uint8Array>({
				write(chunk) {
					chunks.push(Buffer.from(chunk));
				},
			});
			await db.backup(stream, { gzip: true });

			const archive = Buffer.concat(chunks);
			// gzip magic bytes.
			expect(archive[0]).toBe(0x1f);
			expect(archive[1]).toBe(0x8b);

			// Gunzip yields the tar, which extracts and reopens with data intact.
			const restored = extractAndOpen(gunzipSync(archive));
			try {
				expect(await restored.get('key-0')).toBe('value-0');
				expect(await restored.get('key-199')).toBe('value-199');
			} finally {
				restored.close();
			}
		}));

	it('rejects when the consumer stream errors', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 20);

			const stream = new WritableStream<Uint8Array>({
				write() {
					throw new Error('consumer boom');
				},
			});

			await expect(db.backup(stream)).rejects.toThrow(/consumer boom/);
		}));

	it('still routes a string argument to a directory backup', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 5);
			const id = await db.backup(tempPath());
			expect(id).toBe(1); // directory backups return a numeric backup id
		}));

	it('settles without hanging when the database is closed mid-stream', () =>
		dbRunner({ skipOpen: true }, async ({ db }) => {
			db.open();
			await writeAll(db, 200);

			// A slow consumer keeps the stream in flight so close() races an active
			// chunk. close() blocks the JS thread that would deliver the next ack, so
			// the worker must notice cancellation and abandon rather than deadlock.
			const stream = new WritableStream<Uint8Array>({
				async write() {
					await new Promise((r) => setTimeout(r, 5));
				},
			});
			const settled = db.backup(stream).then(
				() => 'settled',
				() => 'settled'
			);

			await new Promise((r) => setTimeout(r, 50)); // let the stream get going
			db.close();

			// If the worker deadlocked against close(), this would time out.
			await expect(settled).resolves.toBe('settled');
		}));

	it('throws on destroy() during a stream and the stream still settles', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 200);

			const stream = new WritableStream<Uint8Array>({
				async write() {
					await new Promise((r) => setTimeout(r, 5));
				},
			});
			const settled = db.backup(stream).then(
				() => 'settled',
				() => 'settled'
			);

			await new Promise((r) => setTimeout(r, 50));
			// The in-flight stream pins the descriptor, so destroy() refuses to tear
			// the database down mid-stream and throws instead.
			expect(() => db.destroy()).toThrow();

			await expect(settled).resolves.toBe('settled');
		}));
});
