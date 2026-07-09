import { TarEncoder, type TarSink } from '../src/tar.js';
import { Parser } from 'tar';
import { describe, expect, it } from 'vitest';

/** Collects everything written to a sink into a single Buffer. */
function collector(): { sink: TarSink; bytes: () => Buffer } {
	const chunks: Buffer[] = [];
	return {
		// Copy: the encoder's contract allows it to reuse/borrow buffers once the
		// sink resolves, so a sink that retains them must take its own copy.
		sink: (chunk) => {
			chunks.push(Buffer.from(chunk));
		},
		bytes: () => Buffer.concat(chunks),
	};
}

type ParsedEntry = { path: string; size: number; data: Buffer };

/** Unpacks a tar archive with the real `tar` package and returns its entries. */
function parseTar(archive: Buffer): Promise<ParsedEntry[]> {
	return new Promise((resolve, reject) => {
		const entries: ParsedEntry[] = [];
		const parser = new Parser();
		parser.on('entry', (entry) => {
			const parts: Buffer[] = [];
			entry.on('data', (c: Buffer) => parts.push(c));
			entry.on('end', () => {
				entries.push({ path: entry.path, size: entry.size ?? 0, data: Buffer.concat(parts) });
			});
		});
		parser.on('end', () => resolve(entries));
		parser.on('error', reject);
		parser.end(archive);
	});
}

describe('TarEncoder', () => {
	it('round-trips multiple files through the real tar parser', async () => {
		const { sink, bytes } = collector();
		const tar = new TarEncoder(sink);

		const files: Record<string, Buffer> = {
			CURRENT: Buffer.from('MANIFEST-000007\n'),
			'MANIFEST-000007': Buffer.from('a'.repeat(300)),
			'000123.sst': Buffer.from('b'.repeat(512)), // exact block boundary
			'000124.sst': Buffer.from('c'.repeat(513)), // needs 511 bytes of padding
			empty: Buffer.alloc(0),
		};

		for (const [name, data] of Object.entries(files)) {
			await tar.addFileData(name, data);
		}
		await tar.finalize();

		const entries = await parseTar(bytes());
		expect(entries.map((e) => e.path).sort()).toEqual(Object.keys(files).sort());
		for (const entry of entries) {
			expect(entry.size).toBe(files[entry.path].length);
			expect(entry.data).toEqual(files[entry.path]);
		}
	});

	it('reassembles a file streamed in many small chunks', async () => {
		const { sink, bytes } = collector();
		const tar = new TarEncoder(sink);

		const payload = Buffer.from(Array.from({ length: 5000 }, (_, i) => i % 256));
		await tar.addFile('big.sst', payload.length);
		for (let i = 0; i < payload.length; i += 7) {
			await tar.writeData(payload.subarray(i, i + 7));
		}
		await tar.finalize();

		const [entry] = await parseTar(bytes());
		expect(entry.path).toBe('big.sst');
		expect(entry.data).toEqual(payload);
	});

	it('produces a 512-aligned archive terminated by two zero blocks', async () => {
		const { sink, bytes } = collector();
		const tar = new TarEncoder(sink);
		await tar.addFileData('CURRENT', Buffer.from('x'));
		await tar.finalize();

		const archive = bytes();
		expect(archive.length % 512).toBe(0);
		// USTAR magic in the first header.
		expect(archive.subarray(257, 262).toString('ascii')).toBe('ustar');
		// Final 1024 bytes are the end-of-archive marker.
		expect(archive.subarray(archive.length - 1024).every((b) => b === 0)).toBe(true);
	});

	it('applies backpressure by awaiting an async sink in order', async () => {
		const order: string[] = [];
		const chunks: Buffer[] = [];
		let inFlight = 0;
		let n = 0;
		const sink: TarSink = (chunk) => {
			const id = n++;
			chunks.push(Buffer.from(chunk));
			// If await were skipped, a second call would start before this resolves.
			expect(inFlight).toBe(0);
			inFlight++;
			order.push(`start:${id}`);
			return new Promise((r) =>
				setTimeout(() => {
					inFlight--;
					order.push(`end:${id}`);
					r();
				}, 1)
			);
		};
		const tar = new TarEncoder(sink);
		await tar.addFileData('a', Buffer.from('hello'));
		await tar.finalize();

		// Calls are strictly serialized: start:0, end:0, start:1, end:1, ...
		for (let i = 0; i < n; i++) {
			expect(order.indexOf(`end:${i}`)).toBe(order.indexOf(`start:${i}`) + 1);
		}
		// Round-trips despite the async sink.
		const [entry] = await parseTar(Buffer.concat(chunks));
		expect(entry.path).toBe('a');
		expect(entry.data).toEqual(Buffer.from('hello'));
	});

	it('encodes oversized file lengths with the GNU base-256 extension', async () => {
		// Capture just the header; we never stream the (huge) payload.
		let header: Uint8Array | undefined;
		const tar = new TarEncoder((chunk) => {
			header ??= Buffer.from(chunk);
		});
		await tar.addFile('huge.blob', 2 ** 34); // 16 GiB, overflows the octal size field

		// High bit of the size field's first byte signals base-256.
		expect(header![124] & 0x80).toBe(0x80);
	});

	it('splits a long path across the USTAR name and prefix fields', async () => {
		const { sink, bytes } = collector();
		const tar = new TarEncoder(sink);
		// > 100 bytes total, with a '/' boundary that lets it split.
		const longPath = `transaction_logs/${'s'.repeat(120)}/3.txnlog`;
		await tar.addFileData(longPath, Buffer.from('log-bytes'));
		await tar.finalize();

		const [entry] = await parseTar(bytes());
		expect(entry.path).toBe(longPath); // reader rejoins prefix + '/' + name
		expect(entry.data).toEqual(Buffer.from('log-bytes'));
	});

	describe('misuse is rejected', () => {
		it('throws when more data than declared is written', async () => {
			const { sink } = collector();
			const tar = new TarEncoder(sink);
			await tar.addFile('a', 4);
			await expect(tar.writeData(Buffer.from('toolong'))).rejects.toThrow(/overruns/);
		});

		it('throws when starting a new entry before the previous is complete', async () => {
			const { sink } = collector();
			const tar = new TarEncoder(sink);
			await tar.addFile('a', 10);
			await tar.writeData(Buffer.from('partial'));
			await expect(tar.addFile('b', 1)).rejects.toThrow(/incomplete/);
		});

		it('throws when finalizing with an incomplete entry', async () => {
			const { sink } = collector();
			const tar = new TarEncoder(sink);
			await tar.addFile('a', 10);
			await expect(tar.finalize()).rejects.toThrow(/unwritten/);
		});

		it('throws when a single path component exceeds the name field', async () => {
			const { sink } = collector();
			const tar = new TarEncoder(sink);
			// No '/' to split on and >100 bytes: cannot be represented.
			await expect(tar.addFile('n'.repeat(101), 0)).rejects.toThrow(/does not fit/);
		});

		it('throws on use after finalize', async () => {
			const { sink } = collector();
			const tar = new TarEncoder(sink);
			await tar.finalize();
			await expect(tar.addFile('a', 0)).rejects.toThrow(/finalized/);
		});
	});
});
