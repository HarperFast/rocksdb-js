/**
 * A minimal, dependency-free streaming USTAR (POSIX tar) encoder.
 *
 * Built for streaming whole-database backups: the set of live RocksDB files
 * (SSTs, blobs, MANIFEST, CURRENT, OPTIONS, WAL) is serialized into a single
 * tar byte stream that can be piped to any destination and unpacked with
 * standard tools (`tar -xf`). The encoder never buffers a whole file — each
 * entry's payload is streamed through in chunks — so peak memory stays flat
 * regardless of database size.
 *
 * Backpressure is the caller's lever: every method `await`s the {@link TarSink},
 * so wiring the sink to a stream writer's `write()` naturally paces production
 * to the consumer.
 *
 * Only what a RocksDB backup needs is implemented: regular files. Paths longer
 * than USTAR's 100-byte `name` field are split across the `name` and `prefix`
 * fields (~255 bytes total), which covers `transaction_logs/<store>/<file>`
 * entries; GNU/pax long-name extensions are not implemented, so a path that
 * still doesn't fit throws rather than being silently truncated. File sizes
 * larger than the octal `size` field can hold (~8 GiB) fall back to the GNU
 * base-256 encoding.
 */

/** Size of a tar block. Every header and the archive length are multiples of this. */
const BLOCK_SIZE = 512;

/** Maximum bytes for the USTAR `name` field. */
const MAX_NAME_LENGTH = 100;

/** Maximum bytes for the USTAR `prefix` field (holds the leading directories). */
const MAX_PREFIX_LENGTH = 155;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * Destination for encoded bytes. Returning a promise applies backpressure: the
 * encoder will not produce the next chunk until it resolves. Chunks must be
 * consumed (or copied) before the returned promise resolves, since the encoder
 * may reuse or hand back borrowed buffers afterward.
 */
export type TarSink = (chunk: Uint8Array) => void | Promise<void>;

/**
 * Per-entry tar metadata. All fields are cosmetic for backup/restore (RocksDB
 * does not consult them); they exist so the archive is well-formed for external
 * tools.
 */
export type TarEntryOptions = {
	/** Unix file mode, masked to 12 bits. Default `0o644`. */
	mode?: number;
	/**
	 * Modification time in seconds since the epoch. Default `0` so archives are
	 * reproducible (byte-identical for identical inputs).
	 */
	mtime?: number;
};

/**
 * Writes a UTF-8 string into a fixed-width header field, left-justified and
 * NUL-padded (the block is already zero-filled, so nothing else is needed).
 * Throws if the encoded value does not fit — tar fields are silently lossy
 * otherwise, which corrupts the archive.
 */
function writeField(block: Uint8Array, value: string, offset: number, length: number): void {
	const bytes = textEncoder.encode(value);
	if (bytes.length > length) {
		throw new RangeError(`tar field value ${JSON.stringify(value)} exceeds ${length} bytes`);
	}
	block.set(bytes, offset);
}

/**
 * Writes a non-negative integer as a NUL-terminated, zero-padded octal string
 * into a `length`-byte field (so `length - 1` octal digits). Values too large
 * for octal fall back to the GNU base-256 extension, where the high bit of the
 * first byte is set and the magnitude is stored big-endian in the remainder.
 */
function writeOctal(block: Uint8Array, value: number, offset: number, length: number): void {
	const digits = length - 1;
	if (value > 8 ** digits - 1) {
		// GNU base-256: signal byte 0x80, then big-endian magnitude. JS safe
		// integers are 53 bits, well within the (length - 1) * 8 bits available.
		let v = value;
		for (let i = offset + length - 1; i > offset; i--) {
			block[i] = v & 0xff;
			v = Math.floor(v / 256);
		}
		block[offset] = 0x80;
		return;
	}
	const str = value.toString(8).padStart(digits, '0');
	for (let i = 0; i < digits; i++) {
		block[offset + i] = str.charCodeAt(i);
	}
	// block[offset + digits] stays 0 (NUL terminator).
}

/**
 * Computes and writes the USTAR header checksum: the unsigned sum of all 512
 * header bytes with the checksum field itself taken as ASCII spaces. Stored as
 * six octal digits, a NUL, then a space (the conventional encoding).
 */
function writeChecksum(block: Uint8Array): void {
	for (let i = 148; i < 156; i++) {
		block[i] = 0x20; // spaces during computation
	}
	let sum = 0;
	for (let i = 0; i < BLOCK_SIZE; i++) {
		sum += block[i];
	}
	const str = sum.toString(8).padStart(6, '0');
	for (let i = 0; i < 6; i++) {
		block[148 + i] = str.charCodeAt(i);
	}
	block[154] = 0; // NUL
	block[155] = 0x20; // space
}

/**
 * Splits a path into USTAR `name` (<=100 bytes) and `prefix` (<=155 bytes)
 * fields at a `/` boundary; a reader rejoins them as `prefix + "/" + name`.
 * Returns an empty prefix when the whole path fits in `name`. Throws when the
 * path cannot be represented — a trailing component over 100 bytes, or leading
 * directories that don't fit in 155. RocksDB and transaction-log filenames never
 * hit this; only a pathological log-store name would.
 */
function splitTarName(path: string): { name: string; prefix: string } {
	// Encode once and search the bytes: `/` is ASCII and UTF-8 never embeds an
	// ASCII byte inside a multi-byte sequence, so every 0x2f byte is a real
	// separator and both halves of a split there are valid UTF-8.
	const encoded = textEncoder.encode(path);
	if (encoded.length <= MAX_NAME_LENGTH) {
		return { name: path, prefix: '' };
	}
	let best = -1;
	for (let i = 0; i < encoded.length; i++) {
		if (encoded[i] !== 0x2f) {
			continue;
		}
		const prefixLength = i;
		const nameLength = encoded.length - i - 1;
		if (nameLength > 0 && nameLength <= MAX_NAME_LENGTH && prefixLength <= MAX_PREFIX_LENGTH) {
			// As the split moves right the prefix grows and the name shrinks, so the
			// last split satisfying both bounds packs the most into `prefix`.
			best = i;
		}
	}
	if (best === -1) {
		throw new RangeError(
			`tar entry name ${JSON.stringify(path)} does not fit the USTAR name (<=${MAX_NAME_LENGTH}) + prefix (<=${MAX_PREFIX_LENGTH}) fields`
		);
	}
	return {
		name: textDecoder.decode(encoded.subarray(best + 1)),
		prefix: textDecoder.decode(encoded.subarray(0, best)),
	};
}

/**
 * Builds a single 512-byte USTAR header block for a regular file. Long paths are
 * split across the `name` and `prefix` fields (see {@link splitTarName}).
 */
function buildHeader(name: string, size: number, mode: number, mtime: number): Uint8Array {
	const block = new Uint8Array(BLOCK_SIZE);
	const split = splitTarName(name);
	writeField(block, split.name, 0, MAX_NAME_LENGTH);
	writeOctal(block, mode & 0o7777, 100, 8); // mode
	writeOctal(block, 0, 108, 8); // uid
	writeOctal(block, 0, 116, 8); // gid
	writeOctal(block, size, 124, 12); // size
	writeOctal(block, Math.floor(mtime), 136, 12); // mtime (floored — octal field, no fraction)
	block[156] = 0x30; // typeflag '0' = regular file
	writeField(block, 'ustar', 257, 6); // magic "ustar\0"
	block[263] = 0x30; // version "00"
	block[264] = 0x30;
	if (split.prefix.length > 0) {
		writeField(block, split.prefix, 345, MAX_PREFIX_LENGTH); // prefix
	}
	writeChecksum(block);
	return block;
}

/**
 * Streaming USTAR encoder. Usage is a strict sequence per file —
 * {@link TarEncoder.addFile} once, then {@link TarEncoder.writeData} until the
 * declared size is reached — followed by a single {@link TarEncoder.finalize}:
 *
 * ```ts
 * const tar = new TarEncoder((chunk) => writer.write(chunk));
 * await tar.addFile('CURRENT', current.length);
 * await tar.writeData(current);
 * await tar.addFile('000123.sst', sstSize);
 * for await (const chunk of sstChunks) await tar.writeData(chunk);
 * await tar.finalize();
 * ```
 *
 * The size passed to `addFile` is authoritative: tar records it in the header
 * before any payload, so it must be known up front (it is — RocksDB's
 * `GetLiveFilesStorageInfo` returns each file's exact size). Writing more or
 * fewer bytes than declared throws.
 */
export class TarEncoder {
	#sink: TarSink;
	/** Payload bytes still expected for the current entry. */
	#remaining: number = 0;
	/** Zero-padding bytes owed after the current entry's payload reaches a block boundary. */
	#pad: number = 0;
	#finalized: boolean = false;

	constructor(sink: TarSink) {
		this.#sink = sink;
	}

	/**
	 * Emits the header for a new file entry. Must be preceded by completing any
	 * prior entry's payload. After this, exactly `size` bytes must be supplied
	 * via {@link TarEncoder.writeData} before the next `addFile` or `finalize`.
	 */
	async addFile(name: string, size: number, options?: TarEntryOptions): Promise<void> {
		if (this.#finalized) {
			throw new Error('TarEncoder already finalized');
		}
		if (this.#remaining !== 0) {
			throw new Error(`previous tar entry incomplete: ${this.#remaining} byte(s) unwritten`);
		}
		if (!Number.isInteger(size) || size < 0) {
			throw new RangeError('tar entry size must be a non-negative integer');
		}
		// Build the header first so an invalid/unsplittable name throws before any
		// encoder state is mutated.
		const header = buildHeader(name, size, options?.mode ?? 0o644, options?.mtime ?? 0);
		this.#remaining = size;
		this.#pad = (BLOCK_SIZE - (size % BLOCK_SIZE)) % BLOCK_SIZE;
		await this.#sink(header);
	}

	/**
	 * Streams a chunk of the current entry's payload. Chunks may be any size; the
	 * total across all `writeData` calls for an entry must equal the size passed
	 * to {@link TarEncoder.addFile}. Block padding is emitted automatically once
	 * the final byte of an entry is written.
	 */
	async writeData(chunk: Uint8Array): Promise<void> {
		if (this.#finalized) {
			throw new Error('TarEncoder already finalized');
		}
		if (chunk.length === 0) {
			return;
		}
		if (chunk.length > this.#remaining) {
			throw new RangeError(
				`tar data overruns entry size by ${chunk.length - this.#remaining} byte(s)`
			);
		}
		this.#remaining -= chunk.length;
		await this.#sink(chunk);
		if (this.#remaining === 0 && this.#pad > 0) {
			const pad = this.#pad;
			this.#pad = 0;
			await this.#sink(new Uint8Array(pad));
		}
	}

	/**
	 * Convenience for an entry whose contents are already fully in memory (e.g.
	 * the small `CURRENT`/`OPTIONS` files RocksDB returns inline).
	 */
	async addFileData(name: string, data: Uint8Array, options?: TarEntryOptions): Promise<void> {
		await this.addFile(name, data.length, options);
		await this.writeData(data);
	}

	/**
	 * Writes the two zero-filled blocks that mark end-of-archive. The encoder is
	 * unusable afterward. Throws if the current entry is not fully written, so a
	 * truncated stream cannot be silently finalized into a valid-looking archive.
	 */
	async finalize(): Promise<void> {
		if (this.#finalized) {
			throw new Error('TarEncoder already finalized');
		}
		if (this.#remaining !== 0) {
			throw new Error(
				`cannot finalize: current tar entry has ${this.#remaining} byte(s) unwritten`
			);
		}
		this.#finalized = true;
		await this.#sink(new Uint8Array(BLOCK_SIZE * 2));
	}
}
