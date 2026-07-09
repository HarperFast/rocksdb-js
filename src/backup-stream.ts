import type { NativeDatabase } from './load-binding.js';
import { TarEncoder } from './tar.js';

/**
 * Options for streaming a backup via `db.backup(stream)`.
 */
export interface BackupStreamOptions {
	/**
	 * Flush the memtable before streaming. Defaults to `true` when the database
	 * was opened with `disableWAL` (otherwise unflushed data would be missing
	 * from the stream), and `false` otherwise — matching `db.backup(dir)`.
	 */
	flushBeforeBackup?: boolean;

	/**
	 * Gzip-compress the archive before writing it to the stream, producing a
	 * `.tar.gz` stream instead of a plain `.tar`. Defaults to `false`.
	 *
	 * Compression runs in the runtime's `CompressionStream`, downstream of the tar
	 * encoder, so end-to-end backpressure is preserved. RocksDB SST files are
	 * usually already block-compressed, so the additional savings vary with the
	 * data and the configured compression.
	 */
	gzip?: boolean;

	/**
	 * Also stream the transaction log store as `transaction_logs/<store>/…`
	 * entries (each `*.txnlog` and `txn.state` file) after the database files.
	 * Defaults to `false`. Extraction reconstructs them next to the database, and
	 * their mtimes are preserved so the store's age-based rotation/retention stays
	 * correct.
	 */
	transactionLogs?: boolean;
}

/** Native event discriminator: a new file header (vs. a payload chunk). */
const EVENT_FILE = 0;

/**
 * Streams a consistent snapshot of the database to a `WritableStream` as a tar
 * archive, with no intermediate copy on disk. The set of live RocksDB files is
 * enumerated and read natively; each file header and payload chunk is framed
 * into the {@link TarEncoder} here in JS and written to `stream`.
 *
 * Backpressure flows end to end: the encoder awaits each `writer.write()`, and
 * the native producer awaits each `emit()` before reading the next chunk, so a
 * slow consumer paces the entire pipeline rather than buffering in memory.
 *
 * The resulting archive can be unpacked with any tar tool (`tar -xf`) into a
 * directory that opens directly as a RocksDB database.
 */
export async function backupToStream(
	db: NativeDatabase,
	stream: WritableStream<Uint8Array>,
	options?: BackupStreamOptions
): Promise<void> {
	// With `gzip`, the tar bytes are written into a CompressionStream whose
	// compressed output is piped to the caller's stream. `pipeTo` propagates
	// backpressure and, on failure, aborts the destination — so the end-to-end
	// pacing and error handling carry through the compressor unchanged.
	let tarDestination: WritableStream<Uint8Array>;
	let piped: Promise<void> | undefined;
	if (options?.gzip) {
		const gzip = new CompressionStream('gzip');
		piped = gzip.readable.pipeTo(stream);
		tarDestination = gzip.writable as WritableStream<Uint8Array>;
	} else {
		tarDestination = stream;
	}

	const writer = tarDestination.getWriter();
	const tar = new TarEncoder((chunk) => writer.write(chunk));

	// Native invokes `emit` once per file header and once per payload chunk, and
	// awaits the returned promise before producing the next event. A rejection
	// here (e.g. the consumer errored) aborts the native stream.
	const emit = async (
		kind: number,
		data: string | Uint8Array,
		size: number,
		mtime: number
	): Promise<void> => {
		if (kind === EVENT_FILE) {
			await tar.addFile(data as string, size, { mtime });
		} else {
			await tar.writeData(data as Uint8Array);
		}
	};

	try {
		await new Promise<void>((resolve, reject) => {
			db.backupStream(resolve, reject, emit, options);
		});
		await tar.finalize();
		await writer.close();
		// Closing the tar destination flushes and closes the compressor, which
		// closes its readable end and lets the pipe finish writing to the caller.
		if (piped) {
			await piped;
		}
	} catch (err) {
		// Surface the failure to the consumer and release the writer lock. The
		// writer may already be errored (consumer-side failure), so ignore abort
		// rejections and rethrow the original cause.
		await writer.abort(err).catch(() => {});
		// Aborting the compressor rejects the pipe; settle it so the rejection is
		// observed rather than surfacing as an unhandled rejection.
		if (piped) {
			await piped.catch(() => {});
		}
		throw err;
	}
}
