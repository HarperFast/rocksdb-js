#include "core/platform.h"
#include "database/backup_transaction_logs.h"
#include "database/database.h"
#include "database/db_descriptor.h"
#include "database/db_handle.h"
#include "database/db_registry.h"
#include "napi/async.h"
#include "napi/helpers.h"
#include "napi/macros.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace rocksdb_js {

namespace {

// Size of each payload chunk read from a live file and handed to JS. Large
// enough to amortize the per-chunk N-API round trip and backpressure ack, small
// enough that a slow consumer does not buffer an unbounded amount.
constexpr size_t kChunkSize = 1 << 20; // 1 MiB

// Discriminator for the single `emit(kind, a, b)` JS callback:
//   File:  emit(0, name: string, size: number) -> tar.addFile(name, size)
//   Chunk: emit(1, data: Buffer, undefined)     -> tar.writeData(data)
constexpr uint32_t kEventFile = 0;
constexpr uint32_t kEventChunk = 1;

/**
 * A single event handed to the JS thread via the threadsafe function. For chunk
 * events `data`/`size` BORROW the worker's reusable read buffer (or RocksDB's
 * inline `replacement_contents`); this is safe only because the worker blocks
 * on the ack condvar until the trampoline has copied the bytes into a JS Buffer
 * (see `AsyncBackupStreamState::emit`).
 */
struct StreamEvent {
	uint32_t kind;
	std::string name;            // File: tar entry name
	uint64_t fileSize = 0;       // File: declared size
	int64_t mtime = 0;           // File: mtime (epoch seconds) for the tar header
	const uint8_t* data = nullptr; // Chunk: borrowed payload pointer
	size_t size = 0;             // Chunk: borrowed payload length
};

/** Best-effort coercion of a JS value (typically an Error) to a UTF-8 string. */
std::string readJsString(napi_env env, napi_value value) {
	napi_value str;
	if (::napi_coerce_to_string(env, value, &str) != napi_ok) {
		return "";
	}
	size_t len = 0;
	if (::napi_get_value_string_utf8(env, str, nullptr, 0, &len) != napi_ok) {
		return "";
	}
	std::string out(len, '\0');
	if (::napi_get_value_string_utf8(env, str, out.data(), len + 1, nullptr) != napi_ok) {
		return "";
	}
	return out;
}

} // namespace

/**
 * State for the `Database::BackupStream` async work. Streams the live files that
 * make up the database to a JS callback (`emit`), one chunk at a time, with a
 * producer/consumer backpressure handshake: the worker thread emits one event
 * then blocks until the JS `emit` promise settles and a continuation flips
 * `ackReady`. Lifetime handling mirrors `AsyncCheckpointState` — the descriptor
 * is pinned and registered in `operationsInFlight` so a concurrent `close()` /
 * teardown waits for the (potentially long-running) stream to finish.
 */
struct AsyncBackupStreamState final : BaseAsyncState<std::shared_ptr<DBHandle>> {
	std::shared_ptr<DBDescriptor> descriptor;
	napi_threadsafe_function tsfn = nullptr;
	bool flushBeforeBackup = false;
	// When true, append the transaction log snapshot as tar entries under
	// `transaction_logs/<store>/` after the DB's live files.
	bool backupTransactionLogs = false;

	// Backpressure handshake between the worker thread (producer) and JS thread
	// (consumer). Exactly one event is in flight at a time.
	std::mutex ackMutex;
	std::condition_variable ackCv;
	bool ackReady = false;
	bool aborted = false;
	std::string abortMessage;

	// Keeps `state` alive until BOTH the async-work complete callback has run AND
	// any outstanding emit-promise continuation has fired. On a teardown abort the
	// worker abandons its wait while one continuation is still pending; that
	// continuation fires later (once the JS thread tearing the DB down unblocks),
	// so its `signalAck` must land on live memory. The complete callback owns the
	// initial ref.
	std::atomic<int> refCount{ 1 };

	void acquire() {
		this->refCount.fetch_add(1, std::memory_order_relaxed);
	}

	void release() {
		if (this->refCount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
			delete this;
		}
	}

	AsyncBackupStreamState(
		napi_env env,
		std::shared_ptr<DBHandle> handle,
		std::shared_ptr<DBDescriptor> descriptor,
		bool flushBeforeBackup
	) :
		BaseAsyncState<std::shared_ptr<DBHandle>>(env, handle),
		descriptor(std::move(descriptor)),
		flushBeforeBackup(flushBeforeBackup) {}

	// Our descriptor ref can be the reason a concurrent close skipped its
	// registry purge (use_count() > 1), so on release we must retry the purge or
	// the registry entry — and the open RocksDB — would linger forever.
	~AsyncBackupStreamState() override {
		if (this->descriptor) {
			std::string path = this->descriptor->path;
			bool readOnly = this->descriptor->readOnly;
			this->descriptor.reset();
			DBRegistry::PurgeIfUnreferenced(path, readOnly);
		}
	}

	/**
	 * Called on the JS thread (promise continuation / failure path) to wake the
	 * blocked worker. `ok == false` requests the worker to abort the stream.
	 */
	void signalAck(bool ok, std::string message) {
		{
			std::lock_guard<std::mutex> lock(this->ackMutex);
			if (!ok) {
				this->aborted = true;
				this->abortMessage = std::move(message);
			}
			this->ackReady = true;
		}
		this->ackCv.notify_one();
	}

	/**
	 * Called on the worker thread. Hands one event to JS and blocks until the JS
	 * side acks (or aborts). Returns false if the stream should stop.
	 */
	bool emit(std::unique_ptr<StreamEvent> ev) {
		{
			std::lock_guard<std::mutex> lock(this->ackMutex);
			this->ackReady = false;
		}

		// On success the trampoline takes ownership of the event and deletes it.
		napi_status status = ::napi_call_threadsafe_function(this->tsfn, ev.get(), napi_tsfn_blocking);
		if (status != napi_ok) {
			// The trampoline will not run; reclaim the event (unique_ptr) and stop.
			std::lock_guard<std::mutex> lock(this->ackMutex);
			this->aborted = true;
			if (this->abortMessage.empty()) {
				this->abortMessage = "Backup stream callback could not be invoked";
			}
			return false;
		}
		ev.release();

		std::unique_lock<std::mutex> lock(this->ackMutex);
		// Wait for the ack, but wake periodically to check for teardown. A closing
		// database blocks the JS thread (finishClose() waits on operationsInFlight;
		// close() on the async-work tracker), and that same JS thread is what would
		// deliver our ack — so an unconditional wait would deadlock. Polling
		// isClosing()/isCancelled() lets the worker abandon and release its DB pin
		// so teardown can proceed; the in-flight continuation fires harmlessly later.
		while (!this->ackReady) {
			if (this->ackCv.wait_for(lock, std::chrono::milliseconds(50), [this] { return this->ackReady; })) {
				break;
			}
			if (this->descriptor->isClosing() || (this->handle && this->handle->isCancelled())) {
				this->aborted = true;
				if (this->abortMessage.empty()) {
					this->abortMessage = "Database closing during backup stream operation";
				}
				return false;
			}
		}
		return !this->aborted;
	}
};

namespace {

/** Promise fulfillment continuation: the chunk was written, wake the worker. */
napi_value onAckResolved(napi_env env, napi_callback_info info) {
	void* data = nullptr;
	::napi_get_cb_info(env, info, nullptr, nullptr, nullptr, &data);
	if (auto* state = static_cast<AsyncBackupStreamState*>(data)) {
		state->signalAck(true, "");
		state->release();
	}
	return nullptr;
}

/** Promise rejection continuation: the consumer errored, abort the stream. */
napi_value onAckRejected(napi_env env, napi_callback_info info) {
	size_t argc = 1;
	napi_value argv[1];
	void* data = nullptr;
	::napi_get_cb_info(env, info, &argc, argv, nullptr, &data);
	std::string message = "Backup stream consumer error";
	if (argc >= 1) {
		std::string coerced = readJsString(env, argv[0]);
		if (!coerced.empty()) {
			message = std::move(coerced);
		}
	}
	if (auto* state = static_cast<AsyncBackupStreamState*>(data)) {
		state->signalAck(false, message);
		state->release();
	}
	return nullptr;
}

/**
 * Threadsafe-function trampoline (runs on the JS thread). Calls `emit(kind, a,
 * b)`, then attaches `then(onAckResolved, onAckRejected)` to the returned
 * promise so the worker is woken once the write settles. Every failure path
 * MUST call `signalAck`, or the blocked worker would hang forever.
 */
void emitTrampoline(napi_env env, napi_value emitFn, void* context, void* data) {
	auto* state = static_cast<AsyncBackupStreamState*>(context);
	std::unique_ptr<StreamEvent> ev(static_cast<StreamEvent*>(data));

	if (env == nullptr || emitFn == nullptr || state == nullptr) {
		if (state != nullptr) {
			state->signalAck(false, "Backup stream environment unavailable");
		}
		return;
	}

	napi_value undefined;
	napi_value argv[4];
	if (::napi_get_undefined(env, &undefined) != napi_ok ||
		::napi_create_uint32(env, ev->kind, &argv[0]) != napi_ok) {
		state->signalAck(false, "Backup stream failed to build event");
		return;
	}
	argv[3] = undefined; // File events overwrite with the mtime; chunks leave it undefined.

	if (ev->kind == kEventFile) {
		// File size/mtime may exceed uint32; pass as doubles (JS number; exact to 2^53).
		if (::napi_create_string_utf8(env, ev->name.c_str(), ev->name.size(), &argv[1]) != napi_ok ||
			::napi_create_double(env, static_cast<double>(ev->fileSize), &argv[2]) != napi_ok ||
			::napi_create_double(env, static_cast<double>(ev->mtime), &argv[3]) != napi_ok) {
			state->signalAck(false, "Backup stream failed to build file event");
			return;
		}
	} else {
		// A chunk's `data` BORROWS the worker's read buffer (or RocksDB's inline
		// `replacement_contents`), both of which live only until the worker returns
		// from its read loop. Normally the worker blocks until we ack, so the borrow
		// is valid — but on a teardown abort the worker abandons its wait and frees
		// that memory while this event may still be queued. Copy under `ackMutex`
		// and bail if the worker has already aborted: the abort is published under
		// the same lock before the worker returns, so observing `!aborted` here
		// guarantees the borrowed bytes are still alive for the copy.
		napi_status copyStatus;
		bool abortedNow;
		{
			std::lock_guard<std::mutex> lock(state->ackMutex);
			abortedNow = state->aborted;
			if (!abortedNow) {
				void* copied = nullptr;
				copyStatus = ::napi_create_buffer_copy(env, ev->size, ev->data, &copied, &argv[1]);
			}
		}
		if (abortedNow) {
			// Worker is gone; its buffer may be freed. Drop the event untouched. No
			// continuation is attached, so the complete callback frees `state`.
			return;
		}
		if (copyStatus != napi_ok || ::napi_get_undefined(env, &argv[2]) != napi_ok) {
			state->signalAck(false, "Backup stream failed to allocate chunk buffer");
			return;
		}
	}

	napi_value promise;
	if (::napi_call_function(env, undefined, emitFn, 4, argv, &promise) != napi_ok) {
		napi_value pending;
		::napi_get_and_clear_last_exception(env, &pending);
		state->signalAck(false, "Backup stream emit callback threw");
		return;
	}

	// promise.then(onAckResolved, onAckRejected); state flows in via the fn data.
	napi_value thenFn;
	napi_value onResolved;
	napi_value onRejected;
	if (::napi_get_named_property(env, promise, "then", &thenFn) != napi_ok ||
		::napi_create_function(env, "onAck", NAPI_AUTO_LENGTH, onAckResolved, state, &onResolved) != napi_ok ||
		::napi_create_function(env, "onAckError", NAPI_AUTO_LENGTH, onAckRejected, state, &onRejected) != napi_ok) {
		state->signalAck(false, "Backup stream failed to attach ack");
		return;
	}

	napi_value cbs[2] = { onResolved, onRejected };
	napi_value thenResult;
	// The continuation that fires owns a refcount so `state` survives until it
	// runs, even if the worker abandoned its wait and the complete callback has
	// already released its own ref. Balanced by the release in
	// onAckResolved/onAckRejected (or below if attaching fails).
	state->acquire();
	if (::napi_call_function(env, promise, thenFn, 2, cbs, &thenResult) != napi_ok) {
		napi_value pending;
		::napi_get_and_clear_last_exception(env, &pending);
		state->signalAck(false, "Backup stream emit did not return a promise");
		state->release();
		return;
	}
	// Ack will arrive via onAckResolved / onAckRejected.
}

/**
 * tsfn thread-finalize callback. Node-API runs this on the JS thread only after
 * the tsfn has been released AND every queued trampoline invocation has drained,
 * so it is the safe point to drop the tsfn's reference to `state`. This is what
 * keeps `state` alive for a trampoline that was still queued when the worker
 * abandoned its wait on teardown — without it, the complete callback could free
 * `state` first and the late trampoline would lock a destroyed `ackMutex`.
 */
void tsfnFinalize(napi_env /*env*/, void* data, void* /*hint*/) {
	if (auto* state = static_cast<AsyncBackupStreamState*>(data)) {
		state->release();
	}
}

/** RAII: re-enable file deletions on every exit path. Best-effort. */
struct FileDeletionGuard {
	rocksdb::DB* db;
	bool active;
	~FileDeletionGuard() {
		if (this->active) {
			this->db->EnableFileDeletions();
		}
	}
};

/** Epoch seconds for a filesystem mtime, for the tar header. Delegates to the
 *  shared, platform-correct conversion rather than re-deriving the clock offset. */
int64_t fileTimeToEpochSeconds(std::filesystem::file_time_type ft) {
	return std::chrono::duration_cast<std::chrono::seconds>(
		convertFileTimeToSystemTime(ft).time_since_epoch()
	).count();
}

/**
 * Streams `[0, byteLimit)` of the file at `path` as chunk events. The caller
 * must have already emitted the matching file-header event. Returns non-OK on a
 * read error or a consumer abort.
 */
rocksdb::Status streamFilePrefix(
	AsyncBackupStreamState* state,
	rocksdb::Env* env,
	const std::string& path,
	uint64_t byteLimit,
	std::vector<char>& buffer
) {
	std::unique_ptr<rocksdb::SequentialFile> seqFile;
	rocksdb::Status s = env->NewSequentialFile(path, &seqFile, rocksdb::EnvOptions());
	if (!s.ok()) {
		return s;
	}
	uint64_t remaining = byteLimit;
	while (remaining > 0) {
		size_t toRead = static_cast<size_t>(std::min<uint64_t>(remaining, kChunkSize));
		rocksdb::Slice result;
		s = seqFile->Read(toRead, &result, buffer.data());
		if (!s.ok()) {
			return s;
		}
		if (result.empty()) {
			return rocksdb::Status::Corruption("File shorter than recorded size", path);
		}
		auto ev = std::make_unique<StreamEvent>();
		ev->kind = kEventChunk;
		ev->data = reinterpret_cast<const uint8_t*>(result.data());
		ev->size = result.size();
		if (!state->emit(std::move(ev))) {
			return rocksdb::Status::Aborted(state->abortMessage);
		}
		remaining -= result.size();
	}
	return rocksdb::Status::OK();
}

/**
 * The actual work, run on the worker thread. Enumerates the live files via
 * `GetLiveFilesStorageInfo` and streams each one (header event + chunk events)
 * to JS, returning the first failing status (or an abort if the consumer
 * errored / the database is closing).
 */
rocksdb::Status doBackupStream(AsyncBackupStreamState* state) {
	if (!state->descriptor || !state->handle || state->handle->isCancelled() ||
		state->descriptor->isClosing()) {
		return rocksdb::Status::Aborted("Database closed during backup stream operation");
	}

	rocksdb::DB* db = state->descriptor->db.get();
	if (db == nullptr) {
		return rocksdb::Status::Aborted("Database is not open");
	}
	rocksdb::Env* env = db->GetEnv();

	// Disable deletions for the whole read so a compaction can't drop a live
	// file before it has been streamed (see DB::GetLiveFilesStorageInfo docs).
	rocksdb::Status disableStatus = db->DisableFileDeletions();
	if (!disableStatus.ok()) {
		return disableStatus;
	}
	FileDeletionGuard deletionGuard{ db, true };

	rocksdb::LiveFilesStorageInfoOptions infoOpts;
	// 0 == always flush the memtable; max == never (rely on the WAL instead).
	infoOpts.wal_size_for_flush =
		state->flushBeforeBackup ? 0 : std::numeric_limits<uint64_t>::max();
	infoOpts.include_checksum_info = false;

	std::vector<rocksdb::LiveFileStorageInfo> files;
	rocksdb::Status s = db->GetLiveFilesStorageInfo(infoOpts, &files);
	if (!s.ok()) {
		return s;
	}

	std::vector<char> buffer(kChunkSize);

	for (const auto& file : files) {
		if (state->handle->isCancelled()) {
			return rocksdb::Status::Aborted("Database closed during backup stream operation");
		}

		// Header event: tar.addFile(name, size).
		{
			auto ev = std::make_unique<StreamEvent>();
			ev->kind = kEventFile;
			ev->name = file.relative_filename;
			ev->fileSize = file.size;
			if (!state->emit(std::move(ev))) {
				return rocksdb::Status::Aborted(state->abortMessage);
			}
		}

		if (!file.replacement_contents.empty()) {
			// Contents supplied inline (e.g. CURRENT); stream straight through.
			const std::string& contents = file.replacement_contents;
			for (size_t offset = 0; offset < contents.size();) {
				size_t n = std::min(kChunkSize, contents.size() - offset);
				auto ev = std::make_unique<StreamEvent>();
				ev->kind = kEventChunk;
				ev->data = reinterpret_cast<const uint8_t*>(contents.data()) + offset;
				ev->size = n;
				if (!state->emit(std::move(ev))) {
					return rocksdb::Status::Aborted(state->abortMessage);
				}
				offset += n;
			}
			continue;
		}

		// Stream exactly `file.size` bytes. For the manifest `trim_to_size` is set
		// and the file on disk may be longer; stopping at `file.size` is what makes
		// the copy consistent. (Shared with the transaction-log path below.)
		std::string path = file.directory + "/" + file.relative_filename;
		s = streamFilePrefix(state, env, path, file.size, buffer);
		if (!s.ok()) {
			return s;
		}
	}

	// Append the transaction log snapshot (if requested) as tar entries under
	// transaction_logs/<store>/<file>. The current file is bounded by its
	// captured size; each entry carries its real mtime (tar restores it on
	// extract) so the restored store's age-based rotation/retention stays correct.
	if (state->backupTransactionLogs) {
		for (const auto& named : collectTransactionLogBackupEntries(state->descriptor.get())) {
			std::string tarPath = "transaction_logs/" + named.storeName + "/" + named.file.relativeName;

			if (!named.file.inlineContents.empty()) {
				// Inline bytes (txn.state): emit the header + the captured bytes.
				const std::string& contents = named.file.inlineContents;
				auto fileEv = std::make_unique<StreamEvent>();
				fileEv->kind = kEventFile;
				fileEv->name = tarPath;
				fileEv->fileSize = contents.size();
				fileEv->mtime = fileTimeToEpochSeconds(named.file.mtime);
				if (!state->emit(std::move(fileEv))) {
					return rocksdb::Status::Aborted(state->abortMessage);
				}
				for (size_t offset = 0; offset < contents.size();) {
					size_t n = std::min(kChunkSize, contents.size() - offset);
					auto chunkEv = std::make_unique<StreamEvent>();
					chunkEv->kind = kEventChunk;
					chunkEv->data = reinterpret_cast<const uint8_t*>(contents.data()) + offset;
					chunkEv->size = n;
					if (!state->emit(std::move(chunkEv))) {
						return rocksdb::Status::Aborted(state->abortMessage);
					}
					offset += n;
				}
				continue;
			}

			// A concurrent retention purge can unlink a rotated file between the
			// snapshot and now. Skip it (an expiring file dropped from the backup is
			// fine) — and skip BEFORE emitting a header we could not then fulfill.
			std::error_code existsEc;
			if (!std::filesystem::exists(named.file.sourcePath, existsEc) || existsEc) {
				continue;
			}

			auto fileEv = std::make_unique<StreamEvent>();
			fileEv->kind = kEventFile;
			fileEv->name = tarPath;
			fileEv->fileSize = named.file.byteLimit;
			fileEv->mtime = fileTimeToEpochSeconds(named.file.mtime);
			if (!state->emit(std::move(fileEv))) {
				return rocksdb::Status::Aborted(state->abortMessage);
			}
			rocksdb::Status ls =
				streamFilePrefix(state, env, named.file.sourcePath.string(), named.file.byteLimit, buffer);
			if (!ls.ok()) {
				return ls;
			}
		}
	}

	return rocksdb::Status::OK();
}

void backupStreamExecute(napi_env, void* data) {
	auto* state = static_cast<AsyncBackupStreamState*>(data);
	rocksdb::Status s = doBackupStream(state);

	// Release the in-flight claim (the worker owns it once the work was queued).
	// Wake a finishClose() that may be waiting before it tears down the DB.
	if (--state->descriptor->operationsInFlight == 0 && state->descriptor->isClosing()) {
		state->descriptor->operationsInFlight.notify_all();
	}

	state->status = s;
	state->signalExecuteCompleted();
}

void backupStreamComplete(napi_env env, napi_status status, void* data) {
	auto* state = static_cast<AsyncBackupStreamState*>(data);
	state->deleteAsyncWork();

	// Release the tsfn. On a normal run its queue is already drained; on a
	// teardown abort a trampoline may still be queued. Either way, tsfnFinalize
	// runs (on this JS thread) after the queue drains and drops the tsfn's `state`
	// ref — so `state` outlives every trampoline even though `release()` below may
	// drop the complete callback's ref first.
	if (state->tsfn != nullptr) {
		::napi_release_threadsafe_function(state->tsfn, napi_tsfn_release);
		state->tsfn = nullptr;
	}

	if (status != napi_cancelled) {
		if (state->status.ok()) {
			napi_value undefined;
			NAPI_STATUS_THROWS_VOID(::napi_get_undefined(env, &undefined));
			state->callResolve(undefined);
		} else {
			napi_value error;
			rocksdb_js::createRocksDBError(env, state->status, "Backup stream failed", error);
			state->callReject(error);
		}
	}

	// Release the complete callback's ref. If a teardown abort left an emit
	// continuation pending, it holds the surviving ref and frees `state` when it
	// fires; otherwise this is the last ref and frees it now.
	state->release();
}

} // namespace

/**
 * Streams a consistent snapshot of the open database to a JS `emit` callback as
 * a sequence of file-header and chunk events, with no on-disk intermediate copy.
 * The JS side frames these into a tar archive written to a user-provided stream.
 *
 * Signature: `backupStream(resolve, reject, emit, options?)`
 *   emit(kind, a, b) -> Promise   // kind 0: (name, size); kind 1: (Buffer)
 *
 * Resolves with `undefined`. The `emit` promise's resolution paces production
 * (backpressure); its rejection aborts the stream.
 *
 * @example
 * ```typescript
 * const stream = fs.createWriteStream('/path/to/backup.tar');
 * await db.backup(stream, { flushBeforeBackup: true });
 * ```
 */
napi_value Database::BackupStream(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(4);
	UNWRAP_DB_HANDLE_AND_OPEN();

	napi_value resolve = argv[0];
	napi_value reject = argv[1];
	napi_value emit = argv[2];
	napi_value options = argv[3];

	napi_valuetype emitType;
	NAPI_STATUS_THROWS(::napi_typeof(env, emit, &emitType));
	if (emitType != napi_function) {
		::napi_throw_type_error(env, nullptr, "Backup stream emit callback must be a function");
		NAPI_RETURN_UNDEFINED();
	}

	// Default flushing to the WAL setting, matching Database::Backup: when the
	// WAL is disabled, unflushed memtable data would otherwise be lost.
	bool flushBeforeBackup = (*dbHandle)->disableWAL;
	NAPI_STATUS_THROWS(getProperty(env, options, "flushBeforeBackup", flushBeforeBackup));

	bool backupTransactionLogs = false;
	NAPI_STATUS_THROWS(getProperty(env, options, "transactionLogs", backupTransactionLogs));

	// Claim an in-flight operation BEFORE queuing so destroy()/shutdown()/
	// PurgeAll() teardown paths wait for this (potentially long) stream. Mirrors
	// Database::CreateCheckpoint.
	auto descriptor = (*dbHandle)->descriptor;
	++descriptor->operationsInFlight;

	bool handedOff = false;
	struct InFlightClaim {
		DBDescriptor* descriptor;
		const bool& handedOff;
		~InFlightClaim() {
			if (!handedOff && --descriptor->operationsInFlight == 0 && descriptor->isClosing()) {
				descriptor->operationsInFlight.notify_all();
			}
		}
	} claim{ descriptor.get(), handedOff };

	if (descriptor->isClosing()) {
		::napi_throw_error(env, nullptr, "Database is closing");
		NAPI_RETURN_UNDEFINED();
	}

	auto state = new AsyncBackupStreamState(env, *dbHandle, descriptor, flushBeforeBackup);
	state->backupTransactionLogs = backupTransactionLogs;

	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef));
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef));

	napi_value resourceName;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, "database.backupStream", NAPI_AUTO_LENGTH, &resourceName));

	// max_queue_size 0 (unbounded) is safe: the worker self-throttles to a single
	// in-flight event via the ack condvar. context = state, so the trampoline and
	// continuations can reach it. The tsfn holds a `state` ref released only in
	// tsfnFinalize (after all queued trampolines drain), so a trampoline queued
	// when the worker abandons on teardown can never run on freed memory.
	NAPI_STATUS_THROWS(::napi_create_threadsafe_function(
		env,
		emit,
		nullptr,
		resourceName,
		0,
		1,
		state,         // thread_finalize_data
		tsfnFinalize,  // thread_finalize_cb
		state,         // context
		emitTrampoline,
		&state->tsfn
	));
	// The tsfn now owns a reference (dropped in tsfnFinalize). Safe to take only
	// after successful creation — on failure the finalize callback never runs.
	state->acquire();

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,
		nullptr,
		resourceName,
		backupStreamExecute,
		backupStreamComplete,
		state,
		&state->asyncWork
	));

	(*dbHandle)->registerAsyncWork();

	// On a queue failure the claim rolls the counter back (execute never runs).
	// The state/tsfn leak on this rare N-API failure path matches the existing
	// async methods (e.g. Database::Backup).
	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork));

	// The worker now owns the in-flight decrement (end of execute).
	handedOff = true;

	NAPI_RETURN_UNDEFINED();
}

} // namespace rocksdb_js
