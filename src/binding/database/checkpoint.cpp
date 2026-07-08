#include "database/database.h"
#include "database/db_handle.h"
#include "database/db_registry.h"
#include "napi/async.h"
#include "napi/helpers.h"
#include "napi/macros.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/checkpoint.h"
#include <memory>
#include <string>

namespace rocksdb_js {

/**
 * State for the `Database::CreateCheckpoint` async work. Holds a live `DBHandle`
 * and its own reference to the descriptor (captured on the JS thread) so the
 * underlying `rocksdb::DB` stays alive for the entire checkpoint even if
 * `close()` is called concurrently — `close()` resets the handle's descriptor
 * after a bounded wait, so relying on `handle->descriptor` from the worker
 * thread is unsafe. This mirrors `AsyncBackupState`.
 *
 * The checkpoint also registers in the descriptor's `operationsInFlight`
 * counter (see `Database::CreateCheckpoint`). Unlike `close()`, the
 * `destroy()` / `shutdown()` / `PurgeAll()` teardown paths call
 * `DBDescriptor::finishClose()` directly without waiting on the async-work
 * tracker, and `finishClose()` resets `descriptor->db`. Registering in
 * `operationsInFlight` makes `finishClose()` wait for the copy to finish
 * before tearing down the database, so the worker never dereferences a
 * freed `rocksdb::DB`.
 *
 * Note: like backup, pinning the descriptor defers the registry purge if a
 * caller closes the database before the checkpoint settles
 * (HarperFast/rocksdb-js#672); the destructor retries the purge when the ref
 * is released so the deferral is temporary rather than a permanent leak.
 */
struct AsyncCheckpointState final : BaseAsyncState<std::shared_ptr<DBHandle>> {
	std::shared_ptr<DBDescriptor> descriptor;
	std::string targetPath;

	AsyncCheckpointState(
		napi_env env,
		std::shared_ptr<DBHandle> handle,
		std::shared_ptr<DBDescriptor> descriptor,
		std::string targetPath
	) :
		BaseAsyncState<std::shared_ptr<DBHandle>>(env, handle),
		descriptor(std::move(descriptor)),
		targetPath(std::move(targetPath)) {}

	~AsyncCheckpointState() override {
		if (this->descriptor) {
			std::string path = this->descriptor->path;
			bool readOnly = this->descriptor->readOnly;
			this->descriptor.reset();
			DBRegistry::PurgeIfUnreferenced(path, readOnly);
		}
	}
};

/**
 * RAII release for a descriptor `operationsInFlight` claim made on the JS
 * thread. Decrements the counter (and wakes a waiting `finishClose()`) on any
 * early return, unless the claim was handed off to the async worker — which
 * then owns the matching decrement at the end of its execute callback.
 */
struct CheckpointInFlightClaim {
	DBDescriptor* descriptor;
	const bool& handedOff;

	~CheckpointInFlightClaim() {
		if (!handedOff && --descriptor->operationsInFlight == 0 && descriptor->isClosing()) {
			descriptor->operationsInFlight.notify_all();
		}
	}
};

/**
 * Creates a hardlinked, point-in-time, fully independent copy of the open
 * database at `targetPath` using RocksDB's `Checkpoint` API. The target path
 * must not already exist (RocksDB creates it). Resolves with `undefined`.
 *
 * Signature: `createCheckpoint(resolve, reject, targetPath)`
 *
 * @example
 * ```typescript
 * await db.createCheckpoint('/path/to/checkpoint');
 * ```
 */
napi_value Database::CreateCheckpoint(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(3);
	UNWRAP_DB_HANDLE_AND_OPEN();

	napi_value resolve = argv[0];
	napi_value reject = argv[1];

	NAPI_GET_STRING(argv[2], targetPath, "Checkpoint target path must be a string");

	// Claim an in-flight operation on the descriptor BEFORE queuing so the
	// destroy()/shutdown()/PurgeAll() teardown paths — which call
	// DBDescriptor::finishClose() directly, bypassing the async-work tracker, and
	// reset descriptor->db — wait for this copy first. Incrementing before
	// checking isClosing() mirrors OperationGuard: close() publishes the closing
	// flag (beginClose()) before finishClose() waits on this counter, so if we
	// still observe isClosing() after our increment the teardown may already be
	// past its wait and about to free the DB — bail rather than start the copy.
	auto descriptor = (*dbHandle)->descriptor;
	++descriptor->operationsInFlight;

	// Releases the claim on any early return below; cleared once the worker takes
	// ownership of the decrement (at the end of execute) after a successful queue.
	bool handedOff = false;
	CheckpointInFlightClaim claim{descriptor.get(), handedOff};

	if (descriptor->isClosing()) {
		::napi_throw_error(env, nullptr, "Database is closing");
		NAPI_RETURN_UNDEFINED();
	}

	auto state = new AsyncCheckpointState(
		env,
		*dbHandle,
		descriptor,
		std::move(targetPath)
	);

	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef));
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef));

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, "database.createCheckpoint", NAPI_AUTO_LENGTH, &name));

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,
		nullptr,
		name,
		[](napi_env, void* data) { // execute
			auto state = reinterpret_cast<AsyncCheckpointState*>(data);
			// state->descriptor is a strong reference held for the whole copy, so it
			// is always valid here; the operationsInFlight registration keeps
			// finishClose() from resetting descriptor->db until this work completes,
			// so the database stays valid even if close() runs concurrently.
			// isCancelled() lets us skip starting a checkpoint once close() has been
			// requested.
			if (!state->handle || state->handle->isCancelled()) {
				state->status = rocksdb::Status::Aborted("Database closed during checkpoint operation");
			} else {
				rocksdb::Checkpoint* checkpoint = nullptr;
				rocksdb::Status s = rocksdb::Checkpoint::Create(state->descriptor->db.get(), &checkpoint);
				if (s.ok()) {
					std::unique_ptr<rocksdb::Checkpoint> guard(checkpoint);
					// log_size_for_flush = 0 (default) always flushes the memtable so
					// the checkpoint contains up-to-date data even when the WAL is
					// disabled.
					s = checkpoint->CreateCheckpoint(state->targetPath);
				}
				state->status = s;
			}
			// Release the in-flight claim made on the JS thread (the worker owns
			// this decrement once the work was queued). Wake any finishClose()
			// waiting for this copy before it tears down descriptor->db. Mirrors
			// OperationGuard's destructor.
			if (--state->descriptor->operationsInFlight == 0 && state->descriptor->isClosing()) {
				state->descriptor->operationsInFlight.notify_all();
			}
			state->signalExecuteCompleted();
		},
		[](napi_env env, napi_status status, void* data) { // complete
			auto state = reinterpret_cast<AsyncCheckpointState*>(data);
			state->deleteAsyncWork();
			if (status != napi_cancelled) {
				if (state->status.ok()) {
					napi_value undefined;
					NAPI_STATUS_THROWS_VOID(::napi_get_undefined(env, &undefined));
					state->callResolve(undefined);
				} else {
					napi_value error;
					rocksdb_js::createRocksDBError(env, state->status, "Create checkpoint failed", error);
					state->callReject(error);
				}
			}
			delete state;
		},
		state,
		&state->asyncWork
	));

	(*dbHandle)->registerAsyncWork();

	// On a queue failure the claim above rolls the counter back (execute never
	// runs); the state leak on this rare N-API failure path matches the existing
	// async methods (e.g. Database::Backup).
	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork));

	// The worker now owns the in-flight decrement (end of execute); stop the
	// claim from releasing it here.
	handedOff = true;

	NAPI_RETURN_UNDEFINED();
}

} // namespace rocksdb_js
