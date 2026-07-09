#include "database/backup.h"
#include "database/backup_transaction_logs.h"
#include "database/database.h"
#include "database/db_handle.h"
#include "database/db_registry.h"
#include "core/file_lock.h"
#include "napi/async.h"
#include "napi/helpers.h"
#include "napi/macros.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/backup_engine.h"
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace rocksdb_js {

/**
 * State for the `Database::Backup` (create) async work. Holds a live `DBHandle`
 * (to keep the underlying RocksDB instance alive during the copy) and the
 * resolved backup options.
 */
struct AsyncBackupState final : BaseAsyncState<std::shared_ptr<DBHandle>> {
	// Hold our own reference to the descriptor (captured on the JS thread) so the
	// underlying rocksdb::DB stays alive for the entire backup even if close() is
	// called concurrently. close() resets the handle's descriptor after a bounded
	// wait, so relying on handle->descriptor from the worker thread is unsafe.
	std::shared_ptr<DBDescriptor> descriptor;
	rocksdb::BackupEngineOptions engineOptions;
	rocksdb::CreateBackupOptions createOptions;
	std::string appMetadata;
	rocksdb::BackupID backupId = 0;
	// When true, snapshot the transaction log store into
	// `<backupDir>/transaction_logs/<backupId>/` after the RocksDB backup.
	bool backupTransactionLogs = false;

	AsyncBackupState(
		napi_env env,
		std::shared_ptr<DBHandle> handle,
		std::shared_ptr<DBDescriptor> descriptor,
		rocksdb::BackupEngineOptions engineOptions,
		rocksdb::CreateBackupOptions createOptions,
		std::string appMetadata
	) :
		BaseAsyncState<std::shared_ptr<DBHandle>>(env, handle),
		descriptor(std::move(descriptor)),
		engineOptions(std::move(engineOptions)),
		createOptions(std::move(createOptions)),
		appMetadata(std::move(appMetadata)) {}

	// Our descriptor ref can be the reason a concurrent close skipped its
	// registry purge (use_count() > 1), so on release we must retry the purge or
	// the registry entry — and the open RocksDB — would linger forever.
	~AsyncBackupState() override {
		if (this->descriptor) {
			std::string path = this->descriptor->path;
			bool readOnly = this->descriptor->readOnly;
			this->descriptor.reset();
			DBRegistry::PurgeIfUnreferenced(path, readOnly);
		}
	}
};

/**
 * State for the `backupRestore` async work. There is no open database during a
 * restore, so the base handle is null.
 */
struct AsyncRestoreState final : BaseAsyncState<std::shared_ptr<DBHandle>> {
	std::string backupDir;
	std::string dbDir;
	std::string walDir;
	rocksdb::BackupEngineOptions engineOptions;
	rocksdb::RestoreOptions restoreOptions;
	bool hasBackupId = false;
	rocksdb::BackupID backupId = 0;

	AsyncRestoreState(napi_env env, std::string backupDir, std::string dbDir, std::string walDir) :
		BaseAsyncState<std::shared_ptr<DBHandle>>(env, nullptr),
		backupDir(std::move(backupDir)),
		dbDir(std::move(dbDir)),
		walDir(std::move(walDir)),
		engineOptions(this->backupDir) {}
};

/**
 * State for the `backupList` async work.
 */
struct AsyncBackupListState final : BaseAsyncState<std::shared_ptr<DBHandle>> {
	std::string backupDir;
	rocksdb::BackupEngineOptions engineOptions;
	std::vector<rocksdb::BackupInfo> backups;

	AsyncBackupListState(napi_env env, std::string backupDir) :
		BaseAsyncState<std::shared_ptr<DBHandle>>(env, nullptr),
		backupDir(std::move(backupDir)),
		engineOptions(this->backupDir) {}
};

/**
 * State for the `backupDelete`, `backupPurge`, and `backupVerify` async work.
 */
struct AsyncBackupOpState final : BaseAsyncState<std::shared_ptr<DBHandle>> {
	enum class Op { Delete, Purge, Verify };

	Op op;
	std::string backupDir;
	rocksdb::BackupEngineOptions engineOptions;
	uint32_t arg = 0; // backupId for Delete/Verify, keepCount for Purge
	bool verifyWithChecksum = false;

	AsyncBackupOpState(napi_env env, Op op, std::string backupDir) :
		BaseAsyncState<std::shared_ptr<DBHandle>>(env, nullptr),
		op(op),
		backupDir(std::move(backupDir)),
		engineOptions(this->backupDir) {}
};

/**
 * Wires up the resolve/reject references, creates and queues the async work.
 * Returns `undefined`. On the rare N-API failure path the state leaks, matching
 * the behavior of the existing async methods (e.g. `Database::Flush`).
 */
template<typename State>
static napi_value queueBackupWork(
	napi_env env,
	const char* resourceName,
	napi_value resolve,
	napi_value reject,
	State* state,
	napi_async_execute_callback execute,
	napi_async_complete_callback complete,
	bool registerWork
) {
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef));
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef));

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, resourceName, NAPI_AUTO_LENGTH, &name));

	NAPI_STATUS_THROWS(::napi_create_async_work(env, nullptr, name, execute, complete, state, &state->asyncWork));

	if (registerWork && state->handle) {
		state->handle->registerAsyncWork();
	}

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork));

	NAPI_RETURN_UNDEFINED();
}

/**
 * Name of the on-disk lock file at the backup directory root. Must match the
 * name used by `withBackupDirLock` in `src/backup.ts` (and mirrored in
 * `test/backup.test.ts`) — every writer to a backup directory contends on the
 * same file.
 */
static constexpr const char* BACKUP_LOCK_FILENAME = ".backup.lock";

/**
 * Worker-thread body of `Database::Backup`. Creates the backup directory
 * (including missing parents — the lock file lives at the directory root and
 * RocksDB itself only creates the leaf), then holds the single-writer
 * directory lock for the duration of the RocksDB backup and the transaction
 * log snapshot. RocksDB has no cross-engine lock on the directory, so a
 * concurrent writer — even in another process — would corrupt the staging
 * directory. Contention rejects rather than queues. `backups.delete`/`purge`
 * take the same lock from JS via `withBackupDirLock` (see `src/backup.ts` and
 * `core/file_lock.h`).
 */
static rocksdb::Status runCreateBackup(AsyncBackupState* state) {
	// state->descriptor keeps the rocksdb::DB alive for the whole copy, so it is
	// safe to use even if close() runs concurrently. isCancelled() lets us skip
	// starting a backup once close() has been requested.
	if (!state->descriptor || !state->handle || state->handle->isCancelled()) {
		return rocksdb::Status::Aborted("Database closed during backup operation");
	}

	const std::string& backupDir = state->engineOptions.backup_dir;

	std::error_code ec;
	std::filesystem::create_directories(std::filesystem::path(backupDir), ec);
	if (ec) {
		return rocksdb::Status::IOError("Failed to create backup directory: " + ec.message(), backupDir);
	}

	uint32_t lockToken = 0;
	try {
		// Deliberately plain string concatenation: `backupDir` is UTF-8 from N-API,
		// and on Windows a std::filesystem::path round-trip (string → path →
		// .string()) re-encodes through the active code page, corrupting non-ASCII
		// paths before tryAcquireFileLock's own UTF-8 → wide conversion. "/" is a
		// valid separator on every platform.
		lockToken = tryAcquireFileLock(backupDir + "/" + BACKUP_LOCK_FILENAME);
	} catch (const std::exception& e) {
		return rocksdb::Status::IOError(e.what());
	}
	if (lockToken == 0) {
		return rocksdb::Status::Busy("Backup directory is locked: " + backupDir);
	}

	// Holding the exclusive directory lock means no other writer can be
	// mid-snapshot, so any transaction-log staging directory found here was left
	// by a crashed backup — sweep it before creating the new backup
	// (`backups.purge` also prunes such leftovers as orphans).
	if (state->backupTransactionLogs) {
		removeStaleTransactionLogStaging(std::filesystem::path(backupDir) / "transaction_logs");
	}

	rocksdb::BackupEngine* engine = nullptr;
	rocksdb::Status status = rocksdb::BackupEngine::Open(state->engineOptions, rocksdb::Env::Default(), &engine);
	if (status.ok()) {
		status = engine->CreateNewBackupWithMetadata(
			state->createOptions,
			state->descriptor->db.get(),
			state->appMetadata,
			&state->backupId
		);

		// After a successful RocksDB backup, snapshot the transaction logs into
		// transaction_logs/<backupId>/ (all-or-nothing; not incremental). The
		// snapshot is staged and atomically renamed into place — and fsynced when
		// `sync` is set — inside backupTransactionLogsToDir, so a crash mid-copy
		// can never leave a partial subtree at the final path. On failure, roll
		// the whole backup back: CreateNewBackupWithMetadata already registered
		// the backup, so leaving it would produce a listed backup whose restore
		// silently yields no transaction logs. The rollback is best-effort — the
		// snapshot's failure status is what rejects the call.
		if (status.ok() && state->backupTransactionLogs) {
			std::filesystem::path logsDir =
				std::filesystem::path(backupDir) / "transaction_logs" / std::to_string(state->backupId);
			status = backupTransactionLogsToDir(state->descriptor.get(), logsDir, state->engineOptions.sync);
			if (!status.ok()) {
				engine->DeleteBackup(state->backupId).PermitUncheckedError();
			}
		}
	}
	delete engine;

	releaseFileLock(lockToken);
	return status;
}

/**
 * Maps a `RestoreOptions.mode` string to the RocksDB enum. Unknown/absent
 * values fall back to the (destructive) default `kPurgeAllFiles`.
 */
static rocksdb::RestoreOptions::Mode parseRestoreMode(const std::string& mode) {
	if (mode == "keepLatestDbSessionIdFiles") {
		return rocksdb::RestoreOptions::Mode::kKeepLatestDbSessionIdFiles;
	}
	if (mode == "verifyChecksum") {
		return rocksdb::RestoreOptions::Mode::kVerifyChecksum;
	}
	return rocksdb::RestoreOptions::Mode::kPurgeAllFiles;
}

/**
 * Creates a new backup of the database into the given backup directory. The
 * database must be open. Resolves with the new backup id.
 *
 * Signature: `backup(resolve, reject, backupDir, options?)`
 *
 * @example
 * ```typescript
 * const id = await db.backup('/path/to/backups');
 * ```
 */
napi_value Database::Backup(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(4);
	UNWRAP_DB_HANDLE_AND_OPEN();

	napi_value resolve = argv[0];
	napi_value reject = argv[1];

	NAPI_GET_STRING(argv[2], backupDir, "Backup directory must be a string");

	rocksdb::BackupEngineOptions engineOptions(backupDir);
	rocksdb::CreateBackupOptions createOptions;
	std::string appMetadata;

	// Default flushing to the WAL setting: when the WAL is disabled, unflushed
	// memtable data would otherwise be lost from the backup.
	createOptions.flush_before_backup = (*dbHandle)->disableWAL;

	napi_value options = argv[3];
	NAPI_STATUS_THROWS(getProperty(env, options, "flushBeforeBackup", createOptions.flush_before_backup));
	NAPI_STATUS_THROWS(getProperty(env, options, "metadata", appMetadata));
	NAPI_STATUS_THROWS(getProperty(env, options, "shareTableFiles", engineOptions.share_table_files));
	NAPI_STATUS_THROWS(getProperty(env, options, "shareFilesWithChecksum", engineOptions.share_files_with_checksum));
	NAPI_STATUS_THROWS(getProperty(env, options, "backupLogFiles", engineOptions.backup_log_files));
	NAPI_STATUS_THROWS(getProperty(env, options, "sync", engineOptions.sync));
	NAPI_STATUS_THROWS(getProperty(env, options, "maxBackgroundOperations", engineOptions.max_background_operations));

	bool backupTransactionLogs = false;
	NAPI_STATUS_THROWS(getProperty(env, options, "transactionLogs", backupTransactionLogs));

	auto state = new AsyncBackupState(
		env,
		*dbHandle,
		(*dbHandle)->descriptor,
		std::move(engineOptions),
		std::move(createOptions),
		std::move(appMetadata)
	);
	state->backupTransactionLogs = backupTransactionLogs;

	return queueBackupWork(
		env,
		"database.backup",
		resolve,
		reject,
		state,
		[](napi_env, void* data) { // execute
			auto state = reinterpret_cast<AsyncBackupState*>(data);
			state->status = runCreateBackup(state);
			state->signalExecuteCompleted();
		},
		[](napi_env env, napi_status status, void* data) { // complete
			auto state = reinterpret_cast<AsyncBackupState*>(data);
			state->deleteAsyncWork();
			if (status != napi_cancelled) {
				if (state->status.ok()) {
					napi_value result;
					NAPI_STATUS_THROWS_VOID(::napi_create_uint32(env, state->backupId, &result));
					state->callResolve(result);
				} else {
					napi_value error;
					rocksdb_js::createRocksDBError(env, state->status, "Backup failed", error);
					state->callReject(error);
				}
			}
			delete state;
		},
		true // registerWork
	);
}

/**
 * Restores a database from a backup directory into a (closed) database
 * directory. Resolves with `undefined`.
 *
 * Signature: `backupRestore(resolve, reject, backupDir, dbDir, walDir, options?)`
 */
static napi_value BackupRestore(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(6);

	napi_value resolve = argv[0];
	napi_value reject = argv[1];

	NAPI_GET_STRING(argv[2], backupDir, "Backup directory must be a string");
	NAPI_GET_STRING(argv[3], dbDir, "Database directory must be a string");
	NAPI_GET_STRING(argv[4], walDir, "WAL directory must be a string");

	napi_value options = argv[5];

	rocksdb::RestoreOptions restoreOptions;
	NAPI_STATUS_THROWS(getProperty(env, options, "keepLogFiles", restoreOptions.keep_log_files));

	std::string mode;
	NAPI_STATUS_THROWS(getProperty(env, options, "mode", mode));
	restoreOptions.mode = parseRestoreMode(mode);

	bool hasBackupId = false;
	uint32_t backupId = 0;
	napi_valuetype optionsType;
	NAPI_STATUS_THROWS(::napi_typeof(env, options, &optionsType));
	if (optionsType == napi_object) {
		bool has = false;
		NAPI_STATUS_THROWS(::napi_has_named_property(env, options, "backupId", &has));
		if (has) {
			napi_value backupIdValue;
			NAPI_STATUS_THROWS(::napi_get_named_property(env, options, "backupId", &backupIdValue));
			napi_valuetype backupIdType;
			NAPI_STATUS_THROWS(::napi_typeof(env, backupIdValue, &backupIdType));
			if (backupIdType == napi_number) {
				NAPI_STATUS_THROWS(::napi_get_value_uint32(env, backupIdValue, &backupId));
				hasBackupId = true;
			}
		}
	}

	auto state = new AsyncRestoreState(env, std::move(backupDir), std::move(dbDir), std::move(walDir));
	state->restoreOptions = restoreOptions;
	state->hasBackupId = hasBackupId;
	state->backupId = backupId;

	return queueBackupWork(
		env,
		"database.backupRestore",
		resolve,
		reject,
		state,
		[](napi_env, void* data) { // execute
			auto state = reinterpret_cast<AsyncRestoreState*>(data);
			rocksdb::BackupEngineReadOnly* engine = nullptr;
			rocksdb::IOStatus s = rocksdb::BackupEngineReadOnly::Open(state->engineOptions, rocksdb::Env::Default(), &engine);
			if (s.ok()) {
				if (state->hasBackupId) {
					s = engine->RestoreDBFromBackup(state->restoreOptions, state->backupId, state->dbDir, state->walDir);
				} else {
					s = engine->RestoreDBFromLatestBackup(state->restoreOptions, state->dbDir, state->walDir);
				}
			}
			delete engine;
			state->status = s;
			state->signalExecuteCompleted();
		},
		[](napi_env env, napi_status status, void* data) { // complete
			auto state = reinterpret_cast<AsyncRestoreState*>(data);
			state->deleteAsyncWork();
			if (status != napi_cancelled) {
				if (state->status.ok()) {
					napi_value undefined;
					NAPI_STATUS_THROWS_VOID(::napi_get_undefined(env, &undefined));
					state->callResolve(undefined);
				} else {
					napi_value error;
					rocksdb_js::createRocksDBError(env, state->status, "Restore failed", error);
					state->callReject(error);
				}
			}
			delete state;
		},
		false // registerWork
	);
}

/**
 * Lists the non-corrupt backups in a backup directory. Resolves with an array
 * of backup info objects.
 *
 * Signature: `backupList(resolve, reject, backupDir)`
 */
static napi_value BackupList(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(3);

	napi_value resolve = argv[0];
	napi_value reject = argv[1];

	NAPI_GET_STRING(argv[2], backupDir, "Backup directory must be a string");

	auto state = new AsyncBackupListState(env, std::move(backupDir));

	return queueBackupWork(
		env,
		"database.backupList",
		resolve,
		reject,
		state,
		[](napi_env, void* data) { // execute
			auto state = reinterpret_cast<AsyncBackupListState*>(data);
			rocksdb::BackupEngineReadOnly* engine = nullptr;
			rocksdb::IOStatus s = rocksdb::BackupEngineReadOnly::Open(state->engineOptions, rocksdb::Env::Default(), &engine);
			if (s.ok()) {
				engine->GetBackupInfo(&state->backups);
			}
			delete engine;
			state->status = s;
			state->signalExecuteCompleted();
		},
		[](napi_env env, napi_status status, void* data) { // complete
			auto state = reinterpret_cast<AsyncBackupListState*>(data);
			state->deleteAsyncWork();
			if (status != napi_cancelled) {
				if (state->status.ok()) {
					napi_value array;
					NAPI_STATUS_THROWS_VOID(::napi_create_array_with_length(env, state->backups.size(), &array));
					for (size_t i = 0; i < state->backups.size(); ++i) {
						const rocksdb::BackupInfo& backup = state->backups[i];
						napi_value obj;
						NAPI_STATUS_THROWS_VOID(::napi_create_object(env, &obj));

						napi_value value;
						NAPI_STATUS_THROWS_VOID(::napi_create_uint32(env, backup.backup_id, &value));
						NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, obj, "backupId", value));

						NAPI_STATUS_THROWS_VOID(::napi_create_int64(env, backup.timestamp, &value));
						NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, obj, "timestamp", value));

						NAPI_STATUS_THROWS_VOID(::napi_create_int64(env, static_cast<int64_t>(backup.size), &value));
						NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, obj, "size", value));

						NAPI_STATUS_THROWS_VOID(::napi_create_uint32(env, backup.number_files, &value));
						NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, obj, "numberFiles", value));

						NAPI_STATUS_THROWS_VOID(::napi_create_string_utf8(env, backup.app_metadata.c_str(), backup.app_metadata.size(), &value));
						NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, obj, "appMetadata", value));

						NAPI_STATUS_THROWS_VOID(::napi_set_element(env, array, i, obj));
					}
					state->callResolve(array);
				} else {
					napi_value error;
					rocksdb_js::createRocksDBError(env, state->status, "List backups failed", error);
					state->callReject(error);
				}
			}
			delete state;
		},
		false // registerWork
	);
}

/**
 * Shared implementation for `backupDelete`, `backupPurge`, and `backupVerify`.
 */
static napi_value queueBackupOp(napi_env env, AsyncBackupOpState* state, napi_value resolve, napi_value reject) {
	return queueBackupWork(
		env,
		"database.backupOp",
		resolve,
		reject,
		state,
		[](napi_env, void* data) { // execute
			auto state = reinterpret_cast<AsyncBackupOpState*>(data);
			if (state->op == AsyncBackupOpState::Op::Verify) {
				// Read-only verification.
				rocksdb::BackupEngineReadOnly* engine = nullptr;
				rocksdb::IOStatus s = rocksdb::BackupEngineReadOnly::Open(state->engineOptions, rocksdb::Env::Default(), &engine);
				if (s.ok()) {
					s = engine->VerifyBackup(state->arg, state->verifyWithChecksum);
				}
				delete engine;
				state->status = s;
			} else {
				// Delete/Purge require a writable engine.
				rocksdb::BackupEngine* engine = nullptr;
				rocksdb::IOStatus s = rocksdb::BackupEngine::Open(state->engineOptions, rocksdb::Env::Default(), &engine);
				if (s.ok()) {
					if (state->op == AsyncBackupOpState::Op::Delete) {
						s = engine->DeleteBackup(state->arg);
					} else {
						s = engine->PurgeOldBackups(state->arg);
					}
				}
				delete engine;
				state->status = s;
			}
			state->signalExecuteCompleted();
		},
		[](napi_env env, napi_status status, void* data) { // complete
			auto state = reinterpret_cast<AsyncBackupOpState*>(data);
			state->deleteAsyncWork();
			if (status != napi_cancelled) {
				if (state->status.ok()) {
					napi_value undefined;
					NAPI_STATUS_THROWS_VOID(::napi_get_undefined(env, &undefined));
					state->callResolve(undefined);
				} else {
					napi_value error;
					rocksdb_js::createRocksDBError(env, state->status, "Backup operation failed", error);
					state->callReject(error);
				}
			}
			delete state;
		},
		false // registerWork
	);
}

/**
 * Deletes a specific backup (reference-counted; shared files are only removed
 * when no remaining backup references them).
 *
 * Signature: `backupDelete(resolve, reject, backupDir, backupId)`
 */
static napi_value BackupDelete(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(4);
	napi_value resolve = argv[0];
	napi_value reject = argv[1];
	NAPI_GET_STRING(argv[2], backupDir, "Backup directory must be a string");

	auto state = new AsyncBackupOpState(env, AsyncBackupOpState::Op::Delete, std::move(backupDir));
	NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[3], &state->arg));

	return queueBackupOp(env, state, resolve, reject);
}

/**
 * Purges all but the newest `keepCount` backups.
 *
 * Signature: `backupPurge(resolve, reject, backupDir, keepCount)`
 */
static napi_value BackupPurge(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(4);
	napi_value resolve = argv[0];
	napi_value reject = argv[1];
	NAPI_GET_STRING(argv[2], backupDir, "Backup directory must be a string");

	auto state = new AsyncBackupOpState(env, AsyncBackupOpState::Op::Purge, std::move(backupDir));
	NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[3], &state->arg));

	return queueBackupOp(env, state, resolve, reject);
}

/**
 * Verifies a backup's file sizes (and optionally checksums).
 *
 * Signature: `backupVerify(resolve, reject, backupDir, backupId, verifyWithChecksum)`
 */
static napi_value BackupVerify(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(5);
	napi_value resolve = argv[0];
	napi_value reject = argv[1];
	NAPI_GET_STRING(argv[2], backupDir, "Backup directory must be a string");

	auto state = new AsyncBackupOpState(env, AsyncBackupOpState::Op::Verify, std::move(backupDir));
	NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[3], &state->arg));

	napi_valuetype checksumType;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[4], &checksumType));
	if (checksumType == napi_boolean) {
		NAPI_STATUS_THROWS(::napi_get_value_bool(env, argv[4], &state->verifyWithChecksum));
	}

	return queueBackupOp(env, state, resolve, reject);
}

void initBackupExports(napi_env env, napi_value exports) {
	struct {
		const char* name;
		napi_callback fn;
	} functions[] = {
		{ "backupRestore", BackupRestore },
		{ "backupList", BackupList },
		{ "backupDelete", BackupDelete },
		{ "backupPurge", BackupPurge },
		{ "backupVerify", BackupVerify },
	};

	for (const auto& function : functions) {
		napi_value fn;
		NAPI_STATUS_THROWS_VOID(::napi_create_function(env, function.name, NAPI_AUTO_LENGTH, function.fn, nullptr, &fn));
		NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, function.name, fn));
	}
}

} // namespace rocksdb_js
