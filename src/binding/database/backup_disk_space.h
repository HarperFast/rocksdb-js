#ifndef __DATABASE_BACKUP_DISK_SPACE_H__
#define __DATABASE_BACKUP_DISK_SPACE_H__

#include "rocksdb/status.h"
#include <cstdint>
#include <string>

namespace rocksdb {
class DB;
class Env;
} // namespace rocksdb

namespace rocksdb_js {

/**
 * Preflight disk-space check for a directory-target backup (the streaming
 * backup path never opens a `BackupEngine` against a volume and is unaffected).
 *
 * Rejects with `Status::NoSpace` when the destination volume clearly lacks room
 * for the backup. The required size is deliberately the *full* live-file
 * footprint of the database, not the incremental delta: a backup only ever
 * copies files (RocksDB dedups already-shared SSTs into the shared pool), so the
 * full size is a safe upper bound on the bytes a backup — incremental or not —
 * can write. The estimate therefore over-rejects rather than under-rejects; the
 * caller can opt out via `checkDiskSpace: false` when backing up incrementally
 * to a right-sized volume.
 *
 * `additionalRequiredBytes` covers non-RocksDB payload the same backup writes to
 * the same volume — currently the transaction-log snapshot (`transactionLogs`),
 * which the RocksDB live-file set does not include. Without it the estimate
 * would under-count for log-enabled backups whose log store dwarfs the SSTs, and
 * a mid-snapshot `NoSpace` rolls the whole backup back to zero.
 *
 * Degrades to `Status::OK` (skip) when free space is unknowable — `GetFreeSpace`
 * unsupported/errored, or reporting 0 (common on some network filesystems) — or
 * when the DB's live-file set can't be read. A backup is never blocked on a
 * number we cannot trust, mirroring the file-lock's degrade-where-unattainable
 * philosophy.
 *
 * `env` supplies `GetFreeSpace`; pass `rocksdb::Env::Default()` in production
 * (a fake env lets tests drive the reject/degrade branches deterministically).
 */
rocksdb::Status checkBackupDiskSpace(
	rocksdb::DB* db,
	const std::string& backupDir,
	bool flushBeforeBackup,
	uint64_t additionalRequiredBytes,
	rocksdb::Env* env
);

} // namespace rocksdb_js

#endif
