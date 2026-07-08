#ifndef __DATABASE_BACKUP_TRANSACTION_LOGS_H__
#define __DATABASE_BACKUP_TRANSACTION_LOGS_H__

#include "rocksdb/status.h"
#include "transaction_log/transaction_log_store.h" // TransactionLogBackupEntry
#include <filesystem>
#include <string>
#include <vector>

namespace rocksdb_js {

struct DBDescriptor;

/**
 * A transaction-log file to back up, tagged with the store it belongs to. The
 * store name is the subdirectory under `transaction_logs/` in both the backup
 * directory layout (`transaction_logs/<backupId>/<store>/<file>`) and the
 * streamed tar layout (`transaction_logs/<store>/<file>`).
 */
struct NamedTransactionLogBackupEntry final {
	std::string storeName;
	TransactionLogBackupEntry file;
};

/**
 * Collects the backup entries for every transaction log store owned by the
 * database at `descriptor` (via the process-global store registry). Each
 * store's files are snapshotted under that store's `dataSetsMutex`. Intended to
 * run on a backup worker thread while the database stays pinned.
 */
std::vector<NamedTransactionLogBackupEntry> collectTransactionLogBackupEntries(DBDescriptor* descriptor);

/**
 * Prefix of the staging directory a snapshot is copied into before being
 * atomically renamed to its final `<backupId>` name. Dot-prefixed so it can
 * never collide with a backup id. A directory still carrying this prefix is a
 * crashed backup's leftover: `removeStaleTransactionLogStaging` sweeps them at
 * backup time (under the backup-dir lock) and `backups.purge` prunes them as
 * orphans (they never match a live id).
 */
inline constexpr const char* TRANSACTION_LOG_STAGING_PREFIX = ".staging-";

/**
 * Best-effort removal of every `.staging-*` leftover under `logsRoot`
 * (`<backupDir>/transaction_logs`). Must only run while holding the backup
 * directory's single-writer lock — that lock is what guarantees no live backup
 * is mid-stage. A missing `logsRoot` is a no-op.
 */
void removeStaleTransactionLogStaging(const std::filesystem::path& logsRoot);

/**
 * Copies the transaction log snapshot into `destBaseDir` (e.g.
 * `<backupDir>/transaction_logs/<backupId>`), laying files out as
 * `<destBaseDir>/<store>/<file>`. Rotated (immutable) files are hard-linked
 * when possible, falling back to a byte copy on any link failure (typically
 * `EXDEV` for a destination on another filesystem — e.g. a mounted network
 * volume — or `ENOTSUP` on filesystems without hard links); the current file
 * and `txn.state` are always copied up to their captured byte limit. The
 * source mtime is preserved on every destination so the store's age-based
 * rotation/retention stays correct after a restore.
 *
 * Crash atomicity: the snapshot is staged in a sibling
 * `TRANSACTION_LOG_STAGING_PREFIX` directory and atomically renamed into
 * `destBaseDir` only after every file copied — the final path either holds the
 * complete snapshot or nothing, never a partial subtree (the backup id is
 * already durably registered with RocksDB by the time this runs, so a partial
 * subtree at the final path would silently restore incomplete logs). When no
 * store has anything to snapshot, `destBaseDir` is not created at all.
 *
 * `sync` mirrors `BackupEngineOptions::sync`: when set, every written file,
 * every created directory, and the publishing rename are fsynced, giving the
 * log payload the same crash durability as the RocksDB files in the backup.
 */
rocksdb::Status backupTransactionLogsToDir(
	DBDescriptor* descriptor,
	const std::filesystem::path& destBaseDir,
	bool sync
);

} // namespace rocksdb_js

#endif
