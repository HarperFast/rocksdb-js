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
 * Copies the transaction log snapshot into `destBaseDir` (e.g.
 * `<backupDir>/transaction_logs/<backupId>`), laying files out as
 * `<destBaseDir>/<store>/<file>`. Rotated (immutable) files are hard-linked
 * when possible, falling back to a byte copy across filesystems or on platforms
 * without hard-link support; the current file and `txn.state` are always copied
 * up to their captured byte limit. The source mtime is preserved on every
 * destination so the store's age-based rotation/retention stays correct after a
 * restore.
 */
rocksdb::Status backupTransactionLogsToDir(
	DBDescriptor* descriptor,
	const std::filesystem::path& destBaseDir
);

} // namespace rocksdb_js

#endif
