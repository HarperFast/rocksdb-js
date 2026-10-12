#ifndef __DATABASE_BACKUP_VERIFY_TABLES_H__
#define __DATABASE_BACKUP_VERIFY_TABLES_H__

#include "core/compression.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/backup_engine.h"
#include <string>

namespace rocksdb_js {

/**
 * Whether a file name read from backup metadata stays inside the backup
 * directory: relative, and with no `..` component.
 */
bool isConfinedBackupPath(const std::string& relative);

/**
 * Proves this build can read every SST in a backup: each table is opened with
 * `SstFileReader` (footer, including `format_version`, plus the meta blocks),
 * and every compression type its properties record must be one this build can
 * decompress. `Open` alone never reads a data block, so a table compressed with
 * an algorithm this build lacks would otherwise pass and fail on the first cold
 * read after a restore.
 *
 * Tables are opened one at a time. Returns the first failure, naming the file
 * relative to `backupDir` and keeping RocksDB's status code. Blob files are not
 * checked: RocksDB exposes no public blob-file reader.
 */
rocksdb::Status verifyBackupTables(
	rocksdb::BackupEngineReadOnly* engine,
	rocksdb::BackupID backupId,
	const std::string& backupDir,
	rocksdb::Env* env = rocksdb::Env::Default(),
	bool (*isSupported)(rocksdb::CompressionType) = isCompressionSupported
);

} // namespace rocksdb_js

#endif
