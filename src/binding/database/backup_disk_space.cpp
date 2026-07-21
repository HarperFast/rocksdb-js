#include "database/backup_disk_space.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include <limits>
#include <string>
#include <vector>

namespace rocksdb_js {

rocksdb::Status checkBackupDiskSpace(
	rocksdb::DB* db,
	const std::string& backupDir,
	bool flushBeforeBackup,
	uint64_t additionalRequiredBytes,
	rocksdb::Env* env
) {
	// Free space on the destination volume. Env::GetFreeSpace maps to statvfs
	// (f_bavail — space available to unprivileged users) on POSIX and
	// GetDiskFreeSpaceEx (bytes available to the caller) on Windows, both of
	// which query the filesystem mounted at the path — so a network mount
	// reports the server's free space.
	//
	// A GetFreeSpace error, or a reported 0, is treated as "unknown" and skips
	// the check. This deliberately also lets a genuinely full local volume (a
	// real 0) through to a mid-backup failure: 0 cannot be distinguished from the
	// spurious 0 some network filesystems report, and blocking every backup on
	// those FSes is the worse failure. Callers needing a hard guarantee should
	// monitor the volume out of band.
	uint64_t freeBytes = 0;
	rocksdb::Status status = env->GetFreeSpace(backupDir, &freeBytes);
	if (!status.ok() || freeBytes == 0) {
		return rocksdb::Status::OK();
	}

	// Required = current on-disk live-file footprint (SST + blob + WAL + MANIFEST
	// + CURRENT + OPTIONS). wal_size_for_flush = max disables the flush that
	// GetLiveFilesStorageInfo performs by default (its default of 0 means "always
	// flush"): this is a read-only preflight and must not mutate the DB.
	rocksdb::LiveFilesStorageInfoOptions opts;
	opts.include_checksum_info = false;
	opts.wal_size_for_flush = std::numeric_limits<uint64_t>::max();
	std::vector<rocksdb::LiveFileStorageInfo> files;
	status = db->GetLiveFilesStorageInfo(opts, &files);
	if (!status.ok()) {
		return rocksdb::Status::OK(); // can't size the DB → skip rather than block
	}

	uint64_t required = additionalRequiredBytes;
	for (const auto& file : files) {
		required += file.size;
	}

	// When the backup flushes first, the not-yet-flushed memtable becomes new SST
	// files after this check, so count its current size. This slightly
	// over-counts (it overlaps the live WAL already summed above, and compression
	// shrinks the resulting SSTs), which keeps the estimate on the conservative
	// side.
	if (flushBeforeBackup) {
		uint64_t memtableBytes = 0;
		if (db->GetAggregatedIntProperty(rocksdb::DB::Properties::kCurSizeAllMemTables, &memtableBytes)) {
			required += memtableBytes;
		}
	}

	if (freeBytes < required) {
		return rocksdb::Status::NoSpace(
			"Insufficient disk space for backup at '" + backupDir + "': " + std::to_string(freeBytes) +
			" bytes free, ~" + std::to_string(required) + " bytes required (set checkDiskSpace:false to override)"
		);
	}
	return rocksdb::Status::OK();
}

} // namespace rocksdb_js
