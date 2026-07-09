#include "database/backup_transaction_logs.h"
#include "database/db_descriptor.h"
#include "transaction_log/transaction_log_store_registry.h"
#include <algorithm>
#include <cstdint>
#include <fstream>
#include <set>
#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#endif

namespace rocksdb_js {

std::vector<NamedTransactionLogBackupEntry> collectTransactionLogBackupEntries(DBDescriptor* descriptor) {
	std::vector<NamedTransactionLogBackupEntry> entries;
	if (descriptor == nullptr) {
		return entries;
	}

	auto stores = TransactionLogStoreRegistry::GetStores(descriptor->path);
	for (const auto& store : stores) {
		if (!store) {
			continue;
		}
		const std::string& storeName = store->name;
		for (auto& file : store->snapshotForBackup()) {
			entries.push_back({ storeName, std::move(file) });
		}
	}
	return entries;
}

/**
 * Flushes a written file's data (and metadata) to stable storage. Honors the
 * backup's `sync` option: matches the durability RocksDB's own backup engine
 * gives the engine files, so the transaction log payload cannot be silently
 * less durable than the rest of the same backup.
 */
static rocksdb::Status syncFile(const std::filesystem::path& path) {
#ifdef _WIN32
	// FlushFileBuffers requires GENERIC_WRITE access.
	HANDLE handle = ::CreateFileW(
		path.c_str(),
		GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
		nullptr,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL,
		nullptr
	);
	if (handle == INVALID_HANDLE_VALUE) {
		return rocksdb::Status::IOError("Failed to open backup transaction log file for sync", path.string());
	}
	BOOL ok = ::FlushFileBuffers(handle);
	::CloseHandle(handle);
	if (!ok) {
		return rocksdb::Status::IOError("Failed to sync backup transaction log file", path.string());
	}
#else
	int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
	if (fd < 0) {
		return rocksdb::Status::IOError("Failed to open backup transaction log file for sync", path.string());
	}
	int rc = ::fsync(fd);
	::close(fd);
	if (rc != 0) {
		return rocksdb::Status::IOError("Failed to sync backup transaction log file", path.string());
	}
#endif
	return rocksdb::Status::OK();
}

/**
 * Flushes a directory's entries (file creations, hard links, renames) to
 * stable storage. A file's own fsync does not persist its directory entry, so
 * without this a crash could durably keep the bytes but lose the name.
 *
 * On Windows this is a no-op: directory handles cannot be flushed the way
 * POSIX directory fds can, and NTFS journals metadata updates. On POSIX,
 * filesystems that reject fsync on a directory fd (some network filesystems
 * return EINVAL/ENOTSUP) are treated as success — the same forfeiture rule the
 * backup lock applies where `flock` is unsupported.
 */
static rocksdb::Status syncDirectory(const std::filesystem::path& path) {
#ifndef _WIN32
	int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (fd < 0) {
		return rocksdb::Status::IOError("Failed to open backup log directory for sync", path.string());
	}
	int rc = ::fsync(fd);
	int syncErrno = errno;
	::close(fd);
	if (rc != 0 && syncErrno != EINVAL && syncErrno != ENOTSUP && syncErrno != EOPNOTSUPP) {
		return rocksdb::Status::IOError("Failed to sync backup log directory", path.string());
	}
#endif
	return rocksdb::Status::OK();
}

/**
 * Copies exactly `byteLimit` bytes from `src` to `dst`, then stamps `dst` with
 * `mtime`. Copying a bounded prefix (rather than the whole file) is what lets us
 * snapshot the current, actively-appended log file to a consistent extent.
 */
static rocksdb::Status copyPrefixWithMtime(
	const std::filesystem::path& src,
	const std::filesystem::path& dst,
	uint64_t byteLimit,
	std::filesystem::file_time_type mtime
) {
	std::ifstream in(src, std::ios::binary);
	if (!in) {
		return rocksdb::Status::IOError("Failed to open transaction log file", src.string());
	}

	std::ofstream out(dst, std::ios::binary | std::ios::trunc);
	if (!out) {
		return rocksdb::Status::IOError("Failed to create backup transaction log file", dst.string());
	}

	char buffer[1 << 16];
	uint64_t remaining = byteLimit;
	while (remaining > 0) {
		std::streamsize toRead =
			static_cast<std::streamsize>(std::min<uint64_t>(remaining, sizeof(buffer)));
		in.read(buffer, toRead);
		if (in.bad()) {
			// badbit is a genuine read I/O failure (e.g. disk error), distinct from
			// hitting EOF early — report it as such rather than as corruption.
			return rocksdb::Status::IOError("Failed to read transaction log file", src.string());
		}
		std::streamsize got = in.gcount();
		if (got <= 0) {
			// Source shorter than the recorded extent — the tar/backup contract
			// requires exactly `byteLimit` bytes, so a short read is a hard error
			// (a vanished source is handled by the caller's existence recheck).
			return rocksdb::Status::Corruption("Transaction log file shorter than recorded size", src.string());
		}
		out.write(buffer, got);
		if (!out) {
			return rocksdb::Status::IOError("Failed to write backup transaction log file", dst.string());
		}
		remaining -= static_cast<uint64_t>(got);
	}
	out.close();
	if (!out) {
		return rocksdb::Status::IOError("Failed to flush backup transaction log file", dst.string());
	}

	// Preserve the source mtime: the store derives file age (rotation/retention)
	// from mtime, so a fresh mtime on a restored file would break retention.
	std::error_code ec;
	std::filesystem::last_write_time(dst, mtime, ec);
	if (ec) {
		return rocksdb::Status::IOError("Failed to preserve transaction log mtime", dst.string());
	}
	return rocksdb::Status::OK();
}

/**
 * Writes `contents` to `dst` and stamps it with `mtime`. Used for entries whose
 * bytes were captured inline at snapshot time (txn.state).
 */
static rocksdb::Status writeBytesWithMtime(
	const std::filesystem::path& dst,
	const std::string& contents,
	std::filesystem::file_time_type mtime
) {
	std::ofstream out(dst, std::ios::binary | std::ios::trunc);
	if (!out) {
		return rocksdb::Status::IOError("Failed to create backup transaction log file", dst.string());
	}
	out.write(contents.data(), static_cast<std::streamsize>(contents.size()));
	out.close();
	if (!out) {
		return rocksdb::Status::IOError("Failed to write backup transaction log file", dst.string());
	}
	std::error_code ec;
	std::filesystem::last_write_time(dst, mtime, ec);
	if (ec) {
		return rocksdb::Status::IOError("Failed to preserve transaction log mtime", dst.string());
	}
	return rocksdb::Status::OK();
}

/**
 * Copies every snapshot entry into `destBaseDir` (`<destBaseDir>/<store>/<file>`),
 * fsyncing each file and every created directory when `sync` is set. Directory
 * syncs run after all files are written so the whole subtree is durable before
 * the caller publishes it.
 */
static rocksdb::Status copySnapshotEntries(
	const std::filesystem::path& destBaseDir,
	const std::vector<NamedTransactionLogBackupEntry>& entries,
	bool sync
) {
	std::set<std::filesystem::path> createdDirs;
	for (const auto& named : entries) {
		std::filesystem::path destDir = destBaseDir / named.storeName;
		std::error_code ec;
		std::filesystem::create_directories(destDir, ec);
		if (ec) {
			return rocksdb::Status::IOError("Failed to create backup log directory", destDir.string());
		}
		createdDirs.insert(destDir);

		std::filesystem::path dst = destDir / named.file.relativeName;

		// Entries captured inline (txn.state) are written straight from memory.
		if (!named.file.inlineContents.empty()) {
			rocksdb::Status s = writeBytesWithMtime(dst, named.file.inlineContents, named.file.mtime);
			if (s.ok() && sync) {
				s = syncFile(dst);
			}
			if (!s.ok()) {
				return s;
			}
			continue;
		}

		if (named.file.immutable) {
			// Rotated files are immutable: hard-link to share the inode (which also
			// preserves mtime for free and survives the live store's own purge).
			std::error_code linkEc;
			std::filesystem::create_hard_link(named.file.sourcePath, dst, linkEc);
			if (!linkEc) {
				if (sync) {
					// The link shares the source inode; fsync makes the shared data
					// durable (the log store may not have synced it yet).
					rocksdb::Status s = syncFile(dst);
					if (!s.ok()) {
						return s;
					}
				}
				continue;
			}
			// Hard link unavailable — typically EXDEV (destination on a different
			// filesystem/volume, e.g. a mounted network share) or ENOTSUP (a
			// filesystem without hard links, e.g. FAT/exFAT/ReFS on Windows). A
			// link failure must never fail the backup: fall through to a byte copy.
		}

		// Byte-by-byte copy of the captured extent: exactly `byteLimit` bytes —
		// the whole file for a rotated file, or the stable prefix of the current
		// file (a concurrent append may be extending it past the snapshot).
		rocksdb::Status s =
			copyPrefixWithMtime(named.file.sourcePath, dst, named.file.byteLimit, named.file.mtime);
		if (!s.ok()) {
			// A concurrent retention purge can unlink a rotated file between the
			// snapshot and this copy. An expiring file dropped from the backup is
			// fine, so skip it — removing the partial destination the failed copy
			// left behind; only a genuine failure (source still present) aborts.
			std::error_code existsEc;
			if (!std::filesystem::exists(named.file.sourcePath, existsEc) || existsEc) {
				std::error_code removeEc;
				std::filesystem::remove(dst, removeEc);
				continue;
			}
			return s;
		}
		if (sync) {
			s = syncFile(dst);
			if (!s.ok()) {
				return s;
			}
		}
	}

	if (sync) {
		for (const auto& dir : createdDirs) {
			rocksdb::Status s = syncDirectory(dir);
			if (!s.ok()) {
				return s;
			}
		}
		if (!createdDirs.empty()) {
			rocksdb::Status s = syncDirectory(destBaseDir);
			if (!s.ok()) {
				return s;
			}
		}
	}
	return rocksdb::Status::OK();
}

void removeStaleTransactionLogStaging(const std::filesystem::path& logsRoot) {
	std::error_code ec;
	std::filesystem::directory_iterator it(logsRoot, ec);
	if (ec) {
		return; // logsRoot missing (no logs ever backed up) — nothing to sweep
	}
	for (const auto& entry : it) {
		if (entry.path().filename().string().rfind(TRANSACTION_LOG_STAGING_PREFIX, 0) == 0) {
			std::error_code removeEc;
			std::filesystem::remove_all(entry.path(), removeEc);
		}
	}
}

rocksdb::Status backupTransactionLogsToDir(
	DBDescriptor* descriptor,
	const std::filesystem::path& destBaseDir,
	bool sync
) {
	auto entries = collectTransactionLogBackupEntries(descriptor);
	if (entries.empty()) {
		// No logs to snapshot: leave no directory at all — restore treats an
		// absent `<backupId>` subtree as "this backup captured no logs".
		return rocksdb::Status::OK();
	}

	// Stage under a sibling name that can never collide with a backup id, then
	// publish with a single atomic rename(). RocksDB durably registers the
	// backup id (making it listed and "restorable") before this snapshot runs,
	// so writing directly to the final path would let a crash mid-copy leave a
	// silently partial subtree that a later restore copies without complaint.
	// With the rename barrier the final path either has the complete snapshot
	// or nothing.
	std::filesystem::path stagingDir =
		destBaseDir.parent_path() / (TRANSACTION_LOG_STAGING_PREFIX + destBaseDir.filename().string());

	rocksdb::Status status = copySnapshotEntries(stagingDir, entries, sync);

	if (status.ok()) {
		std::error_code ec;
		std::filesystem::rename(stagingDir, destBaseDir, ec);
		if (ec) {
			status = rocksdb::Status::IOError(
				"Failed to publish transaction log snapshot: " + ec.message(), destBaseDir.string());
		} else if (sync) {
			// The rename is directory metadata: persist it, or a crash could roll
			// the published snapshot back to its (invisible) staging name. The
			// staged subtree itself was already synced by copySnapshotEntries.
			status = syncDirectory(destBaseDir.parent_path());
		}
	}

	if (!status.ok()) {
		std::error_code removeEc;
		std::filesystem::remove_all(stagingDir, removeEc);
	}
	return status;
}

} // namespace rocksdb_js
