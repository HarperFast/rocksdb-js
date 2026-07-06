#include "database/backup_transaction_logs.h"
#include "database/db_descriptor.h"
#include "transaction_log/transaction_log_store_registry.h"
#include <algorithm>
#include <cstdint>
#include <fstream>

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

rocksdb::Status backupTransactionLogsToDir(
	DBDescriptor* descriptor,
	const std::filesystem::path& destBaseDir
) {
	auto entries = collectTransactionLogBackupEntries(descriptor);
	for (const auto& named : entries) {
		std::filesystem::path destDir = destBaseDir / named.storeName;
		std::error_code ec;
		std::filesystem::create_directories(destDir, ec);
		if (ec) {
			return rocksdb::Status::IOError("Failed to create backup log directory", destDir.string());
		}

		std::filesystem::path dst = destDir / named.file.relativeName;

		// Entries captured inline (txn.state) are written straight from memory.
		if (!named.file.inlineContents.empty()) {
			rocksdb::Status s = writeBytesWithMtime(dst, named.file.inlineContents, named.file.mtime);
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
				continue;
			}
			// Hard link unavailable (cross-filesystem, or platform without support);
			// fall through to a byte copy.
		}

		rocksdb::Status s =
			copyPrefixWithMtime(named.file.sourcePath, dst, named.file.byteLimit, named.file.mtime);
		if (!s.ok()) {
			// A concurrent retention purge can unlink a rotated file between the
			// snapshot and this copy. An expiring file dropped from the backup is
			// fine, so skip it; only a genuine failure (source still present) aborts.
			std::error_code existsEc;
			if (!std::filesystem::exists(named.file.sourcePath, existsEc) || existsEc) {
				continue;
			}
			return s;
		}
	}
	return rocksdb::Status::OK();
}

} // namespace rocksdb_js
