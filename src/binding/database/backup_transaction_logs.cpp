#include "database/backup_transaction_logs.h"
#include "core/platform.h"
#include "database/db_descriptor.h"
#include "database/flush_capture_gate.h"
#include "transaction_log/transaction_log_store_registry.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <thread>
#include <fstream>
#include <set>

namespace rocksdb_js {

rocksdb::Status captureLiveFilesWithTransactionLogs(
	DBDescriptor* descriptor,
	rocksdb::DB* db,
	const rocksdb::LiveFilesStorageInfoOptions& opts,
	std::vector<rocksdb::LiveFileStorageInfo>* files,
	std::vector<CapturedTransactionLogStore>& captured,
	const std::function<bool()>& abandon
) {
	captured.clear();
	files->clear();
	constexpr uint64_t noFlush = std::numeric_limits<uint64_t>::max();
	if (opts.wal_size_for_flush != noFlush) {
		// Only for RocksDB's flush of every column family; the list is taken below.
		rocksdb::Status s = db->GetLiveFilesStorageInfo(opts, files);
		files->clear();
		if (!s.ok()) {
			return s;
		}
	}
	rocksdb::LiveFilesStorageInfoOptions listOpts = opts;
	listOpts.wal_size_for_flush = noFlush;

	FlushCaptureGate* gate = descriptor->flushCaptureGate();
	std::unique_lock<std::mutex> serialized;
	std::optional<FlushCaptureHold> hold;
	if (gate) {
		serialized = std::unique_lock<std::mutex>(gate->captureMutex);
		hold.emplace(*gate);
	}

	// A running flush that is not parked may have installed files whose position
	// its OnFlushCompleted has not recorded yet. Parked ones have installed
	// nothing. Parked flushes are a subset of running ones and only grow while
	// the gate is held, so reading them first makes `running <= parked` exact.
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes(1);
	for (;;) {
		if (descriptor->isClosing() || (abandon && abandon())) {
			return rocksdb::Status::Aborted("Database closed during backup");
		}
		const uint32_t parked = hold ? hold->parkedFlushes() : 0;
		uint64_t running = 0;
		if (!db->GetIntProperty(rocksdb::DB::Properties::kNumRunningFlushes, &running)) {
			return rocksdb::Status::NotSupported("Unable to read rocksdb.num-running-flushes");
		}
		if (running <= parked) {
			break;
		}
		if (std::chrono::steady_clock::now() >= deadline) {
			return rocksdb::Status::Busy(
				"Backup could not capture a consistent transaction log position: flushes kept running for a minute"
			);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}

	try {
		for (const auto& store : TransactionLogStoreRegistry::GetStores(descriptor->identityPath)) {
			if (!store) {
				continue;
			}
			auto retentionPin = TransactionLogStore::pinRetention(store);
			captured.push_back({ store, std::move(retentionPin), store->captureFlushedStateForBackup() });
		}
	} catch (const std::exception& e) {
		captured.clear();
		return rocksdb::Status::IOError("Failed to capture transaction log state", e.what());
	}
	rocksdb::Status s = db->GetLiveFilesStorageInfo(listOpts, files);
	if (!s.ok()) {
		captured.clear();
	}
	return s;
}

rocksdb::Status collectTransactionLogBackupEntries(
	DBDescriptor* descriptor,
	std::vector<NamedTransactionLogBackupEntry>& entries,
	const std::function<bool()>& abandon,
	bool waitForUnresolved,
	const std::vector<CapturedTransactionLogStore>* captured
) {
	if (descriptor == nullptr) {
		return rocksdb::Status::OK();
	}

	try {
		auto stores = TransactionLogStoreRegistry::GetStores(descriptor->identityPath);
		auto closing = [descriptor, &abandon]() { return descriptor->isClosing() || (abandon && abandon()); };
		if (captured) {
			for (const auto& capture : *captured) {
				// A store destroyed since the capture is left out, as it would have been
				// had the backup started after the destroy.
				if (std::find(stores.begin(), stores.end(), capture.store) == stores.end()) {
					continue;
				}
				for (auto& file : capture.store->snapshotForBackup(closing, waitForUnresolved, &capture.flushedState)) {
					entries.push_back({ capture.store->name, std::move(file), capture.retentionPin });
				}
			}
		} else {
			for (const auto& store : stores) {
				if (!store) {
					continue;
				}
				auto retentionPin = TransactionLogStore::pinRetention(store);
				for (auto& file : store->snapshotForBackup(closing, waitForUnresolved)) {
					entries.push_back({ store->name, std::move(file), retentionPin });
				}
			}
		}
	} catch (const std::exception& e) {
		return rocksdb::Status::IOError("Failed to snapshot transaction log metadata", e.what());
	} catch (...) {
		return rocksdb::Status::IOError("Failed to snapshot transaction log metadata", "unknown error");
	}
	return rocksdb::Status::OK();
}

/**
 * Flushes a written file's data (and metadata) to stable storage. Honors the
 * backup's `sync` option: matches the durability RocksDB's own backup engine
 * gives the engine files, so the transaction log payload cannot be silently
 * less durable than the rest of the same backup.
 */
static rocksdb::Status syncBackupFile(const std::filesystem::path& path) {
	if (!rocksdb_js::syncFile(path)) {
		return rocksdb::Status::IOError("Failed to sync backup transaction log file", path.string());
	}
	return rocksdb::Status::OK();
}

/**
 * Flushes a directory's entries (file creations, hard links, renames) to
 * stable storage. A file's own fsync does not persist its directory entry, so
 * without this a crash could durably keep the bytes but lose the name.
 */
static rocksdb::Status syncBackupDirectory(const std::filesystem::path& path) {
	// A filesystem that cannot sync a directory forfeits the guarantee rather than
	// making backups impossible, as the backup lock does where `flock` is unsupported.
	if (!rocksdb_js::syncDirectory(path, /*allowUnsupported=*/true)) {
		return rocksdb::Status::IOError("Failed to sync backup log directory", path.string());
	}
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

std::string backupInlineContents(
	const NamedTransactionLogBackupEntry& named, const std::set<std::string>& storesMissingSegments
) {
	TransactionLogPurgeRecord record;
	if (named.file.relativeName != TRANSACTION_LOG_PURGE_RECORD_NAME ||
		!storesMissingSegments.contains(named.storeName) ||
		decodeTransactionLogPurgeRecordImage(named.file.inlineContents.data(), named.file.inlineContents.size(), record) !=
			TransactionLogPurgeRecordState::Valid) {
		return named.file.inlineContents;
	}
	record.loseCoverage();
	return encodeTransactionLogPurgeRecordImage(record);
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
	std::set<std::string> storesMissingSegments;
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
			rocksdb::Status s = writeBytesWithMtime(dst, backupInlineContents(named, storesMissingSegments), named.file.mtime);
			if (s.ok() && sync) {
				s = syncBackupFile(dst);
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
					rocksdb::Status s = syncBackupFile(dst);
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
			// The entries pin retention, but a destroy or an out-of-band delete can
			// still unlink a file between the snapshot and this copy. Skip it,
			// removing the partial destination the failed copy left behind; only a
			// genuine failure (source still present) aborts.
			std::error_code existsEc;
			if (!std::filesystem::exists(named.file.sourcePath, existsEc) || existsEc) {
				std::error_code removeEc;
				std::filesystem::remove(dst, removeEc);
				storesMissingSegments.insert(named.storeName);
				continue;
			}
			return s;
		}
		if (sync) {
			s = syncBackupFile(dst);
			if (!s.ok()) {
				return s;
			}
		}
	}

	if (sync) {
		for (const auto& dir : createdDirs) {
			rocksdb::Status s = syncBackupDirectory(dir);
			if (!s.ok()) {
				return s;
			}
		}
		if (!createdDirs.empty()) {
			rocksdb::Status s = syncBackupDirectory(destBaseDir);
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
	bool sync,
	const std::function<bool()>& abandon,
	const std::vector<CapturedTransactionLogStore>* captured
) {
	std::vector<NamedTransactionLogBackupEntry> entries;
	rocksdb::Status status = collectTransactionLogBackupEntries(descriptor, entries, abandon, true, captured);
	if (!status.ok()) {
		return status;
	}
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

	status = copySnapshotEntries(stagingDir, entries, sync);

	if (status.ok()) {
		std::error_code ec;
		std::filesystem::rename(stagingDir, destBaseDir, ec);
		if (ec) {
			status = rocksdb::Status::IOError(
				"Failed to publish transaction log snapshot: " + ec.message(), destBaseDir.string()
			);
		} else if (sync) {
			// The rename is directory metadata: persist it, or a crash could roll
			// the published snapshot back to its (invisible) staging name. The
			// staged subtree itself was already synced by copySnapshotEntries.
			status = syncBackupDirectory(destBaseDir.parent_path());
		}
	}

	if (!status.ok()) {
		std::error_code removeEc;
		std::filesystem::remove_all(stagingDir, removeEc);
	}
	return status;
}

} // namespace rocksdb_js
