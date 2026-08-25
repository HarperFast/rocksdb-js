#ifndef __CORE_BLOB_RELOCATION_H__
#define __CORE_BLOB_RELOCATION_H__

#include <functional>
#include <optional>
#include <string>

namespace rocksdb_js {

/**
 * One column family's state as the cold open sees it, before this open's
 * `blobs.*` request has been allowed to reach it.
 */
struct BlobRelocationInput {
	/** The database directory, used to name the database in the messages. */
	std::string dbPath;
	/**
	 * Where an empty `blob_dir` actually puts blob files: `paths[0]` when the
	 * database is opened with storage paths, the database directory otherwise.
	 * RocksDB derives every blob file path from `cf_paths.front()`, which falls
	 * back to `db_paths.front()`, and the `blob_dir` patch keeps that as the
	 * empty-value behavior — so a tiered database's flat blob files sit on
	 * `paths[0]` rather than beside the OPTIONS file. Empty means `dbPath`.
	 */
	std::string defaultBlobDir;
	/** The family being decided. */
	std::string cfName;
	/** Whether `cfName` is the family this open names (`options.name`). */
	bool isTarget = false;
	/**
	 * The `blob_dir` this family recorded in the OPTIONS file, or `nullopt` when
	 * the family is not on disk yet. Engaged-but-empty means `defaultBlobDir`,
	 * which is what every family that never moved its blob files records.
	 */
	std::optional<std::string> persistedBlobDir;
	/**
	 * The same field for the open's TARGET family. Disengaged when the target is
	 * not on disk yet, in which case nothing moved out from under it and no
	 * other family can be said to have moved with it.
	 */
	std::optional<std::string> targetPersistedBlobDir;
	/** `blobs.dir` as requested by this open; empty means "flatten". */
	std::string requestedDir;
	/** `blobs.allowDirChange` as requested by this open. */
	bool allowDirChange = false;
	/**
	 * Where this family's blob files would go if the decision changed nothing:
	 * the persisted value restored onto the family, with the target's explicit
	 * request already applied.
	 */
	std::string currentBlobDir;
};

/** What the open should do with one family's `blob_dir`. */
struct BlobRelocationDecision {
	/** Non-empty means refuse the open with this message. */
	std::string error;
	/** The `blob_dir` to open the family with, when `error` is empty. */
	std::string blobDir;
};

/**
 * Decides one column family's blob directory for a cold open, and whether the
 * open may proceed at all.
 *
 * Node-free and free of the `blob_dir` field itself so a GoogleTest can cover
 * it: the call site compiles only into a build WITH the downstream `blob_dir`
 * patch (`ROCKSDB_HAS_CF_BLOB_DIR`), and no prebuild carries that patch yet, so
 * every integration test of these rules skips on every build that exists today.
 * The same reasoning that put the unpatched-build OPTIONS scan in
 * `core/options_file.cpp` applies here in mirror image.
 *
 * `holdsBlobFiles` reports whether a directory currently contains `.blob`
 * files; `directoryExists` reports whether a directory is present. Both are
 * injected so the rules can be exercised without a filesystem.
 */
BlobRelocationDecision decideBlobRelocation(
	const BlobRelocationInput& input,
	const std::function<bool(const std::string&)>& holdsBlobFiles,
	const std::function<bool(const std::string&)>& directoryExists
);

}

#endif
