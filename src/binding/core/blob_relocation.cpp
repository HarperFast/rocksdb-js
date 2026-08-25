#include "core/blob_relocation.h"

namespace rocksdb_js {

BlobRelocationDecision decideBlobRelocation(
	const BlobRelocationInput& input,
	const std::function<bool(const std::string&)>& holdsBlobFiles,
	const std::function<bool(const std::string&)>& directoryExists
) {
	// An empty `blob_dir` is not "no directory", it is wherever RocksDB puts
	// blob files without one: `paths[0]` on a tiered database, the database
	// directory otherwise.
	const std::string& defaultBlobDir =
		input.defaultBlobDir.empty() ? input.dbPath : input.defaultBlobDir;
	auto resolveBlobDir = [&defaultBlobDir](const std::string& dir) -> const std::string& {
		return dir.empty() ? defaultBlobDir : dir;
	};

	BlobRelocationDecision decision;
	decision.blobDir = input.currentBlobDir;

	// `allowDirChange` states where blob files that ALREADY moved now live, and
	// a move is not per-family: the files sitting in one directory move
	// together. So it reaches past the family this open names — but only as far
	// as the move actually went.
	//
	// Omitting `dir` says the whole database was flattened into its own
	// directory, which is what restoring a backup produces, so every family goes
	// flat. With a `dir`, only the families that shared the target's old
	// directory moved with it; one whose blobs were somewhere else keeps its
	// own, because re-pointing it would strand files that never moved. A
	// database with several distinct blob directories therefore needs one open
	// per directory — the option names a single destination, so it cannot
	// describe more than one move.
	//
	// Only `dir` reaches other families. The rest of `blobs.*` stays per-family
	// (invariant 15): none of it describes where files already are.
	const bool sharesTargetBlobDir = input.targetPersistedBlobDir && input.persistedBlobDir &&
		*input.persistedBlobDir == *input.targetPersistedBlobDir;
	const bool acknowledged = input.allowDirChange &&
		(input.requestedDir.empty() || input.isTarget || sharesTargetBlobDir);

	// The acknowledgement is a claim about files on disk, so it is checked
	// against them rather than trusted, and one rule covers both forms: a family
	// whose recorded directory still holds `.blob` files has not moved. Compared
	// against the PERSISTED directory, because the target's `currentBlobDir`
	// already carries the request by now, and skipped when nothing is actually
	// changing so a flag left in a config file does not start refusing every
	// open.
	//
	// Both sides resolve an empty directory: an empty `blob_dir` means the
	// database directory, not "no directory".
	//
	// Deliberately strict about a HALF-finished move: the destination holding
	// some files is not evidence, and neither is its being empty —
	// `ensureBlobDirExists` creates it.
	if (acknowledged && input.persistedBlobDir) {
		const std::string& from = resolveBlobDir(*input.persistedBlobDir);
		if (from != resolveBlobDir(input.requestedDir) && holdsBlobFiles(from)) {
			decision.error =
				"Cannot open \"" + input.dbPath + "\" with blobs.allowDirChange: column family \"" +
				input.cfName + "\" still has blob files in \"" + from +
				"\", the directory it recorded them in. Nothing is moved for you — finish "
				"moving them out of that directory before reopening. If this is a restored "
				"copy, that directory belongs to the database it was restored from and "
				"sharing it would corrupt both: restore where it is not reachable.";
			return decision;
		}
	}

	if (!input.isTarget && input.allowDirChange) {
		if (input.requestedDir.empty()) {
			decision.blobDir.clear();
		} else if (sharesTargetBlobDir) {
			decision.blobDir = input.requestedDir;
		}
	}

	// A blob file's directory is derived from blob_dir every time it is opened —
	// unlike an SST's path index, it is not recorded per file. Reopening with a
	// different directory therefore does not move the existing blob files, it
	// strands them: reads of every value >= min_blob_size fail with "No such
	// file or directory". Refuse rather than open a database whose large values
	// have silently gone missing, unless the caller says they have already moved
	// the files.
	if (input.isTarget && !input.allowDirChange && input.persistedBlobDir &&
		*input.persistedBlobDir != decision.blobDir
	) {
		decision.error =
			"Cannot reopen \"" + input.dbPath + "\" column family \"" + input.cfName +
			"\" with blobs.dir \"" + decision.blobDir + "\": its blob files were written to \"" +
			*input.persistedBlobDir + "\". Move the blob files to the new directory while the "
			"database is closed and reopen with blobs.allowDirChange, or reopen with the "
			"original blobs.dir.";
		return decision;
	}

	// A directory that is gone is not a soft failure: every read of a value >=
	// min_blob_size fails and the first flush errors the whole database
	// read-only, long after the open that could have named the cause. The target
	// family's directory was created by the caller before this point if it was
	// missing, so in practice this catches the families this open never named —
	// including a restored database still pointing at a source volume that is
	// not mounted here.
	if (!decision.blobDir.empty() && !directoryExists(decision.blobDir)) {
		decision.error =
			"Cannot open \"" + input.dbPath + "\": column family \"" + input.cfName +
			"\" keeps its blob files in \"" + decision.blobDir + "\", which does not exist. "
			"Mount or restore that directory, or move the blob files and reopen with "
			"blobs.dir set to their new location plus blobs.allowDirChange.";
		return decision;
	}

	return decision;
}

}
