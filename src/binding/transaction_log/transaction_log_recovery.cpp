#include "transaction_log/transaction_log_recovery.h"
#include "transaction_log/transaction_log_file.h" // header-size constants
#include "core/encoding.h"                         // readDoubleBE / readUint32BE

namespace rocksdb_js {

namespace {

// Number of consecutive well-formed frames that, on their own, signal that real
// log data has resumed after a framing break. Combined with the "chain reaches
// EOF" signal below, this distinguishes mid-file corruption (valid entries still
// follow — must NOT truncate) from a torn tail (only partial bytes follow).
constexpr int RESYNC_MIN_FRAMES = 8;

// Returns true if a complete, in-bounds frame begins at `pos`. The only sane
// bound on an entry's length is the physical file size: a single entry can
// legitimately exceed the rotation threshold (the first entry written to a fresh
// file is always written in full), so maxFileSize must NOT be used as a cap. A
// zero timestamp is an end-of-entries marker, not a frame.
inline bool frameFits(const char* data, uint32_t pos, uint32_t fileSize) {
	if (static_cast<uint64_t>(pos) + TRANSACTION_LOG_ENTRY_HEADER_SIZE > fileSize) {
		return false;
	}
	if (readDoubleBE(data + pos) == 0) {
		return false;
	}
	uint32_t length = readUint32BE(data + pos + 8);
	if (length == 0) {
		return false;
	}
	return static_cast<uint64_t>(pos) + TRANSACTION_LOG_ENTRY_HEADER_SIZE + length <= fileSize;
}

// Returns true if valid log data resumes at some offset in [from, fileSize):
// either a run of at least RESYNC_MIN_FRAMES well-formed frames, or any run that
// lands exactly on EOF. Random bytes aligning a length-chain exactly to EOF is
// ~1/2^32, so even a short run that hits EOF is a reliable resume signal — and it
// is what protects a mid-file break followed by fewer than RESYNC_MIN_FRAMES
// committed entries from being truncated away. Effectively linear: non-resyncing
// offsets fail after ~one frame check; only a true resume point walks a chain.
bool validFramingResumes(const char* data, uint32_t from, uint32_t fileSize) {
	for (uint32_t start = from; static_cast<uint64_t>(start) + TRANSACTION_LOG_ENTRY_HEADER_SIZE <= fileSize;
		 ++start) {
		uint32_t pos = start;
		int frames = 0;
		while (frameFits(data, pos, fileSize)) {
			pos += TRANSACTION_LOG_ENTRY_HEADER_SIZE + readUint32BE(data + pos + 8);
			if (++frames >= RESYNC_MIN_FRAMES || pos == fileSize) {
				return true;
			}
		}
	}
	return false;
}

} // namespace

RecoveryScan scanTransactionLogForRecovery(const char* data, uint32_t fileSize) {
	if (fileSize <= TRANSACTION_LOG_FILE_HEADER_SIZE) {
		return { RecoveryScan::Kind::Clean, fileSize };
	}

	uint32_t pos = TRANSACTION_LOG_FILE_HEADER_SIZE;
	while (true) {
		if (pos == fileSize) {
			// reached the end exactly on an entry boundary
			return { RecoveryScan::Kind::Clean, fileSize };
		}
		if (static_cast<uint64_t>(pos) + TRANSACTION_LOG_ENTRY_HEADER_SIZE > fileSize) {
			// fewer than a full entry header remains: a partial header at the
			// tail. Nothing valid can follow, so this is a torn tail.
			return { RecoveryScan::Kind::TruncateTail, pos };
		}
		if (readDoubleBE(data + pos) == 0) {
			// zero padding marks the end of entries (matches the reader/parser);
			// everything before it is valid.
			return { RecoveryScan::Kind::Clean, pos };
		}
		uint32_t length = readUint32BE(data + pos + 8);
		if (length == 0 ||
			static_cast<uint64_t>(pos) + TRANSACTION_LOG_ENTRY_HEADER_SIZE + length > fileSize) {
			// This frame is broken. If valid framing resumes after it, this is
			// mid-file corruption: truncating would drop committed entries that
			// are still framed, so leave it for the caller to surface. Otherwise
			// it is a torn tail we can safely drop back to `pos`.
			if (validFramingResumes(data, pos + 1, fileSize)) {
				return { RecoveryScan::Kind::MidFileCorruption, pos };
			}
			return { RecoveryScan::Kind::TruncateTail, pos };
		}
		pos += TRANSACTION_LOG_ENTRY_HEADER_SIZE + length;
	}
}

} // namespace rocksdb_js
