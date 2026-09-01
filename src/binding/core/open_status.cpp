#include "open_status.h"

#include <cctype>
#include <string>

namespace rocksdb_js {

// True when `extension` appears in `message` at the END of a filename token —
// followed by a separator (space, quote, colon, paren, comma) or the end of
// the string. A directory merely named like "archive.log/" or "exports.sst/"
// is followed by a path separator and does not match, so a genuine corruption
// inside such a path cannot ride the directory name into the race
// classification.
static bool namesFileWithExtension(const std::string& message, const char* extension) {
	const size_t extensionLength = std::char_traits<char>::length(extension);
	size_t offset = 0;
	while ((offset = message.find(extension, offset)) != std::string::npos) {
		const size_t end = offset + extensionLength;
		if (end == message.size()) {
			return true;
		}
		const char next = message[end];
		if (next == ':' || next == ' ' || next == '\'' || next == '"' ||
			next == ')' || next == ',' || next == '\n'
		) {
			return true;
		}
		offset = end;
	}
	return false;
}

bool isMissingSstOpenRace(const rocksdb::Status& status) {
	if (status.ok()) {
		return false;
	}
	// The race surfaces as an IOError (sometimes with the kPathNotFound
	// subcode) or as a Corruption status wrapping the IO error's text. The
	// Corruption wrap is constructed fresh inside RocksDB, so the subcode is
	// lost there and only the message carries the not-found signal.
	if (!status.IsIOError() && !status.IsCorruption()) {
		return false;
	}
	// The race can trip on any file class a live writer reclaims: SSTs
	// (compaction inputs), blob files (blob GC), or WAL segments (deleted by
	// flush — `.log` here is the numbered WAL, not the extensionless info LOG).
	const std::string message = status.ToString();
	if (!namesFileWithExtension(message, ".sst") &&
		!namesFileWithExtension(message, ".blob") &&
		!namesFileWithExtension(message, ".log")
	) {
		return false;
	}
	if (status.IsPathNotFound()) {
		return true;
	}
	// POSIX strerror text, Windows FormatMessage text ("The system cannot find
	// the file specified" / "... the path specified"), and RocksDB's own
	// subcode name as printed by Status::ToString.
	return message.find("No such file or directory") != std::string::npos ||
		message.find("The system cannot find") != std::string::npos ||
		message.find("PathNotFound") != std::string::npos;
}

} // namespace rocksdb_js
