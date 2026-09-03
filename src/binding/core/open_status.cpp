#include "open_status.h"

#include <cctype>
#include <string>

namespace rocksdb_js {

// The extension must END a filename token (followed by a separator or the end
// of the string): a directory merely named "archive.log/" is followed by a
// path separator, so a genuine corruption inside such a path cannot ride the
// directory name into the race classification.
static bool namesFileWithExtension(const std::string& message, const char* extension) {
	const size_t extensionLength = std::char_traits<char>::length(extension);
	size_t offset = 0;
	while ((offset = message.find(extension, offset)) != std::string::npos) {
		const size_t end = offset + extensionLength;
		if (end == message.size()) {
			return true;
		}
		const char next = message[end];
		// Whitespace, quoting, or punctuation ends the filename token. `\r` and
		// `\t` matter on Windows, whose FormatMessage text this classifier
		// explicitly matches and which ends lines with CRLF.
		if (next == ':' || next == ' ' || next == '\'' || next == '"' ||
			next == ')' || next == ',' || next == ';' ||
			next == '\n' || next == '\r' || next == '\t'
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
	// The Corruption wrap is constructed fresh inside RocksDB, so its subcode is
	// lost and only the message carries the not-found signal.
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
