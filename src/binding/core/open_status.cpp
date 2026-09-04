#include "open_status.h"

#include <cctype>
#include <filesystem>
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

static std::string namedRocksFile(const std::string& message, const char* extension) {
	const size_t extensionLength = std::char_traits<char>::length(extension);
	size_t offset = 0;
	while ((offset = message.find(extension, offset)) != std::string::npos) {
		const size_t end = offset + extensionLength;
		if (end < message.size()) {
			const char next = message[end];
			if (next != ':' && next != ' ' && next != '\'' && next != '"' &&
				next != ')' && next != ',' && next != ';' &&
				next != '\n' && next != '\r' && next != '\t'
			) {
				offset = end;
				continue;
			}
		}
		size_t start = offset;
		while (start > 0 && std::isdigit(static_cast<unsigned char>(message[start - 1]))) {
			--start;
		}
		// RocksDB data files are a run of digits, not merely any filename
		// ending in digits. Require a token/path boundary before the run so
		// `archive123.sst` cannot be misread as the missing `123.sst`.
		const bool hasStartBoundary = start == 0 ||
			message[start - 1] == '/' || message[start - 1] == '\\' ||
			message[start - 1] == ':' || message[start - 1] == ' ' ||
			message[start - 1] == '\'' || message[start - 1] == '"' ||
			message[start - 1] == '(' || message[start - 1] == '\n' ||
			message[start - 1] == '\r' || message[start - 1] == '\t';
		if (start < offset && hasStartBoundary) {
			return message.substr(start, end - start);
		}
		offset += extensionLength;
	}
	return {};
}

bool isMissingSstOpenRace(
	const rocksdb::Status& status,
	const std::filesystem::path& databasePath
) {
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
	if (message.find("No such file or directory") != std::string::npos ||
		message.find("The system cannot find") != std::string::npos ||
		message.find("PathNotFound") != std::string::npos
	) {
		return true;
	}

	// A localized Corruption wrapper can lose the original PathNotFound subcode.
	if (!databasePath.empty()) {
		for (const char* extension : { ".sst", ".blob", ".log" }) {
			std::string filename = namedRocksFile(message, extension);
			if (filename.empty()) {
				continue;
			}
			std::error_code error;
			bool exists = std::filesystem::exists(databasePath / filename, error);
			if (!error && !exists) {
				return true;
			}
		}
	}
	return false;
}

} // namespace rocksdb_js
