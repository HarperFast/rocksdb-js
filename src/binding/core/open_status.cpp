#include "open_status.h"

#include <string>

namespace rocksdb_js {

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
	if (message.find(".sst") == std::string::npos &&
		message.find(".blob") == std::string::npos &&
		message.find(".log") == std::string::npos
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
