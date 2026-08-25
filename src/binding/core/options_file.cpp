#include "core/options_file.h"

namespace rocksdb_js {

std::optional<std::string> findPersistedBlobDir(const std::string& optionsFileContents) {
	static const std::string key = "blob_dir=";
	for (size_t pos = optionsFileContents.find(key); pos != std::string::npos;
		pos = optionsFileContents.find(key, pos + key.size())
	) {
		// Only whitespace may precede it on its line: anything else means this is
		// the tail of a longer key (a future `wal_blob_dir=`) or a commented-out
		// line. RocksDB never writes comments, but a hand-edited file would
		// otherwise make an unpatched build refuse an untiered database, naming a
		// directory nothing was ever configured with.
		size_t lineStart = optionsFileContents.rfind('\n', pos);
		lineStart = lineStart == std::string::npos ? 0 : lineStart + 1;
		bool indentOnly = true;
		for (size_t i = lineStart; i < pos; i++) {
			const char c = optionsFileContents[i];
			if (c != ' ' && c != '\t' && c != '\r') {
				indentOnly = false;
				break;
			}
		}
		if (!indentOnly) {
			continue;
		}
		const size_t valueStart = pos + key.size();
		const size_t valueEnd = optionsFileContents.find_first_of("\r\n", valueStart);
		std::string value = optionsFileContents.substr(
			valueStart,
			valueEnd == std::string::npos ? std::string::npos : valueEnd - valueStart
		);
		// An unset field is written as `blob_dir=`; keep looking, since a later
		// column family may have one.
		if (!value.empty()) {
			return value;
		}
	}
	return std::nullopt;
}

}
