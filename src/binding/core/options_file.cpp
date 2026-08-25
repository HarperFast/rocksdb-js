#include "core/options_file.h"

namespace rocksdb_js {

std::optional<std::string> findPersistedBlobDir(const std::string& optionsFileContents) {
	static const std::string key = "blob_dir=";
	for (size_t pos = optionsFileContents.find(key); pos != std::string::npos;
		pos = optionsFileContents.find(key, pos + key.size())
	) {
		// Must be a whole key rather than the tail of a longer one, or a future
		// `wal_blob_dir=` would be read as this field.
		if (pos > 0 && optionsFileContents[pos - 1] != '\n' &&
			optionsFileContents[pos - 1] != '\r' && optionsFileContents[pos - 1] != ' ' &&
			optionsFileContents[pos - 1] != '\t'
		) {
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
