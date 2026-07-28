#include "core/compression.h"
#include <algorithm>
#include <cctype>
#include "rocksdb/convenience.h"

namespace rocksdb_js {

namespace {

struct CompressionEntry {
	const char* name;
	rocksdb::CompressionType type;
};

// Friendly names for the built-in RocksDB compression algorithms. The names
// are the public API surface; the enum values are the persisted on-disk format
// and must not change. Custom compressors (0x80+) are intentionally omitted.
constexpr CompressionEntry kCompressionTable[] = {
	{ "none", rocksdb::kNoCompression },
	{ "snappy", rocksdb::kSnappyCompression },
	{ "zlib", rocksdb::kZlibCompression },
	{ "bzip2", rocksdb::kBZip2Compression },
	{ "lz4", rocksdb::kLZ4Compression },
	{ "lz4hc", rocksdb::kLZ4HCCompression },
	{ "xpress", rocksdb::kXpressCompression },
	{ "zstd", rocksdb::kZSTD },
};

} // namespace

std::optional<rocksdb::CompressionType> compressionTypeFromName(const std::string& name) {
	std::string lowered = name;
	std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char c) {
		return static_cast<char>(std::tolower(c));
	});
	for (const auto& entry : kCompressionTable) {
		if (lowered == entry.name) {
			return entry.type;
		}
	}
	return std::nullopt;
}

std::string compressionNameFromType(rocksdb::CompressionType type) {
	for (const auto& entry : kCompressionTable) {
		if (entry.type == type) {
			return entry.name;
		}
	}
	return "unknown";
}

bool isCompressionSupported(rocksdb::CompressionType type) {
	if (type == rocksdb::kNoCompression) {
		return true;
	}
	const std::vector<rocksdb::CompressionType>& supported = rocksdb::GetSupportedCompressions();
	return std::find(supported.begin(), supported.end(), type) != supported.end();
}

const std::vector<std::string>& supportedCompressionNames() {
	// Computed once: the linked RocksDB's compiled-in compressors don't change
	// over the process lifetime.
	static const std::vector<std::string> names = [] {
		std::vector<std::string> result;
		for (rocksdb::CompressionType type : rocksdb::GetSupportedCompressions()) {
			std::string name = compressionNameFromType(type);
			if (name != "unknown") {
				result.push_back(name);
			}
		}
		return result;
	}();
	return names;
}

} // namespace rocksdb_js
