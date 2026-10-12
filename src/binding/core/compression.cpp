#include "core/compression.h"
#include <algorithm>
#include <cctype>
#include <memory>
#include "rocksdb/advanced_compression.h"
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

namespace {

int hexDigitValue(char c) {
	if (c >= '0' && c <= '9') {
		return c - '0';
	}
	if (c >= 'a' && c <= 'f') {
		return c - 'a' + 10;
	}
	if (c >= 'A' && c <= 'F') {
		return c - 'A' + 10;
	}
	return -1;
}

} // namespace

std::optional<std::vector<rocksdb::CompressionType>> tableCompressionTypes(const std::string& compressionName) {
	const std::shared_ptr<rocksdb::CompressionManager>& builtin = rocksdb::GetBuiltinV2CompressionManager();
	std::vector<rocksdb::CompressionType> types;

	size_t compatEnd = compressionName.find(';');
	if (compatEnd == std::string::npos) {
		for (unsigned char t = rocksdb::kNoCompression; t <= rocksdb::kLastBuiltinCompression; ++t) {
			auto type = static_cast<rocksdb::CompressionType>(t);
			if (compressionName == builtin->CompressionTypeToString(type)) {
				if (type != rocksdb::kNoCompression) {
					types.push_back(type);
				}
				return types;
			}
		}
		// Written by RocksDB releases that predate the finalized zstd format.
		if (compressionName == "ZSTDNotFinal") {
			types.push_back(rocksdb::kZSTD);
			return types;
		}
		return std::nullopt;
	}

	// An empty compatibility name means compression was disabled for the file.
	std::string compat = compressionName.substr(0, compatEnd);
	if (!compat.empty() && compat != builtin->CompatibilityName()) {
		return std::nullopt;
	}

	size_t hexStart = compatEnd + 1;
	size_t hexEnd = compressionName.find(';', hexStart);
	if (hexEnd == std::string::npos) {
		return std::nullopt;
	}
	if ((hexEnd - hexStart) % 2 != 0) {
		return std::nullopt;
	}
	for (size_t i = hexStart; i < hexEnd; i += 2) {
		int hi = hexDigitValue(compressionName[i]);
		int lo = hexDigitValue(compressionName[i + 1]);
		if (hi < 0 || lo < 0) {
			return std::nullopt;
		}
		types.push_back(static_cast<rocksdb::CompressionType>((hi << 4) | lo));
	}
	return types;
}

} // namespace rocksdb_js
