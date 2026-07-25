#include "core/compression.h"
#include "rocksdb/convenience.h"
#include <algorithm>
#include <vector>

namespace rocksdb_js {

namespace {

struct CompressionName final {
	const char* name;
	rocksdb::CompressionType type;
};

// The names the binding accepts; the TS `RocksDBCompression` union mirrors it.
constexpr CompressionName kCompressionNames[] = {
	{"none", rocksdb::kNoCompression},
	{"snappy", rocksdb::kSnappyCompression},
	{"zlib", rocksdb::kZlibCompression},
	{"bzip2", rocksdb::kBZip2Compression},
	{"lz4", rocksdb::kLZ4Compression},
	{"lz4hc", rocksdb::kLZ4HCCompression},
	{"zstd", rocksdb::kZSTD},
};

bool isLinkedIn(rocksdb::CompressionType type) {
	// GetSupportedCompressions() does not report kNoCompression.
	if (type == rocksdb::kNoCompression) {
		return true;
	}
	static const std::vector<rocksdb::CompressionType> supported = rocksdb::GetSupportedCompressions();
	return std::find(supported.begin(), supported.end(), type) != supported.end();
}

} // namespace

bool tryResolveCompressionType(
	const std::string& name,
	rocksdb::CompressionType& type,
	std::string& error
) {
	const CompressionName* match = nullptr;
	for (const auto& entry : kCompressionNames) {
		if (name == entry.name) {
			match = &entry;
			break;
		}
	}

	if (match == nullptr) {
		error = "Unknown compression type: \"" + name + "\". Available in this build: " + supportedCompressionNames();
		return false;
	}

	if (!isLinkedIn(match->type)) {
		error = "Compression type \"" + name +
			"\" is not supported by this RocksDB build. Available in this build: " + supportedCompressionNames();
		return false;
	}

	type = match->type;
	return true;
}

std::string supportedCompressionNames() {
	std::string result;
	for (const auto& entry : kCompressionNames) {
		if (!isLinkedIn(entry.type)) {
			continue;
		}
		if (!result.empty()) {
			result += ", ";
		}
		result += entry.name;
	}
	return result;
}

} // namespace rocksdb_js
