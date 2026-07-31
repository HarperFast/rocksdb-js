#ifndef __CORE_COMPRESSION_H__
#define __CORE_COMPRESSION_H__

#include <optional>
#include <string>
#include <vector>
#include "rocksdb/compression_type.h"

namespace rocksdb_js {

/**
 * Maps a friendly compression name (e.g. "lz4", "zstd", "none") to a RocksDB
 * `CompressionType`. Case-insensitive. Returns `std::nullopt` for an unknown
 * name.
 */
std::optional<rocksdb::CompressionType> compressionTypeFromName(const std::string& name);

/**
 * Maps a RocksDB `CompressionType` back to its friendly name (the inverse of
 * `compressionTypeFromName`). Returns "unknown" for a type this binding does
 * not surface (e.g. custom compressors).
 */
std::string compressionNameFromType(rocksdb::CompressionType type);

/**
 * Whether the given compression type is compiled into the linked RocksDB
 * build. `kNoCompression` is always supported.
 */
bool isCompressionSupported(rocksdb::CompressionType type);

/**
 * The friendly names of every compression algorithm compiled into the linked
 * RocksDB build, derived from `rocksdb::GetSupportedCompressions()`. The set is
 * fixed for a given binary, so the result is computed once and cached.
 */
const std::vector<std::string>& supportedCompressionNames();

} // namespace rocksdb_js

#endif
