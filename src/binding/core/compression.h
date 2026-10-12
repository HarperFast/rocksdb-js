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

/**
 * Decodes `TableProperties::compression_name` into the compression types an SST
 * records. Accepts both encodings RocksDB writes: the `format_version >= 7`
 * form `<compatibility_name>;<hex-coded types>;...` and the older single
 * built-in name (e.g. "LZ4"). Returns `std::nullopt` for anything else,
 * including a compatibility name other than the built-in manager's, because a
 * table that names a compression manager this build does not have cannot be
 * read by it.
 */
std::optional<std::vector<rocksdb::CompressionType>> tableCompressionTypes(const std::string& compressionName);

} // namespace rocksdb_js

#endif
