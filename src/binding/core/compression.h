#ifndef __CORE_COMPRESSION_H__
#define __CORE_COMPRESSION_H__

#include "rocksdb/options.h"
#include <string>

namespace rocksdb_js {

/**
 * Maps a lowercase algorithm name to its `CompressionType`. Returns false with
 * `error` set if the name is unknown or its compressor was not linked in.
 * RocksDB degrades silently in that case, so it has to be caught here.
 */
bool tryResolveCompressionType(
	const std::string& name,
	rocksdb::CompressionType& type,
	std::string& error
);

/** Comma-separated names usable in this build, for error messages. */
std::string supportedCompressionNames();

} // namespace rocksdb_js

#endif
