#ifndef __CORE_OPTIONS_FILE_H__
#define __CORE_OPTIONS_FILE_H__

#include <optional>
#include <string>

namespace rocksdb_js {

/**
 * Finds the first non-empty `blob_dir=` value in the text of a RocksDB OPTIONS
 * file, or `std::nullopt` when no column family records one.
 *
 * Text rather than `LoadLatestOptions` because the only caller is a build
 * WITHOUT the downstream `blob_dir` patch: its `ConfigOptions` parser has no
 * such field and silently drops the line, so the parsed options cannot show
 * what the file says. That caller is the guard stopping an unpatched build from
 * opening a database whose large values live on another volume — the whole
 * point is to see a field this build does not understand.
 */
std::optional<std::string> findPersistedBlobDir(const std::string& optionsFileContents);

}

#endif
