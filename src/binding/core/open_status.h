#ifndef __CORE_OPEN_STATUS_H__
#define __CORE_OPEN_STATUS_H__

#include <filesystem>
#include "rocksdb/status.h"

namespace rocksdb_js {

/**
 * Classifies a failed `DB::OpenForReadOnly`/`DB::OpenAsSecondary` status as the
 * missing-file open race: the open's MANIFEST/WAL replay named a file that was
 * gone by the time the open tried to read it. Against a database another
 * process is actively writing, that is the writer reclaiming an obsolete file
 * mid-open — a compaction deleting input SSTs, blob GC deleting rewritten
 * `.blob` files (values >= 2KB live there in this codebase), or a flush
 * deleting a consumed WAL segment (`.log`) — the database is healthy — but
 * RocksDB reports it either as a Corruption status blaming the MANIFEST ("The
 * file MANIFEST-… may be corrupted") or as a bare IOError, depending on where
 * the open trips.
 *
 * Both signals are required: a status naming an `.sst`/`.blob`/`.log` without
 * a not-found signal is real corruption (e.g. a block checksum mismatch inside
 * the file), and a not-found without one is a different missing file (e.g.
 * CURRENT, the database-does-not-exist shape). Neither may be classified as
 * the race.
 */
bool isMissingSstOpenRace(
	const rocksdb::Status& status,
	const std::filesystem::path& databasePath = {}
);

} // namespace rocksdb_js

#endif
