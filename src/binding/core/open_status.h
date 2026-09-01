#ifndef __CORE_OPEN_STATUS_H__
#define __CORE_OPEN_STATUS_H__

#include "rocksdb/status.h"

namespace rocksdb_js {

/**
 * Classifies a failed `DB::OpenForReadOnly`/`DB::OpenAsSecondary` status as the
 * missing-SST open race: the MANIFEST replay named an SST file that was gone by
 * the time the open tried to read it. Against a database another process is
 * actively writing, that is a concurrent compaction unlinking an input file
 * mid-open — the database is healthy — but RocksDB reports it either as a
 * Corruption status blaming the MANIFEST ("The file MANIFEST-… may be
 * corrupted") or as a bare IOError, depending on where the open trips.
 *
 * Both signals are required: a status naming an `.sst` without a not-found
 * signal is real corruption (e.g. a block checksum mismatch inside the file),
 * and a not-found without an `.sst` is a different missing file (e.g. CURRENT,
 * the database-does-not-exist shape). Neither may be classified as the race.
 */
bool isMissingSstOpenRace(const rocksdb::Status& status);

} // namespace rocksdb_js

#endif
