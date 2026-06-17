#ifndef __DATABASE_BACKUP_H__
#define __DATABASE_BACKUP_H__

#include <node_api.h>

namespace rocksdb_js {

/**
 * Registers the module-level backup management functions (`backupRestore`,
 * `backupList`, `backupDelete`, `backupPurge`, `backupVerify`) on the exports
 * object. These operate on a backup directory and do not require an open
 * database. Creating a backup is exposed separately as `Database::Backup`
 * since it needs a live database handle.
 */
void initBackupExports(napi_env env, napi_value exports);

} // namespace rocksdb_js

#endif
