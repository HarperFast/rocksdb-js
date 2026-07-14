#ifndef __CORE_FILE_LOCK_H__
#define __CORE_FILE_LOCK_H__

#include <cstdint>
#include <string>

namespace rocksdb_js {

/**
 * Opens `file` (creating it and any missing parent directories) and takes a
 * non-blocking advisory lock on it — `flock` on POSIX, `LockFileEx` on
 * Windows. The lock is exclusive by default; pass `shared` for a shared
 * (reader) lock, which coexists with other shared holders but conflicts with
 * an exclusive holder in either direction. Returns an opaque non-zero token to
 * pass to `releaseFileLock`, `0` if a conflicting holder currently has the
 * lock, or throws `DBException` on a hard error.
 *
 * The OS handle is opened, held, and closed entirely within native code and is
 * never exposed to JavaScript. This is deliberate: the addon statically links
 * its own C runtime (`binding.gyp` `RuntimeLibrary: 0`), so a descriptor created
 * by Node/libuv is not resolvable here — `_get_osfhandle` on such an fd would
 * fault. Owning the handle natively sidesteps the cross-runtime boundary.
 *
 * A shared lock opens the file read-only and never creates it, so a reader
 * (e.g. a restore) can lock an existing lock file on a read-only backup
 * directory (immutable/WORM store, read-only NFS/bind mount) — the exclusive
 * path still opens read-write and creates the file. When even that read-only
 * open fails because the media is read-only for *every* process (`EROFS` on
 * POSIX, `ERROR_WRITE_PROTECT` on Windows), the shared lock degrades to a
 * successful no-op: no exclusive holder can exist on a directory nothing can
 * write, so the lock would protect nothing there. Permission denial
 * (`EACCES`/`EPERM`, `ERROR_ACCESS_DENIED`) is *not* degraded — it means only
 * the calling identity is blocked, so a more-privileged writer could still hold
 * a real exclusive lock; those cases hard-fail rather than skip coordination.
 *
 * The kernel owns the lock, so it is released when the handle is closed —
 * including implicitly when the process dies — with no staleness heuristic.
 * On filesystems that don't implement advisory locking (`EOPNOTSUPP`/`ENOTSUP`,
 * e.g. some FUSE/9p mounts) it degrades to a successful no-op lock rather than
 * making backups impossible.
 */
uint32_t tryAcquireFileLock(const std::string& file, bool shared = false);

/**
 * Releases a lock previously returned by `tryAcquireFileLock` by closing its
 * handle (which releases the kernel lock). A no-op for token `0` or an unknown
 * token.
 */
void releaseFileLock(uint32_t token);

} // namespace rocksdb_js

#endif
