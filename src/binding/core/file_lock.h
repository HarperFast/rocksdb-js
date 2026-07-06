#ifndef __CORE_FILE_LOCK_H__
#define __CORE_FILE_LOCK_H__

#include <cstdint>
#include <string>

namespace rocksdb_js {

/**
 * Opens `file` (creating it if missing) and takes a non-blocking exclusive
 * advisory lock on it — `flock` on POSIX, `LockFileEx` on Windows. Returns an
 * opaque non-zero token to pass to `releaseFileLock`, `0` if another holder
 * currently has the lock, or throws `DBException` on a hard error (including a
 * clear "does not exist" message when `file`'s parent directory is missing).
 *
 * The OS handle is opened, held, and closed entirely within native code and is
 * never exposed to JavaScript. This is deliberate: the addon statically links
 * its own C runtime (`binding.gyp` `RuntimeLibrary: 0`), so a descriptor created
 * by Node/libuv is not resolvable here — `_get_osfhandle` on such an fd would
 * fault. Owning the handle natively sidesteps the cross-runtime boundary.
 *
 * The kernel owns the lock, so it is released when the handle is closed —
 * including implicitly when the process dies — with no staleness heuristic.
 * On filesystems that don't implement advisory locking (`EOPNOTSUPP`/`ENOTSUP`,
 * e.g. some FUSE/9p mounts) it degrades to a successful no-op lock rather than
 * making backups impossible.
 */
uint32_t tryAcquireFileLock(const std::string& file);

/**
 * Releases a lock previously returned by `tryAcquireFileLock` by closing its
 * handle (which releases the kernel lock). A no-op for token `0` or an unknown
 * token.
 */
void releaseFileLock(uint32_t token);

} // namespace rocksdb_js

#endif
