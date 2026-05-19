// Standalone implementation of TransactionLogFile::writevAll. Kept in its own
// translation unit so it can be linked into a small unit-test executable
// without dragging in N-API and RocksDB. Production builds include the full
// transaction_log_file.h (for the TransactionLogFile struct declaration);
// test builds define ROCKSDB_JS_WRITEV_ALL_STANDALONE to provide a minimal
// forward declaration instead.

#if defined(_WIN32)
// no-op on Windows; the Win32 backend has its own retry loop in
// transaction_log_file_windows.cpp.
#else

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <sys/uio.h>
#include <vector>

#ifdef ROCKSDB_JS_WRITEV_ALL_STANDALONE
namespace rocksdb_js {
struct TransactionLogFile {
	static int64_t writevAll(int fd, const iovec* iovecs, int iovcnt);
};
} // namespace rocksdb_js
#else
#include "transaction_log_file.h"
#endif

namespace rocksdb_js {

int64_t TransactionLogFile::writevAll(int fd, const iovec* iovecs, int iovcnt) {
	if (iovcnt <= 0) {
		return 0;
	}

	// writev has a limit on the number of iovecs (IOV_MAX, typically 1024 on macOS)
	// and may return short on partial writes (EINTR, ENOSPC, NFS/FUSE, etc.).
	// We track byte progress through the iovec array and advance into a partial
	// iovec's remainder by adjusting iov_base/iov_len so a short writev does not
	// silently drop the tail of an entry.
	constexpr int MAX_IOVS = 1024;  // IOV_MAX on most systems
	int64_t totalWritten = 0;

	// mutable copy so we can advance into a partial iovec without mutating the
	// caller's array
	std::vector<iovec> pending(iovecs, iovecs + iovcnt);
	size_t pendingIdx = 0;

	while (pendingIdx < pending.size()) {
		int toWrite = static_cast<int>(std::min(pending.size() - pendingIdx, static_cast<size_t>(MAX_IOVS)));
		ssize_t written = ::writev(fd, &pending[pendingIdx], toWrite);

		if (written < 0) {
			if (errno == EINTR) {
				// interrupted before any bytes were transferred; retry
				continue;
			}
			return -1;
		}

		if (written == 0) {
			// shouldn't happen for regular files; bail to avoid an infinite loop
			return -1;
		}

		totalWritten += written;

		// advance pendingIdx and trim a partial iovec by `written` bytes
		size_t remainingBytes = static_cast<size_t>(written);
		while (remainingBytes > 0 && pendingIdx < pending.size()) {
			iovec& iov = pending[pendingIdx];
			if (remainingBytes >= iov.iov_len) {
				remainingBytes -= iov.iov_len;
				++pendingIdx;
			} else {
				iov.iov_base = static_cast<char*>(iov.iov_base) + remainingBytes;
				iov.iov_len -= remainingBytes;
				remainingBytes = 0;
			}
		}
	}

	return totalWritten;
}

} // namespace rocksdb_js

#endif // !_WIN32
