#include "transaction_log_file.h"

#ifdef PLATFORM_POSIX

#include "core/debug.h"
#include "core/encoding.h"
#include "core/platform.h"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <limits.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <unistd.h>
#include <vector>

// Hook point for unit tests: compile with -DROCKSDB_JS_WRITEV=my_mock_fn to
// inject a partial-write simulation. The macro must expand to a callable with
// the same signature as ::writev. When defined, the block below emits a
// forward declaration so the compiler sees the symbol before first use.
// Production builds leave this macro undefined and call ::writev directly.
#ifdef ROCKSDB_JS_WRITEV
extern "C" ssize_t ROCKSDB_JS_WRITEV(int, const struct iovec*, int);
#else
#define ROCKSDB_JS_WRITEV ::writev
#endif

// Hook point for unit tests: compile with -DROCKSDB_JS_MADVISE=my_mock_fn to
// capture/intercept the madvise() call made by adviseCold() (e.g. to assert it
// is scoped to the file-backed range, or to simulate an old kernel returning
// EINVAL). The macro must expand to a callable with the same signature as
// ::madvise. Production builds call ::madvise directly.
#ifdef ROCKSDB_JS_MADVISE
extern "C" int ROCKSDB_JS_MADVISE(void*, size_t, int);
#else
#define ROCKSDB_JS_MADVISE ::madvise
#endif

namespace rocksdb_js {

TransactionLogFile::TransactionLogFile(const std::filesystem::path& p, const uint32_t seq) :
	path(p),
	sequenceNumber(seq)
{}

void TransactionLogFile::close() {
	std::lock_guard<std::mutex> lock(this->fileMutex);

	// Explicitly remove our reference to the memory map.
	if (this->memoryMap) {
		DEBUG_LOG("%p TransactionLogFile::close Closing memory map for: %s (ref count=%ld)\n",
			this, this->path.string().c_str(), this->memoryMap.use_count());
		this->memoryMap.reset();
#if TRANSACTION_LOG_ENABLE_ANONYMOUS_OVERLAY
		this->lastOverlaySize.store(0, std::memory_order_relaxed);
#endif
	}

	if (this->fd >= 0) {
		DEBUG_LOG("%p TransactionLogFile::close Closing file: %s (fd=%d)\n",
			this, this->path.string().c_str(), this->fd);
		::close(this->fd);
		this->fd = -1;
	}
}

void TransactionLogFile::flush() {
	std::unique_lock<std::mutex> lock(this->fileMutex);
	uint32_t currentSize = this->size.load(std::memory_order_relaxed);
	// Only flush if there's new data since the last flush
	if (this->fd == -1 || currentSize <= this->lastFlushedSize) {
		return; // return early
	}
	int fdToFlush = this->fd;
	// Perform the flush without holding the lock (since fdatasync/fsync can be slow)
	lock.unlock();
	DEBUG_LOG("%p TransactionLogFile::flush Flushing file: %s (fd=%d, size=%u, lastFlushedSize=%u)\n",
		this, this->path.string().c_str(), fdToFlush, currentSize, this->lastFlushedSize);

	// macOS doesn't have fdatasync, use fsync instead
	// fdatasync is faster on Linux as it doesn't sync metadata
#ifdef __APPLE__
	if (::fsync(fdToFlush) < 0) {
		DEBUG_LOG("%p TransactionLogFile::flush ERROR: fsync failed: %s (errno=%d)\n",
			this, ::strerror(errno), errno);
		throw rocksdb_js::DBException("Failed to flush file: " + this->path.string());
	}
#else
	if (::fdatasync(fdToFlush) < 0) {
		DEBUG_LOG("%p TransactionLogFile::flush ERROR: fdatasync failed: %s (errno=%d)\n",
			this, ::strerror(errno), errno);
		throw rocksdb_js::DBException("Failed to flush file: " + this->path.string());
	}
#endif

	// Update the last flushed size after successful sync
	lock.lock();
	this->lastFlushedSize = currentSize;
}

void TransactionLogFile::openFile() {
	if (this->fd >= 0) {
		DEBUG_LOG("%p TransactionLogFile::openFile File already open: %s\n", this, this->path.string().c_str());
		return;
	}

	// Fresh (re)open: until the first append, a zero timestamp seen while indexing is a genuine
	// end-of-data marker (and this->size may be seeded from a padded on-disk size that needs
	// correcting), so findPositionByTimestamp is allowed to correct this->size. See hasAppendedSinceOpen.
	this->hasAppendedSinceOpen.store(false);

	DEBUG_LOG("%p TransactionLogFile::openFile Opening file: %s\n", this, this->path.string().c_str());

	// ensure parent directory exists (may have been deleted by purge())
	auto parentPath = this->path.parent_path();
	if (!parentPath.empty()) {
		try {
			DEBUG_LOG("%p TransactionLogFile::openFile Creating parent directory: %s\n", this, parentPath.string().c_str());
			rocksdb_js::tryCreateDirectory(parentPath);
		} catch (const std::filesystem::filesystem_error& e) {
			DEBUG_LOG("%p TransactionLogFile::openFile Failed to create parent directory: %s (error=%s)\n",
				this, parentPath.string().c_str(), e.what());
			throw rocksdb_js::DBException("Failed to create parent directory: " + parentPath.string());
		}
	}

	// open file for both reading and writing
	this->fd = ::open(this->path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0640);
	if (this->fd < 0) {
		DEBUG_LOG("%p TransactionLogFile::openFile Failed to open sequence file for read/write: %s (error=%d)\n",
			this, this->path.string().c_str(), errno);
		throw rocksdb_js::DBException("Failed to open sequence file for read/write: " + this->path.string());
	}

	// get file size
	struct stat st;
	if (::fstat(this->fd, &st) < 0) {
		DEBUG_LOG("%p TransactionLogFile::openFile Failed to get file size: %s (error=%d)\n",
			this, this->path.string().c_str(), errno);
		throw rocksdb_js::DBException("Failed to get file size: " + this->path.string());
	}
	this->size = st.st_size;
	DEBUG_LOG("%p TransactionLogFile::openFile File size: %s (size=%zu)\n",
		this, this->path.string().c_str(), this->size.load(std::memory_order_relaxed));
}

// Precondition: caller holds fileMutex (the guard for this->memoryMap /
// this->fd / this->frozenMapCache). The public getMemoryMap() wrapper acquires
// it; the open path holds it already. This is the only place memoryMap /
// frozenMapCache are (re)assigned, so holding fileMutex makes that shared_ptr
// access race-free against close()/removeFile()/adviseCold().
std::shared_ptr<MemoryMap> TransactionLogFile::getMemoryMapLocked(uint32_t fileSize, bool isCurrent) {
	// mmap with length 0 has undefined behavior according to POSIX.
	// Different runtimes handle this differently - Node.js/Bun tolerate it,
	// but Deno stalls. Return nullptr for empty or too-small files.
	if (fileSize == 0) {
		DEBUG_LOG("%p TransactionLogFile::getMemoryMapLocked fileSize is 0, returning nullptr\n", this);
		return nullptr;
	}

	// Reuse an existing live mapping that is already large enough — the strong
	// ref for the current file, or a still-live frozen handout.
	std::shared_ptr<MemoryMap> map = this->memoryMap ? this->memoryMap : this->frozenMapCache.lock();
	if (!(map && map->map && map->mapSize >= fileSize)) {
#if TRANSACTION_LOG_ENABLE_ANONYMOUS_OVERLAY
		// On POSIX, mmap(fd, maxFileSize) over a small file causes SIGBUS on
		// pages entirely beyond the file. We first create an anonymous
		// zero-filled mapping for the full region, then overlay the actual file
		// content at the start via MAP_FIXED. Pages beyond the file remain
		// anonymous and safely read as zero.
		void* anonMap = ::mmap(NULL, fileSize, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		DEBUG_LOG("%p TransactionLogFile::getMemoryMap new anonymous map: %p (size=%u)\n", this, anonMap, fileSize);
		if (anonMap == MAP_FAILED) {
			DEBUG_LOG("%p TransactionLogFile::getMemoryMap ERROR: mmap (anonymous) failed: %s\n", this, ::strerror(errno));
			return nullptr;
		}

		uint32_t actualSize = std::min(this->size.load(std::memory_order_relaxed), fileSize);
		if (actualSize > 0 && this->fd >= 0) {
			void* fileMap = ::mmap(anonMap, actualSize, PROT_READ, MAP_SHARED | MAP_FIXED, this->fd, 0);
			if (fileMap == MAP_FAILED) {
				DEBUG_LOG("%p TransactionLogFile::getMemoryMap ERROR: mmap (file overlay) failed: %s\n", this, ::strerror(errno));
				::munmap(anonMap, fileSize);
				return nullptr;
			}
		}
		this->lastOverlaySize.store(actualSize, std::memory_order_relaxed);

		// The MemoryMap destructor calls munmap on the full region, which
		// correctly frees both anonymous and file-backed pages. Removing files
		// that are memory mapped is perfectly fine on POSIX, and the memory map
		// can be safely used indefinitely.
		map = std::make_shared<MemoryMap>(anonMap, fileSize);
#else
		void* newMap = ::mmap(NULL, fileSize, PROT_READ, MAP_SHARED, this->fd, 0);
		DEBUG_LOG("%p TransactionLogFile::getMemoryMap new memory map: %p\n", this, newMap);
		if (newMap == MAP_FAILED) {
			DEBUG_LOG("%p TransactionLogFile::getMemoryMap ERROR: mmap failed: %s", this, ::strerror(errno));
			return nullptr;
		}
		map = std::make_shared<MemoryMap>(newMap, fileSize);
#endif
	}
	map->fileSize = fileSize;

	// Ownership: the current (actively-written) file keeps a strong reference —
	// the writer extends its overlay and the index reads through it. A frozen
	// file keeps only a weak handle, so the returned shared_ptr (and the JS
	// external buffer it becomes) is the sole owner and the mapping is unmapped
	// when JS releases it.
	if (isCurrent) {
		this->memoryMap = map;
#if TRANSACTION_LOG_ENABLE_ANONYMOUS_OVERLAY
		this->updateMemoryMapOverlay(); // extend the overlay if the file grew since this map was created
#endif
	} else {
		// Frozen: keep only a weak handle so the returned shared_ptr (the JS
		// external buffer) is the sole owner and the mapping is unmapped when JS
		// releases it. (No need to set frozenMapCache on the current-file branch:
		// it is read only when memoryMap is null, and downgradeMapToFrozen() seeds
		// it from memoryMap at the moment the file is frozen.)
		this->memoryMap.reset();
		this->frozenMapCache = map;
	}
	return map;
}

size_t TransactionLogFile::adviseCold() {
#ifdef MADV_COLD
	// Once we observe EINVAL (kernel < 5.4 lacks MADV_COLD), latch off so we
	// don't keep issuing a syscall that will always fail. Process-global: a
	// kernel that lacks the feature lacks it for every log file.
	if (madvColdUnsupported.load(std::memory_order_relaxed)) {
		return 0;
	}

	// Pin the live map under fileMutex so a concurrent close()/removeFile()/
	// getMemoryMap() (which (re)assign memoryMap under the same lock) cannot
	// munmap it out from under the madvise() below. We hold only a shared_ptr
	// copy across the syscall, not the lock. The current file holds a strong
	// memoryMap; a frozen file's map lives in frozenMapCache (weak) while a JS
	// buffer keeps it alive — cool that too while it is still resident.
	std::shared_ptr<MemoryMap> map;
	uint32_t actualSize;
	{
		std::lock_guard<std::mutex> lock(this->fileMutex);
		map = this->memoryMap ? this->memoryMap : this->frozenMapCache.lock();
		if (!map || !map->map) {
			return 0;
		}
		actualSize = std::min(this->size.load(std::memory_order_relaxed), map->mapSize);
	}

	// Floor the length to a page boundary. The mapping base is page-aligned (it
	// comes from mmap), but actualSize is the exact file extent and is rarely a
	// page multiple. Rounding *down* guarantees we never advise a page that
	// overlaps the MAP_PRIVATE|MAP_ANONYMOUS zero-fill tail beyond actualSize —
	// advising (let alone evicting) that region would be destructive.
	long pageSize = ::sysconf(_SC_PAGESIZE);
	if (pageSize <= 0) {
		pageSize = 4096;
	}
	size_t length = static_cast<size_t>(actualSize) & ~(static_cast<size_t>(pageSize) - 1);
	if (length == 0) {
		return 0;
	}

	if (ROCKSDB_JS_MADVISE(map->map, length, MADV_COLD) != 0) {
		if (errno == EINVAL) {
			DEBUG_LOG("%p TransactionLogFile::adviseCold MADV_COLD unsupported (EINVAL); disabling\n", this);
			madvColdUnsupported.store(true, std::memory_order_relaxed);
		} else {
			DEBUG_LOG("%p TransactionLogFile::adviseCold madvise failed: %s (errno=%d)\n",
				this, ::strerror(errno), errno);
		}
		return 0;
	}

	DEBUG_LOG("%p TransactionLogFile::adviseCold MADV_COLD %zu bytes of %s\n",
		this, length, this->path.string().c_str());
	return length;
#else
	// macOS and other POSIX platforms without MADV_COLD: no-op.
	return 0;
#endif
}

#if TRANSACTION_LOG_ENABLE_ANONYMOUS_OVERLAY
void TransactionLogFile::updateMemoryMapOverlay() {
	// Precondition: caller holds fileMutex (writeEntriesV1 holds it; getMemoryMap
	// acquires it before calling in). Do not lock here — fileMutex is not
	// recursive, so re-locking from getMemoryMap would deadlock.
	if (!this->memoryMap || !this->memoryMap->map || this->fd < 0) return;

	uint32_t actualSize = std::min(this->size.load(std::memory_order_relaxed), this->memoryMap->mapSize);
	if (actualSize == 0) return;

	uint32_t lastOverlay = this->lastOverlaySize.load(std::memory_order_relaxed);
	uint32_t overlayPageEnd = lastOverlay > 0 ? ((lastOverlay + 4095u) & ~4095u) : 0;
	if (actualSize <= overlayPageEnd) return;

	DEBUG_LOG("%p TransactionLogFile::updateMemoryMapOverlay Extending overlay %u -> %u\n",
		this, lastOverlay, actualSize);
	void* result = ::mmap(this->memoryMap->map, actualSize, PROT_READ, MAP_SHARED | MAP_FIXED, this->fd, 0);
	if (result != MAP_FAILED) {
		this->lastOverlaySize.store(actualSize, std::memory_order_relaxed);
	}
}
#endif

int64_t TransactionLogFile::readFromFile(void* buffer, uint32_t size, int64_t offset) {
	if (offset >= 0) {
		return static_cast<int64_t>(::pread(this->fd, buffer, size, offset));
	}
	return static_cast<int64_t>(::read(this->fd, buffer, size));
}

bool TransactionLogFile::removeFile() {
	std::unique_lock<std::mutex> lock(this->fileMutex);

	if (this->memoryMap) {
		DEBUG_LOG("%p TransactionLogFile::removeFile Releasing memory map before removing file: %s\n",
			this, this->path.string().c_str());
		this->memoryMap.reset();
#if TRANSACTION_LOG_ENABLE_ANONYMOUS_OVERLAY
		this->lastOverlaySize.store(0, std::memory_order_relaxed);
#endif
	}

	if (this->fd >= 0) {
		DEBUG_LOG("%p TransactionLogFile::removeFile Closing file: %s (fd=%d)\n",
			this, this->path.string().c_str(), this->fd);
		::close(this->fd);
		this->fd = -1;
	}

	DEBUG_LOG("%p TransactionLogFile::removeFile Removing file: %s\n", this, this->path.string().c_str());
	auto removed = std::filesystem::remove(this->path);
	if (!removed) {
		DEBUG_LOG("%p TransactionLogFile::removeFile Failed to remove file %s\n",
			this, this->path.string().c_str());
		return false;
	}

	DEBUG_LOG("%p TransactionLogFile::removeFile Removed file %s\n",
		this, this->path.string().c_str());
	return true;
}

int64_t TransactionLogFile::writeBatchToFile(iovec* iovecs, int iovcnt) {
	if (iovcnt <= 0) {
		return 0;
	}

	// writev has a per-call iovec limit (IOV_MAX) and may return short on
	// partial writes (EINTR, ENOSPC, NFS/FUSE, etc.). Track byte progress
	// through the iovec array and advance into a partial iovec's remainder so
	// a short writev does not silently drop the tail of an entry. The caller's
	// iovec array is mutated in place to avoid a scratch allocation on the hot
	// write path.
	int64_t totalWritten = 0;
	int pendingIdx = 0;

	while (pendingIdx < iovcnt) {
		int toWrite = std::min(iovcnt - pendingIdx, static_cast<int>(IOV_MAX));
		ssize_t written = ROCKSDB_JS_WRITEV(this->fd, &iovecs[pendingIdx], toWrite);

		if (written < 0) {
			if (errno == EINTR) {
				continue;
			}
			return -1;
		}

		if (written == 0) {
			// shouldn't happen for regular files; bail to avoid an infinite loop
			return -1;
		}

		totalWritten += written;

		size_t remainingBytes = static_cast<size_t>(written);
		while (remainingBytes > 0 && pendingIdx < iovcnt) {
			iovec& iov = iovecs[pendingIdx];
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

int64_t TransactionLogFile::writeToFile(const void* buffer, uint32_t size, int64_t offset) {
	if (offset >= 0) {
		return static_cast<int64_t>(::pwrite(this->fd, buffer, size, offset));
	}
	return static_cast<int64_t>(::write(this->fd, buffer, size));
}

bool TransactionLogFile::truncateFile(uint32_t newSize) {
	if (this->fd < 0) {
		return false;
	}
	if (::ftruncate(this->fd, static_cast<off_t>(newSize)) != 0) {
		DEBUG_LOG("%p TransactionLogFile::truncateFile ftruncate failed: %s (errno=%d)\n",
			this, ::strerror(errno), errno);
		return false;
	}
	// Persist the size change so a second crash can't resurrect the dropped
	// tail. fsync (not fdatasync) because the metadata size must be durable.
	if (::fsync(this->fd) != 0) {
		DEBUG_LOG("%p TransactionLogFile::truncateFile fsync after ftruncate failed: %s (errno=%d)\n",
			this, ::strerror(errno), errno);
		// the truncation itself succeeded; a later flush() will sync again
	}
	return true;
}

} // namespace rocksdb_js

#endif
