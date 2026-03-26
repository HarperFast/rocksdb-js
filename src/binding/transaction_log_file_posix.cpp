#include "transaction_log_file.h"

#ifdef PLATFORM_POSIX

#include "macros.h"
#include "util.h"
#include <sys/mman.h>

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
		throw std::runtime_error("Failed to flush file: " + this->path.string());
	}
#else
	if (::fdatasync(fdToFlush) < 0) {
		DEBUG_LOG("%p TransactionLogFile::flush ERROR: fdatasync failed: %s (errno=%d)\n",
			this, ::strerror(errno), errno);
		throw std::runtime_error("Failed to flush file: " + this->path.string());
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
			throw std::runtime_error("Failed to create parent directory: " + parentPath.string());
		}
	}

	// open file for both reading and writing
	this->fd = ::open(this->path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0640);
	if (this->fd < 0) {
		DEBUG_LOG("%p TransactionLogFile::openFile Failed to open sequence file for read/write: %s (error=%d)\n",
			this, this->path.string().c_str(), errno);
		throw std::runtime_error("Failed to open sequence file for read/write: " + this->path.string());
	}

	// get file size
	struct stat st;
	if (::fstat(this->fd, &st) < 0) {
		DEBUG_LOG("%p TransactionLogFile::openFile Failed to get file size: %s (error=%d)\n",
			this, this->path.string().c_str(), errno);
		throw std::runtime_error("Failed to get file size: " + this->path.string());
	}
	this->size = st.st_size;
	DEBUG_LOG("%p TransactionLogFile::openFile File size: %s (size=%zu)\n",
		this, this->path.string().c_str(), this->size.load(std::memory_order_relaxed));
}

std::shared_ptr<MemoryMap> TransactionLogFile::getMemoryMap(uint32_t fileSize) {
	// mmap with length 0 has undefined behavior according to POSIX.
	// Different runtimes handle this differently - Node.js/Bun tolerate it,
	// but Deno stalls. Return nullptr for empty or too-small files.
	if (fileSize == 0) {
		DEBUG_LOG("%p TransactionLogFile::getMemoryMap fileSize is 0, returning nullptr\n", this);
		return nullptr;
	}

	uint32_t actualFileSize = this->size.load(std::memory_order_relaxed);

	if (this->memoryMap && this->memoryMap->mapSize >= fileSize) {
		// The existing anonymous mapping is large enough. If the file has grown
		// since the last time we overlaid it, extend the MAP_SHARED overlay so
		// readers immediately see the new data without requiring a new mmap.
		if (this->fd >= 0 && actualFileSize > this->mmapOverlaySize) {
			uint32_t newOverlaySize = std::min(actualFileSize, fileSize);
			void* updated = ::mmap(this->memoryMap->map, newOverlaySize, PROT_READ,
				MAP_SHARED | MAP_FIXED, this->fd, 0);
			if (updated != MAP_FAILED) {
				this->mmapOverlaySize = newOverlaySize;
				DEBUG_LOG("%p TransactionLogFile::getMemoryMap Extended file overlay to %u bytes\n",
					this, newOverlaySize);
			} else {
				DEBUG_LOG("%p TransactionLogFile::getMemoryMap WARNING: overlay extension failed: %s\n",
					this, ::strerror(errno));
			}
		}
		this->memoryMap->fileSize = fileSize;
		return this->memoryMap;
	}

	// Allocate an anonymous read-only region of the full requested size.
	// All bytes default to zero, so readers that iterate past the end of
	// actual file content will see 0.0 timestamps and stop gracefully
	// instead of triggering a SIGBUS (which MAP_SHARED alone would cause
	// when the file is smaller than fileSize on Linux).
	void* anonMap = ::mmap(NULL, fileSize, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (anonMap == MAP_FAILED) {
		DEBUG_LOG("%p TransactionLogFile::getMemoryMap ERROR: anonymous mmap failed: %s\n",
			this, ::strerror(errno));
		return nullptr;
	}
	DEBUG_LOG("%p TransactionLogFile::getMemoryMap Created anonymous map %p (size=%u, actualFileSize=%u)\n",
		this, anonMap, fileSize, actualFileSize);

	// Overlay the real file content at the start of the anonymous mapping so
	// that readers see up-to-date data. MAP_FIXED replaces the corresponding
	// pages of the anonymous region with file-backed shared pages.
	uint32_t overlaySize = 0;
	if (this->fd >= 0 && actualFileSize > 0) {
		overlaySize = std::min(actualFileSize, fileSize);
		void* fileMap = ::mmap(anonMap, overlaySize, PROT_READ,
			MAP_SHARED | MAP_FIXED, this->fd, 0);
		if (fileMap == MAP_FAILED) {
			DEBUG_LOG("%p TransactionLogFile::getMemoryMap ERROR: file overlay mmap failed: %s\n",
				this, ::strerror(errno));
			::munmap(anonMap, fileSize);
			return nullptr;
		}
		DEBUG_LOG("%p TransactionLogFile::getMemoryMap Overlaid %u bytes of file content at %p\n",
			this, overlaySize, anonMap);
	}

	this->mmapOverlaySize = overlaySize;
	this->memoryMap = std::make_shared<MemoryMap>(anonMap, fileSize);
	this->memoryMap->fileSize = fileSize;
	return this->memoryMap;
}

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

int64_t TransactionLogFile::writeBatchToFile(const iovec* iovecs, int iovcnt) {
	if (iovcnt == 0) {
		return 0;
	}

	// writev has a limit on the number of iovecs (IOV_MAX, typically 1024 on macOS)
	// if we exceed this, we need to batch the writes
	constexpr int MAX_IOVS = 1024;  // IOV_MAX on most systems
	int64_t totalWritten = 0;
	int remaining = iovcnt;
	int offset = 0;

	while (remaining > 0) {
		int toWrite = std::min(remaining, MAX_IOVS);
		ssize_t written = ::writev(this->fd, iovecs + offset, toWrite);

		if (written < 0) {
			DEBUG_LOG("%p TransactionLogFile::writeBatchToFile writev failed: errno=%d (%s)\n",
				this, errno, strerror(errno));
			return -1;
		}

		totalWritten += written;
		offset += toWrite;
		remaining -= toWrite;
	}

	return totalWritten;
}

int64_t TransactionLogFile::writeToFile(const void* buffer, uint32_t size, int64_t offset) {
	if (offset >= 0) {
		return static_cast<int64_t>(::pwrite(this->fd, buffer, size, offset));
	}
	return static_cast<int64_t>(::write(this->fd, buffer, size));
}

} // namespace rocksdb_js

#endif
