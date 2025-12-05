#include "transaction_log_file.h"
#include "macros.h"
#include "util.h"
#include <sys/mman.h>

#ifdef PLATFORM_POSIX

namespace rocksdb_js {

TransactionLogFile::TransactionLogFile(const std::filesystem::path& p, const uint32_t seq) :
	path(p),
	sequenceNumber(seq)
{}

void TransactionLogFile::close() {
	std::unique_lock<std::mutex> lock(this->fileMutex);

	if (this->fd >= 0) {
		DEBUG_LOG("%p TransactionLogFile::close Closing file: %s (fd=%d)\n",
			this, this->path.string().c_str(), this->fd)
		::close(this->fd);
		this->fd = -1;
	}
}

void TransactionLogFile::openFile() {
	if (this->fd >= 0) {
		DEBUG_LOG("%p TransactionLogFile::openFile File already open: %s\n", this, this->path.string().c_str())
		return;
	}

	DEBUG_LOG("%p TransactionLogFile::openFile Opening file: %s\n", this, this->path.string().c_str())

	// ensure parent directory exists (may have been deleted by purge())
	auto parentPath = this->path.parent_path();
	if (!parentPath.empty()) {
		try {
			std::filesystem::create_directories(parentPath);
		} catch (const std::filesystem::filesystem_error& e) {
			DEBUG_LOG("%p TransactionLogFile::openFile Failed to create parent directory: %s (error=%s)\n",
				this, parentPath.string().c_str(), e.what())
			throw std::runtime_error("Failed to create parent directory: " + parentPath.string());
		}
	}

	// open file for both reading and writing
	this->fd = ::open(this->path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
	if (this->fd < 0) {
		DEBUG_LOG("%p TransactionLogFile::openFile Failed to open sequence file for read/write: %s (error=%d)\n",
			this, this->path.string().c_str(), errno)
		throw std::runtime_error("Failed to open sequence file for read/write: " + this->path.string());
	}

	// get file size
	struct stat st;
	if (::fstat(this->fd, &st) < 0) {
		DEBUG_LOG("%p TransactionLogFile::openFile Failed to get file size: %s (error=%d)\n",
			this, this->path.string().c_str(), errno)
		throw std::runtime_error("Failed to get file size: " + this->path.string());
	}
	this->size = st.st_size;
	DEBUG_LOG("%p TransactionLogFile::openFile File size: %s (size=%zu)\n",
		this, this->path.string().c_str(), this->size)
}

MemoryMap* TransactionLogFile::getMemoryMap(uint32_t fileSize) {
	if (!this->memoryMap) {
		void* map = ::mmap(NULL, fileSize, PROT_READ, MAP_SHARED, this->fd, 0);
		DEBUG_LOG("%p TransactionLogFile::getMemoryMap new memory map: %p\n", this, map);
		if (map == MAP_FAILED) {
			DEBUG_LOG("%p TransactionLogFile::getMemoryMap ERROR: mmap failed: %s", this, ::strerror(errno))
			return nullptr;
		}
		// If successful, return a MemoryMap object for tracking references.
		// Note, that we do not need to do any cleanup from this class's
		// destructor. Removing files that are memory mapped is perfectly fine,
		// and the memory map can be safely used indefinitely (the file descriptor
		// doesn't need to be kept open either).
		memoryMap = new MemoryMap(map, fileSize);
	}
	memoryMap->fileSize = fileSize;
	return memoryMap;
}

int64_t TransactionLogFile::readFromFile(void* buffer, uint32_t size, int64_t offset) {
	if (offset >= 0) {
		return static_cast<int64_t>(::pread(this->fd, buffer, size, offset));
	}
	return static_cast<int64_t>(::read(this->fd, buffer, size));
}

bool TransactionLogFile::removeFile() {
	std::unique_lock<std::mutex> lock(this->fileMutex);

	if (this->fd >= 0) {
		DEBUG_LOG("%p TransactionLogFile::removeFile Closing file: %s (fd=%d)\n",
			this, this->path.string().c_str(), this->fd)
		::close(this->fd);
		this->fd = -1;
	}

	auto removed = std::filesystem::remove(this->path);
	if (!removed) {
		DEBUG_LOG("%p TransactionLogFile::removeFile Failed to remove file %s\n",
			this, this->path.string().c_str())
		return false;
	}

	DEBUG_LOG("%p TransactionLogFile::removeFile Removed file %s\n",
		this, this->path.string().c_str())
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
				this, errno, strerror(errno))
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

MemoryMap::MemoryMap(void* map, uint32_t mapSize) : map(map), mapSize(mapSize) {}

MemoryMap::~MemoryMap() {
	if (this->map != nullptr) {
		::munmap(this->map, this->mapSize);
	}
}

} // namespace rocksdb_js

#endif
