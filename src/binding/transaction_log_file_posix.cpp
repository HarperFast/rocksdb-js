#include "transaction_log_file.h"
#include "macros.h"
#include "util.h"

#ifdef PLATFORM_POSIX

namespace rocksdb_js {

TransactionLogFile::TransactionLogFile(const std::filesystem::path& p, const uint32_t seq) :
	path(p),
	sequenceNumber(seq),
	fd(-1)
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
		std::filesystem::create_directories(parentPath);
	}

	// open file for both reading and writing
	this->fd = ::open(this->path.c_str(), O_RDWR | O_CREAT, 0644);
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
}

int64_t TransactionLogFile::readFromFile(void* buffer, uint32_t size, int64_t offset) {
	if (offset >= 0) {
		return static_cast<int64_t>(::pread(this->fd, buffer, size, offset));
	}
	return static_cast<int64_t>(::read(this->fd, buffer, size));
}

int64_t TransactionLogFile::writeBatchToFile(const iovec* iovecs, int iovcnt) {
	if (iovcnt == 0) {
		return 0;
	}
	return static_cast<int64_t>(::writev(this->fd, iovecs, iovcnt));
}

int64_t TransactionLogFile::writeToFile(const void* buffer, uint32_t size, int64_t offset) {
	if (offset >= 0) {
		return static_cast<int64_t>(::pwrite(this->fd, buffer, size, offset));
	}
	return static_cast<int64_t>(::write(this->fd, buffer, size));
}

} // namespace rocksdb_js

#endif
