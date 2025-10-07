#include "transaction_log_file.h"
#include "macros.h"
#include "util.h"

#ifdef PLATFORM_POSIX

namespace rocksdb_js {

void TransactionLogFile::close() {
	if (this->mappedData) {
		::munmap(this->mappedData, this->mappedSize);
		this->mappedData = nullptr;
	}
	if (this->fd >= 0) {
		::close(this->fd);
		this->fd = -1;
	}
}

void TransactionLogFile::open() {
	if (this->fd >= 0) {
		return;
	}

	// open file for both reading and writing
	this->fd = ::open(this->path.c_str(), O_RDWR | O_CREAT, 0644);
	if (this->fd < 0) {
		throw std::runtime_error("Failed to open sequence file for read/write: " + this->path.string());
	}

	// Get file size
	struct stat st;
	if (::fstat(this->fd, &st) < 0) {
		throw std::runtime_error("Failed to get file size: " + this->path.string());
	}
	this->mappedSize = st.st_size;

	if (this->mappedSize > 0) {
		// map file into memory for reading
		this->mappedData = static_cast<char*>(::mmap(nullptr, this->mappedSize, PROT_READ, MAP_SHARED, this->fd, 0));

		if (this->mappedData == MAP_FAILED) {
			throw std::runtime_error("Failed to mmap sequence file: " + this->path.string());
		}

		// advise OS about access pattern
		::madvise(this->mappedData, this->mappedSize, MADV_SEQUENTIAL);
	}

	// Fix: Store the path string in a local variable to avoid temporary object issues
	std::string pathStr = this->path.string();
	DEBUG_LOG("TransactionLogFile::open Opened %s (fd=%d)\n", pathStr.c_str(), this->fd);
}

int64_t TransactionLogFile::readFromFile(void* buffer, size_t size, int64_t offset) {
	if (this->fd < 0) {
		this->open();
	}

	if (offset >= 0) {
		return static_cast<int64_t>(::pread(this->fd, buffer, size, offset));
	}
	return static_cast<int64_t>(::read(this->fd, buffer, size));
}

int64_t TransactionLogFile::writeToFile(const void* buffer, size_t size, int64_t offset) {
	if (this->fd < 0) {
		this->open();
	}

	if (offset >= 0) {
		return static_cast<int64_t>(::pwrite(this->fd, buffer, size, offset));
	}
	return static_cast<int64_t>(::write(this->fd, buffer, size));
}

} // namespace rocksdb_js

#endif
