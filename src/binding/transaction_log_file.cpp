#include "transaction_log_file.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

TransactionLogFile::~TransactionLogFile() {
	this->close();
}

void TransactionLogFile::close() {
	if (this->mappedData) {
		::munmap(this->mappedData, this->mappedSize);
		this->mappedData = nullptr;
	}
	if (this->fd >= 0) {
		::close(this->fd);
		this->fd = -1;
		this->isOpen = false;
	}
}

void TransactionLogFile::open() {
	if (this->isOpen) {
		return;
	}

	// open file for both reading and writing
	this->fd = ::open(this->path.c_str(), O_RDWR | O_CREAT, 0644);
	if (this->fd < 0) {
		throw std::runtime_error("Failed to open sequence file for read/write: " + this->path.string());
	}

	this->isOpen = true;

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

	DEBUG_LOG("TransactionLogFile::open Opened %s (fd=%d)\n", this->path.string().c_str(), this->fd);
}

ssize_t TransactionLogFile::read(void* buffer, size_t size, off_t offset) {
	if (!this->isOpen) {
		open();
	}

	if (offset >= 0) {
		// read from specific offset
		return ::pread(this->fd, buffer, size, offset);
	} else {
		// Read from current position
		return ::read(this->fd, buffer, size);
	}
}

ssize_t TransactionLogFile::write(const void* buffer, size_t size, off_t offset) {
	if (!this->isOpen) {
		open();
	}

	if (offset >= 0) {
		// write at specific offset
		return ::pwrite(this->fd, buffer, size, offset);
	} else {
		// Append to end of file
		return ::write(this->fd, buffer, size);
	}
}

} // namespace rocksdb_js