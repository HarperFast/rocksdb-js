#include "transaction_log_file.h"
#include "macros.h"
#include "util.h"

#ifdef PLATFORM_POSIX

namespace rocksdb_js {

void TransactionLogFile::close() {
	std::unique_lock<std::mutex> lock(this->closeMutex);

	// Wait for all active operations to complete
	closeCondition.wait(lock, [this] {
		return this->activeOperations.load() == 0;
	});

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

	// get file size
	struct stat st;
	if (::fstat(this->fd, &st) < 0) {
		throw std::runtime_error("Failed to get file size: " + this->path.string());
	}
	this->size.store(st.st_size);

	std::string pathStr = this->path.string();
	DEBUG_LOG("TransactionLogFile::open Opened %s (fd=%d, size=%zu)\n", pathStr.c_str(), this->fd, this->size.load());
}

int64_t TransactionLogFile::readFromFile(void* buffer, size_t size, int64_t offset) {
	this->activeOperations.fetch_add(1);

	if (this->fd < 0) {
		this->open();
	}

	int64_t result;
	if (offset >= 0) {
		result = static_cast<int64_t>(::pread(this->fd, buffer, size, offset));
	} else {
		result = static_cast<int64_t>(::read(this->fd, buffer, size));
	}

	this->activeOperations.fetch_sub(1);

	// notify if this was the last operation
	if (this->activeOperations.load() == 0) {
		closeCondition.notify_one();
	}

	return result;
}

int64_t TransactionLogFile::writeToFile(const void* buffer, size_t size, int64_t offset) {
	this->activeOperations.fetch_add(1);

	if (this->fd < 0) {
		this->open();
	}

	int64_t bytesWritten;
	if (offset >= 0) {
		bytesWritten = static_cast<int64_t>(::pwrite(this->fd, buffer, size, offset));
	} else {
		bytesWritten = static_cast<int64_t>(::write(this->fd, buffer, size));
	}

	// update size if write was successful
	if (bytesWritten > 0) {
		if (offset >= 0) {
			// for writes at specific offset, update size if we wrote beyond current end
			size_t newEnd = static_cast<size_t>(offset) + static_cast<size_t>(bytesWritten);
			size_t currentSize = this->size.load();
			while (newEnd > currentSize && !this->size.compare_exchange_weak(currentSize, newEnd)) {
				// retry if compare_exchange_weak failed due to concurrent modification
			}
		} else {
			// for append writes, add to current size
			this->size.fetch_add(static_cast<size_t>(bytesWritten));
		}
	}

	this->activeOperations.fetch_sub(1);

	// notify if this was the last operation
	if (this->activeOperations.load() == 0) {
		closeCondition.notify_one();
	}

	return bytesWritten;
}

} // namespace rocksdb_js

#endif