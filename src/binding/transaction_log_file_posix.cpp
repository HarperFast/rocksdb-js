#include "transaction_log_file.h"
#include "macros.h"
#include "util.h"

#ifdef PLATFORM_POSIX

namespace rocksdb_js {

TransactionLogFile::TransactionLogFile(const std::filesystem::path& p, const uint32_t seq)
	: path(p), sequenceNumber(seq), fd(-1), size(0), activeOperations(0) {}

/**
 * Closes the log file.
 */
void TransactionLogFile::close() {
	std::unique_lock<std::mutex> lock(this->fileMutex);

	if (this->fd >= 0) {
		DEBUG_LOG("TransactionLogFile::close Waiting for active operations to complete: %s (fd=%d)\n",
			this->path.string().c_str(), this->fd)

		// wait for all active operations to complete
		this->closeCondition.wait(lock, [this] {
			return this->activeOperations.load() == 0;
		});

		DEBUG_LOG("TransactionLogFile::close Closing file: %s (fd=%d)\n",
			this->path.string().c_str(), this->fd)

		::close(this->fd);
		this->fd = -1;
	}
}

/**
 * Opens the log file for reading and writing.
 */
void TransactionLogFile::open() {
	DEBUG_LOG("TransactionLogFile::open Opening file: %s\n", this->path.string().c_str())

	std::unique_lock<std::mutex> lock(this->fileMutex);

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

/**
 * Gets the last write time of the log file or throws an error if the file does
 * not exist.
 */
std::chrono::system_clock::time_point TransactionLogFile::getLastWriteTime() {
	std::unique_lock<std::mutex> lock(this->fileMutex);

	// wait for all active operations to complete
	this->closeCondition.wait(lock, [this] {
		return this->activeOperations.load() == 0;
	});

	if (!std::filesystem::exists(this->path)) {
		throw std::filesystem::filesystem_error(
			"File does not exist",
			this->path,
			std::make_error_code(std::errc::no_such_file_or_directory)
		);
	}

	auto mtime = std::filesystem::last_write_time(this->path);

	// convert file_time to system_clock time_point
	auto mtime_sys = std::chrono::system_clock::time_point(
		std::chrono::duration_cast<std::chrono::system_clock::duration>(
			mtime.time_since_epoch())
	);

	return mtime_sys;
}

/**
 * Reads data from the log file.
 */
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
		this->closeCondition.notify_one();
	}

	return result;
}

/**
 * Writes data to the log file.
 */
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
		this->closeCondition.notify_one();
	}

	return bytesWritten;
}

} // namespace rocksdb_js

#endif