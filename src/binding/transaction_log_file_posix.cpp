#include "transaction_log_file.h"
#include "macros.h"
#include "util.h"

#ifdef PLATFORM_POSIX

namespace rocksdb_js {

TransactionLogFile::TransactionLogFile(const std::filesystem::path& p, const uint32_t seq) :
	path(p),
	sequenceNumber(seq),
	fd(-1),
	version(1),
	blockSize(4096),
	currentBlockSize(0),
	blockCount(0),
	size(0),
	activeOperations(0)
{}

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
	std::unique_lock<std::mutex> lock(this->fileMutex);

	if (this->fd >= 0) {
		DEBUG_LOG("TransactionLogFile::open File already open: %s\n", this->path.string().c_str())
		return;
	}

	DEBUG_LOG("TransactionLogFile::open Opening file: %s\n", this->path.string().c_str())

	// ensure parent directory exists (may have been deleted by purge())
	auto parentPath = this->path.parent_path();
	if (!parentPath.empty()) {
		std::filesystem::create_directories(parentPath);
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

	// read the file header
	char buffer[4];
	if (st.st_size == 0) {
		// file is empty, initialize it
		writeUint32BE(buffer, this->version);
		this->writeToFile(buffer, sizeof(buffer));
		writeUint32BE(buffer, this->blockSize);
		this->writeToFile(buffer, sizeof(buffer));
		this->size.store(8);
	} else if (st.st_size < 8) {
		throw std::runtime_error("File is too small to be a valid transaction log file: " + this->path.string());
	} else {
		// try to read the version and block size from the file
		int64_t result = this->readFromFile(buffer, sizeof(buffer), 0);
		if (result < 0) {
			throw std::runtime_error("Failed to read version from file: " + this->path.string());
		}
		this->version = readUint32BE(buffer);

		result = this->readFromFile(buffer, sizeof(buffer), 4);
		if (result < 0) {
			throw std::runtime_error("Failed to block size from file: " + this->path.string());
		}
		this->blockSize = readUint32BE(buffer);
	}

	uint32_t blockCount = static_cast<uint32_t>(std::ceil(static_cast<double>(this->size.load() - 8) / this->blockSize));
	this->blockCount.store(blockCount);

	DEBUG_LOG("TransactionLogFile::open Opened %s (fd=%d, version=%u, size=%zu, blockSize=%u, blockCount=%u)\n", pathStr.c_str(),
		this->fd, this->version, this->size.load(), this->blockSize, blockCount)
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
	return convertFileTimeToSystemTime(mtime);
}

/**
 * Reads data from the log file.
 */
int64_t TransactionLogFile::readFromFile(void* buffer, uint32_t size, uint32_t offset) {
	// acquire mutex to safely check if file needs opening and increment activeOperations
	{
		std::unique_lock<std::mutex> lock(this->fileMutex);

		// ensure file is open BEFORE incrementing activeOperations to avoid deadlock
		// with close() which waits for activeOperations == 0 while holding fileMutex
		if (this->fd < 0) {
			// open() will acquire the mutex again, so release it first
			lock.unlock();
			this->open();
			lock.lock();
		}

		// increment active operations counter while holding the lock
		this->activeOperations.fetch_add(1);
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
int64_t TransactionLogFile::writeToFile(const void* buffer, uint32_t size, uint32_t offset) {
	// acquire mutex to safely check if file needs opening and increment activeOperations
	{
		std::unique_lock<std::mutex> lock(this->fileMutex);

		// ensure file is open BEFORE incrementing activeOperations to avoid deadlock
		// with close() which waits for activeOperations == 0 while holding fileMutex
		if (this->fd < 0) {
			// open() will acquire the mutex again, so release it first
			lock.unlock();
			this->open();
			lock.lock();
		}

		// increment active operations counter while holding the lock
		this->activeOperations.fetch_add(1);
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
			uint32_t newEnd = static_cast<uint32_t>(offset) + static_cast<uint32_t>(bytesWritten);
			uint32_t currentSize = this->size.load();
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
