#include "transaction_log_file.h"
#include "macros.h"
#include "util.h"

#ifdef PLATFORM_WINDOWS

namespace rocksdb_js {

TransactionLogFile::TransactionLogFile(const std::filesystem::path& p, const uint32_t seq)
	: path(p), sequenceNumber(seq), fileHandle(INVALID_HANDLE_VALUE), size(0), activeOperations(0) {}

void TransactionLogFile::close() {
	std::unique_lock<std::mutex> lock(this->fileMutex);

	// Wait for all active operations to complete
	this->closeCondition.wait(lock, [this] {
		return this->activeOperations.load() == 0;
	});

	if (this->fileHandle != INVALID_HANDLE_VALUE) {
		::CloseHandle(this->fileHandle);
		this->fileHandle = INVALID_HANDLE_VALUE;
	}
}

void TransactionLogFile::open() {
	DEBUG_LOG("TransactionLogFile::open Opening file: %s\n", this->path.string().c_str())

	std::unique_lock<std::mutex> lock(this->fileMutex);

	if (this->fileHandle != INVALID_HANDLE_VALUE) {
		return;
	}

	// open file for both reading and writing
	this->fileHandle = ::CreateFileW(this->path.c_str(), GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr,
		OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);

	if (this->fileHandle == INVALID_HANDLE_VALUE) {
		throw std::runtime_error("Failed to open sequence file for read/write: " + this->path.string());
	}

	// Get file size
	LARGE_INTEGER fileSize;
	if (!GetFileSizeEx(this->fileHandle, &fileSize)) {
		throw std::runtime_error("Failed to get file size: " + this->path.string());
	}
	this->size.store(static_cast<size_t>(fileSize.QuadPart));

	DEBUG_LOG("TransactionLogFile::open Opened %s (handle=%p, size=%zu)\n",
		this->path.string().c_str(), this->fileHandle, this->size.load());
}

std::chrono::system_clock::time_point TransactionLogFile::getLastWriteTime() {
	std::unique_lock<std::mutex> lock(this->fileMutex);

	// Wait for all active operations to complete
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

	// Convert file_time to system_clock time_point
	auto mtime_sys = std::chrono::system_clock::time_point(
		std::chrono::duration_cast<std::chrono::system_clock::duration>(
			mtime.time_since_epoch())
	);

	return mtime_sys;
}

int64_t TransactionLogFile::readFromFile(void* buffer, size_t size, int64_t offset) {
	// Increment active operations counter
	this->activeOperations.fetch_add(1);

	// Ensure file is open
	if (this->fileHandle == INVALID_HANDLE_VALUE) {
		this->open();
	}

	if (offset >= 0) {
		if (::SetFilePointer(this->fileHandle, offset, nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
			this->activeOperations.fetch_sub(1);
			return -1;
		}
	}

	DWORD bytesRead;
	bool success = ::ReadFile(this->fileHandle, buffer, size, &bytesRead, nullptr);

	// Decrement active operations counter
	this->activeOperations.fetch_sub(1);

	// Notify if this was the last operation
	if (this->activeOperations.load() == 0) {
		this->closeCondition.notify_all();
	}

	return success ? static_cast<int64_t>(bytesRead) : -1;
}

int64_t TransactionLogFile::writeToFile(const void* buffer, size_t size, int64_t offset) {
	// Increment active operations counter
	this->activeOperations.fetch_add(1);

	// Ensure file is open
	if (this->fileHandle == INVALID_HANDLE_VALUE) {
		this->open();
	}

	if (offset >= 0) {
		if (::SetFilePointer(this->fileHandle, offset, nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
			this->activeOperations.fetch_sub(1);
			return -1;
		}
	}

	DWORD bytesWritten;
	bool success = ::WriteFile(this->fileHandle, buffer, size, &bytesWritten, nullptr);

	if (success && bytesWritten > 0) {
		// Update size if write was successful
		if (offset >= 0) {
			// For writes at specific offset, update size if we wrote beyond current end
			size_t newEnd = static_cast<size_t>(offset) + static_cast<size_t>(bytesWritten);
			size_t currentSize = this->size.load();
			while (newEnd > currentSize && !this->size.compare_exchange_weak(currentSize, newEnd)) {
				// Retry if compare_exchange_weak failed due to concurrent modification
			}
		} else {
			// For append writes, add to current size
			this->size.fetch_add(static_cast<size_t>(bytesWritten));
		}
	}

	// Decrement active operations counter
	this->activeOperations.fetch_sub(1);

	// Notify if this was the last operation
	if (this->activeOperations.load() == 0) {
		this->closeCondition.notify_all();
	}

	return success ? static_cast<int64_t>(bytesWritten) : -1;
}

} // namespace rocksdb_js

#endif