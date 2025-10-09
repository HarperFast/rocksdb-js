#include "transaction_log_file.h"
#include "macros.h"
#include "util.h"

#ifdef PLATFORM_WINDOWS

namespace rocksdb_js {

void TransactionLogFile::close() {
	std::unique_lock<std::mutex> lock(this->closeMutex);

	// Wait for all active operations to complete
	closeCondition.wait(lock, [this] {
		return this->activeOperations.load() == 0;
	});

	if (this->fileHandle != INVALID_HANDLE_VALUE) {
		::CloseHandle(this->fileHandle);
		this->fileHandle = INVALID_HANDLE_VALUE;
	}
}

void TransactionLogFile::open() {
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
		closeCondition.notify_all();
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
		closeCondition.notify_all();
	}

	return success ? static_cast<int64_t>(bytesWritten) : -1;
}

} // namespace rocksdb_js

#endif