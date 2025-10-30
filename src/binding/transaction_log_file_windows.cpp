#include "transaction_log_file.h"
#include "macros.h"
#include "util.h"

#ifdef PLATFORM_WINDOWS

namespace rocksdb_js {

std::string getWindowsErrorMessage(DWORD errorCode) {
    if (errorCode == 0) {
        return "No error";
    }

    LPSTR messageBuffer = nullptr;
    size_t size = FormatMessageA(
        FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        nullptr,
        errorCode,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        (LPSTR)&messageBuffer,
        0,
        nullptr
    );

    std::string message(messageBuffer, size);
    LocalFree(messageBuffer);

    // remove trailing newline characters
    while (!message.empty() && (message.back() == '\n' || message.back() == '\r')) {
        message.pop_back();
    }

    return message;
}

TransactionLogFile::TransactionLogFile(const std::filesystem::path& p, const uint32_t seq) :
	path(p),
	sequenceNumber(seq),
	fileHandle(INVALID_HANDLE_VALUE)
{}

void TransactionLogFile::close() {
	std::unique_lock<std::mutex> lock(this->fileMutex);

	if (this->fileHandle != INVALID_HANDLE_VALUE) {
		// wait for all active operations to complete
		DEBUG_LOG("TransactionLogFile::close Waiting for active operations to complete: %s (handle=%p)\n",
			this->path.string().c_str(), this->fileHandle)

		this->closeCondition.wait(lock, [this] {
			return this->activeOperations.load() == 0;
		});

		DEBUG_LOG("TransactionLogFile::close Closing file: %s (handle=%p)\n",
			this->path.string().c_str(), this->fileHandle)

		::CloseHandle(this->fileHandle);
		this->fileHandle = INVALID_HANDLE_VALUE;
	}
}

void TransactionLogFile::openFile() {
	if (this->fileHandle != INVALID_HANDLE_VALUE) {
		DEBUG_LOG("TransactionLogFile::openFile File already open: %s\n", this->path.string().c_str())
		return;
	}

	DEBUG_LOG("TransactionLogFile::openFile Opening file: %s\n", this->path.string().c_str())

	// ensure parent directory exists (may have been deleted by purge())
	auto parentPath = this->path.parent_path();
	if (!parentPath.empty()) {
		std::filesystem::create_directories(parentPath);
	}

	// open file for both reading and writing
	this->fileHandle = ::CreateFileW(this->path.wstring().c_str(),
		GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr,
		OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);

	if (this->fileHandle == INVALID_HANDLE_VALUE) {
		DWORD error = ::GetLastError();
		std::string errorMessage = getWindowsErrorMessage(error);
		DEBUG_LOG("TransactionLogFile::openFile Failed to open sequence file for read/write: %s (error=%lu: %s)\n",
			this->path.string().c_str(), error, errorMessage.c_str())
		throw std::runtime_error("Failed to open sequence file for read/write: " + this->path.string());
	}

	// Get file size
	LARGE_INTEGER fileSize;
	if (!GetFileSizeEx(this->fileHandle, &fileSize)) {
		DWORD error = ::GetLastError();
		std::string errorMessage = getWindowsErrorMessage(error);
		DEBUG_LOG("TransactionLogFile::openFile Failed to get file size: %s (error=%lu: %s)\n",
			this->path.string().c_str(), error, errorMessage.c_str())
		throw std::runtime_error("Failed to get file size: " + this->path.string());
	}
	this->size = static_cast<size_t>(fileSize.QuadPart);
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
	return convertFileTimeToSystemTime(mtime);
}

int64_t TransactionLogFile::readFromFile(void* buffer, uint32_t size, int64_t offset) {
	// acquire mutex to safely check if file needs opening and increment activeOperations
	{
		std::unique_lock<std::mutex> lock(this->fileMutex);

		// ensure file is open BEFORE incrementing activeOperations to avoid deadlock
		// with close() which waits for activeOperations == 0 while holding fileMutex
		if (this->fileHandle == INVALID_HANDLE_VALUE) {
			// open() will acquire the mutex again, but unique_lock allows recursive locking
			// actually, we need to release the lock first to avoid deadlock
			lock.unlock();
			this->open();
			lock.lock();
		}

		// increment active operations counter while holding the lock
		this->activeOperations.fetch_add(1);
	}

	if (offset >= 0) {
		if (::SetFilePointer(this->fileHandle, offset, nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
			this->activeOperations.fetch_sub(1);
			return -1;
		}
	}

	DWORD bytesRead;
	bool success = ::ReadFile(this->fileHandle, buffer, size, &bytesRead, nullptr);

	// decrement active operations counter
	this->activeOperations.fetch_sub(1);

	// notify if this was the last operation
	if (this->activeOperations.load() == 0) {
		this->closeCondition.notify_all();
	}

	return success ? static_cast<int64_t>(bytesRead) : -1;
}

int64_t TransactionLogFile::writeToFile(const void* buffer, uint32_t size, int64_t offset) {
	// acquire mutex to safely check if file needs opening and increment activeOperations
	{
		std::unique_lock<std::mutex> lock(this->fileMutex);

		// ensure file is open BEFORE incrementing activeOperations to avoid deadlock
		// with close() which waits for activeOperations == 0 while holding fileMutex
		if (this->fileHandle == INVALID_HANDLE_VALUE) {
			DEBUG_LOG("TransactionLogFile::writeToFile File not open, opening: %s\n", this->path.string().c_str())
			// open() will acquire the mutex again, so release it first
			lock.unlock();
			this->open();
			lock.lock();
		}

		// increment active operations counter while holding the lock
		this->activeOperations.fetch_add(1);
	}

	if (offset >= 0) {
		if (::SetFilePointer(this->fileHandle, offset, nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
			this->activeOperations.fetch_sub(1);
			return -1;
		}
	}

	DWORD bytesWritten;
	bool success = ::WriteFile(this->fileHandle, buffer, size, &bytesWritten, nullptr);

	// Note: size is not updated here because it will be updated by the caller
	// while holding fileMutex to ensure consistency with other metadata

	// decrement active operations counter
	this->activeOperations.fetch_sub(1);

	// notify if this was the last operation
	if (this->activeOperations.load() == 0) {
		this->closeCondition.notify_all();
	}

	return success ? static_cast<int64_t>(bytesWritten) : -1;
}

/**
 * Writes multiple buffers to the log file using WriteFileGather().
 */
int64_t TransactionLogFile::writeBatchToFile(const iovec* iovecs, int iovcnt) {
	if (iovcnt == 0) {
		return 0;
	}

	// acquire mutex to safely check if file needs opening and increment activeOperations
	{
		std::unique_lock<std::mutex> lock(this->fileMutex);

		// ensure file is open BEFORE incrementing activeOperations to avoid deadlock
		if (this->fileHandle == INVALID_HANDLE_VALUE) {
			lock.unlock();
			this->open();
			lock.lock();
		}

		// increment active operations counter while holding the lock
		this->activeOperations.fetch_add(1);
	}

	// Calculate total bytes to write
	DWORD totalBytes = 0;
	for (int i = 0; i < iovcnt; i++) {
		totalBytes += static_cast<DWORD>(iovecs[i].iov_len);
	}

	// Convert iovec array to FILE_SEGMENT_ELEMENT array
	// FILE_SEGMENT_ELEMENT requires page-aligned buffers for true scatter-gather,
	// but we can also use it with regular buffers
	std::vector<FILE_SEGMENT_ELEMENT> segments(iovcnt + 1); // +1 for null terminator
	for (int i = 0; i < iovcnt; i++) {
		segments[i].Buffer = iovecs[i].iov_base;
	}
	segments[iovcnt].Buffer = nullptr; // null terminator

	// Get current file position for overlapped structure
	LARGE_INTEGER currentPos;
	currentPos.QuadPart = 0;
	if (!SetFilePointerEx(this->fileHandle, currentPos, &currentPos, FILE_CURRENT)) {
		this->activeOperations.fetch_sub(1);
		if (this->activeOperations.load() == 0) {
			this->closeCondition.notify_all();
		}
		return -1;
	}

	// Set up overlapped structure for WriteFileGather
	OVERLAPPED overlapped = {0};
	overlapped.Offset = currentPos.LowPart;
	overlapped.OffsetHigh = currentPos.HighPart;

	// WriteFileGather requires an event for synchronous operation
	overlapped.hEvent = CreateEvent(nullptr, TRUE, FALSE, nullptr);
	if (overlapped.hEvent == nullptr) {
		this->activeOperations.fetch_sub(1);
		if (this->activeOperations.load() == 0) {
			this->closeCondition.notify_all();
		}
		return -1;
	}

	// Perform the gathered write
	bool success = ::WriteFileGather(this->fileHandle, segments.data(), totalBytes, nullptr, &overlapped);
	DWORD error = GetLastError();

	if (!success && error != ERROR_IO_PENDING) {
		CloseHandle(overlapped.hEvent);
		this->activeOperations.fetch_sub(1);
		if (this->activeOperations.load() == 0) {
			this->closeCondition.notify_all();
		}
		return -1;
	}

	// Wait for the operation to complete
	DWORD bytesWritten;
	if (!GetOverlappedResult(this->fileHandle, &overlapped, &bytesWritten, TRUE)) {
		CloseHandle(overlapped.hEvent);
		this->activeOperations.fetch_sub(1);
		if (this->activeOperations.load() == 0) {
			this->closeCondition.notify_all();
		}
		return -1;
	}

	CloseHandle(overlapped.hEvent);

	// Update file pointer
	LARGE_INTEGER newPos;
	newPos.QuadPart = currentPos.QuadPart + bytesWritten;
	SetFilePointerEx(this->fileHandle, newPos, nullptr, FILE_BEGIN);

	// Note: size is not updated here because it will be updated by the caller
	// while holding fileMutex to ensure consistency with other metadata

	this->activeOperations.fetch_sub(1);

	// notify if this was the last operation
	if (this->activeOperations.load() == 0) {
		this->closeCondition.notify_all();
	}

	return static_cast<int64_t>(bytesWritten);
}

} // namespace rocksdb_js

#endif
