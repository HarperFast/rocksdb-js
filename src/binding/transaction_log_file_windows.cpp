#include "transaction_log_file.h"
#include "macros.h"
#include "util.h"

#ifdef PLATFORM_WINDOWS

namespace rocksdb_js {

std::string getWindowsErrorMessage(DWORD errorCode);

TransactionLogFile::TransactionLogFile(const std::filesystem::path& p, const uint32_t seq) :
	path(p),
	sequenceNumber(seq),
	fileHandle(INVALID_HANDLE_VALUE)
{}

void TransactionLogFile::close() {
	std::unique_lock<std::mutex> lock(this->fileMutex);

	if (this->fileHandle != INVALID_HANDLE_VALUE) {
		DEBUG_LOG("%p TransactionLogFile::close Closing file: %s (handle=%p)\n",
			this, this->path.string().c_str(), this->fileHandle)
		::CloseHandle(this->fileHandle);
		this->fileHandle = INVALID_HANDLE_VALUE;
	}
}

void TransactionLogFile::openFile() {
	if (this->fileHandle != INVALID_HANDLE_VALUE) {
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
	this->fileHandle = ::CreateFileW(this->path.wstring().c_str(),
		GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr,
		OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);

	if (this->fileHandle == INVALID_HANDLE_VALUE) {
		DWORD error = ::GetLastError();
		std::string errorMessage = getWindowsErrorMessage(error);
		DEBUG_LOG("%p TransactionLogFile::openFile Failed to open sequence file for read/write: %s (error=%lu: %s)\n",
			this, this->path.string().c_str(), error, errorMessage.c_str())
		throw std::runtime_error("Failed to open sequence file for read/write: " + this->path.string());
	}

	// Get file size
	LARGE_INTEGER fileSize;
	if (!::GetFileSizeEx(this->fileHandle, &fileSize)) {
		DWORD error = ::GetLastError();
		std::string errorMessage = getWindowsErrorMessage(error);
		DEBUG_LOG("%p TransactionLogFile::openFile Failed to get file size: %s (error=%lu: %s)\n",
			this, this->path.string().c_str(), error, errorMessage.c_str())
		throw std::runtime_error("Failed to get file size: " + this->path.string());
	}
	this->size = static_cast<size_t>(fileSize.QuadPart);
}

int64_t TransactionLogFile::readFromFile(void* buffer, uint32_t size, int64_t offset) {
	if (offset >= 0 && ::SetFilePointer(this->fileHandle, offset, nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
		return -1;
	}

	DWORD bytesRead;
	bool success = ::ReadFile(this->fileHandle, buffer, size, &bytesRead, nullptr);
	return success ? static_cast<int64_t>(bytesRead) : -1;
}

int64_t TransactionLogFile::writeBatchToFile(const iovec* iovecs, int iovcnt) {
	if (iovcnt == 0) {
		return 0;
	}

	// calculate total bytes to write
	DWORD totalBytes = 0;
	for (int i = 0; i < iovcnt; i++) {
		totalBytes += static_cast<DWORD>(iovecs[i].iov_len);
	}

	// convert iovec array to FILE_SEGMENT_ELEMENT array
	// FILE_SEGMENT_ELEMENT requires page-aligned buffers for true scatter-gather,
	// but we can also use it with regular buffers
	std::vector<FILE_SEGMENT_ELEMENT> segments(iovcnt + 1); // +1 for null terminator
	for (int i = 0; i < iovcnt; i++) {
		segments[i].Buffer = iovecs[i].iov_base;
	}
	segments[iovcnt].Buffer = nullptr; // null terminator

	// get current file position for overlapped structure
	LARGE_INTEGER currentPos;
	currentPos.QuadPart = 0;
	if (!::SetFilePointerEx(this->fileHandle, currentPos, &currentPos, FILE_CURRENT)) {
		return -1;
	}

	// Set up overlapped structure for WriteFileGather
	OVERLAPPED overlapped = {0};
	overlapped.Offset = currentPos.LowPart;
	overlapped.OffsetHigh = currentPos.HighPart;

	// WriteFileGather requires an event for synchronous operation
	overlapped.hEvent = ::CreateEvent(nullptr, TRUE, FALSE, nullptr);
	if (overlapped.hEvent == nullptr) {
		return -1;
	}

	// perform the gathered write
	bool success = ::WriteFileGather(this->fileHandle, segments.data(), totalBytes, nullptr, &overlapped);
	DWORD error = ::GetLastError();

	if (!success && error != ERROR_IO_PENDING) {
		::CloseHandle(overlapped.hEvent);
		return -1;
	}

	// wait for the operation to complete
	DWORD bytesWritten;
	if (!::GetOverlappedResult(this->fileHandle, &overlapped, &bytesWritten, TRUE)) {
		::CloseHandle(overlapped.hEvent);
		return -1;
	}

	::CloseHandle(overlapped.hEvent);

	// update file pointer
	LARGE_INTEGER newPos;
	newPos.QuadPart = currentPos.QuadPart + bytesWritten;
	::SetFilePointerEx(this->fileHandle, newPos, nullptr, FILE_BEGIN);

	return static_cast<int64_t>(bytesWritten);
}

int64_t TransactionLogFile::writeToFile(const void* buffer, uint32_t size, int64_t offset) {
	if (offset >= 0 && ::SetFilePointer(this->fileHandle, offset, nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
		return -1;
	}

	DWORD bytesWritten;
	bool success = ::WriteFile(this->fileHandle, buffer, size, &bytesWritten, nullptr);
	return success ? static_cast<int64_t>(bytesWritten) : -1;
}

std::string getWindowsErrorMessage(DWORD errorCode) {
	if (errorCode == 0) {
		return "No error";
	}

	LPSTR messageBuffer = nullptr;
	size_t size = ::FormatMessageA(
		FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
		nullptr,
		errorCode,
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		(LPSTR)&messageBuffer,
		0,
		nullptr
	);

	std::string message(messageBuffer, size);
	::LocalFree(messageBuffer);

	// remove trailing newline characters
	while (!message.empty() && (message.back() == '\n' || message.back() == '\r')) {
		message.pop_back();
	}

	return message;
}

} // namespace rocksdb_js

#endif
