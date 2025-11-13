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
{
	if (this->blockSize % 2 != 0) {
		throw std::runtime_error("Invalid block size: " + std::to_string(this->blockSize) + ". Block size must be an even number");
	}
}

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
		try {
			std::filesystem::create_directories(parentPath);
		} catch (const std::filesystem::filesystem_error& e) {
			DEBUG_LOG("%p TransactionLogFile::openFile Failed to create parent directory: %s (error=%s)\n",
				this, parentPath.string().c_str(), e.what())
			throw std::runtime_error("Failed to create parent directory: " + parentPath.string());
		}
	}

	// open file for both reading and writing
	this->fileHandle = ::CreateFileW(
		this->path.wstring().c_str(),
		GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE,
		nullptr,
		OPEN_ALWAYS,
		FILE_ATTRIBUTE_NORMAL,
		nullptr
	);

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

	// seek to end of file before writing (file pointer may have been moved by reads)
	if (::SetFilePointer(this->fileHandle, 0, nullptr, FILE_END) == INVALID_SET_FILE_POINTER) {
		DWORD error = ::GetLastError();
		std::string errorMessage = getWindowsErrorMessage(error);
		DEBUG_LOG("%p TransactionLogFile::writeBatchToFile SetFilePointer failed (error=%lu: %s)\n",
			this, error, errorMessage.c_str())
		return -1;
	}

	// emulate writev() by writing each buffer sequentially
	int64_t totalBytesWritten = 0;

	for (int i = 0; i < iovcnt; i++) {
		// skip empty iovecs
		if (iovecs[i].iov_len == 0) {
			continue;
		}

		// validate pointer
		if (iovecs[i].iov_base == nullptr) {
			DEBUG_LOG("%p TransactionLogFile::writeBatchToFile ERROR: iovec[%d] has null pointer\n", this, i)
			return totalBytesWritten > 0 ? totalBytesWritten : -1;
		}

		DWORD bytesWritten;
		bool success = ::WriteFile(
			this->fileHandle,
			iovecs[i].iov_base,
			static_cast<DWORD>(iovecs[i].iov_len),
			&bytesWritten,
			nullptr
		);

		if (!success) {
			DWORD error = ::GetLastError();
			std::string errorMessage = getWindowsErrorMessage(error);
			DEBUG_LOG("%p TransactionLogFile::writeBatchToFile WriteFile failed at iovec[%d] (error=%lu: %s, ptr=%p, len=%zu)\n",
				this, i, error, errorMessage.c_str(), iovecs[i].iov_base, iovecs[i].iov_len)
			// if we've written some data but this write failed, return what we
			// wrote; otherwise return -1 to indicate error
			return totalBytesWritten > 0 ? totalBytesWritten : -1;
		}

		totalBytesWritten += bytesWritten;

		// partial write - stop here
		if (bytesWritten < iovecs[i].iov_len) {
			DEBUG_LOG("%p TransactionLogFile::writeBatchToFile Partial write at iovec[%d]: wrote %lu of %zu bytes\n",
				this, i, bytesWritten, iovecs[i].iov_len)
			break;
		}
	}

	return totalBytesWritten;
}

int64_t TransactionLogFile::writeToFile(const void* buffer, uint32_t size, int64_t offset) {
	if (offset >= 0) {
		if (::SetFilePointer(this->fileHandle, offset, nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
			return -1;
		}
	} else {
		// offset < 0 means append to end of file
		if (::SetFilePointer(this->fileHandle, 0, nullptr, FILE_END) == INVALID_SET_FILE_POINTER) {
			return -1;
		}
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
