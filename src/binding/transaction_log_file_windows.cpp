#include "transaction_log_file.h"
#include "macros.h"
#include "util.h"

#ifdef PLATFORM_WINDOWS

namespace rocksdb_js {

void TransactionLogFile::close() {
	if (this->mappedData) {
		::UnmapViewOfFile(this->mappedData);
		this->mappedData = nullptr;
	}
	if (this->mappingHandle) {
		::CloseHandle(this->mappingHandle);
		this->mappingHandle = nullptr;
	}
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
	this->mappedSize = static_cast<size_t>(fileSize.QuadPart);

	if (this->mappedSize > 0) {
		// Create file mapping
		this->mappingHandle = ::CreateFileMappingW(this->fileHandle, nullptr, PAGE_READONLY, 0, 0, nullptr);
		if (!this->mappingHandle) {
			throw std::runtime_error("Failed to create file mapping: " + this->path.string());
		}

		// Map view of file
		this->mappedData = static_cast<char*>(::MapViewOfFile(this->mappingHandle, FILE_MAP_READ, 0, 0, 0));
		if (!this->mappedData) {
			::CloseHandle(this->mappingHandle);
			this->mappingHandle = nullptr;
			throw std::runtime_error("Failed to map view of file: " + this->path.string());
		}
	}

	DEBUG_LOG("TransactionLogFile::open Opened %s (handle=%p)\n", this->path.string().c_str(), this->fileHandle);
}

int64_t TransactionLogFile::readFromFile(void* buffer, size_t size, int64_t offset) {
	if (this->fileHandle == INVALID_HANDLE_VALUE) {
		this->open();
	}

	if (offset >= 0) {
		if (::SetFilePointer(this->fileHandle, offset, nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
			return -1;
		}
	}

	DWORD bytesRead;
	if (::ReadFile(this->fileHandle, buffer, size, &bytesRead, nullptr)) {
		return static_cast<int64_t>(bytesRead);
	}
	return -1;
}

int64_t TransactionLogFile::writeToFile(const void* buffer, size_t size, int64_t offset) {
	if (this->fileHandle == INVALID_HANDLE_VALUE) {
		this->open();
	}

	if (offset >= 0) {
		if (::SetFilePointer(this->fileHandle, offset, nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
			return -1;
		}
	}

	DWORD bytesWritten;
	if (::WriteFile(this->fileHandle, buffer, size, &bytesWritten, nullptr)) {
		return static_cast<int64_t>(bytesWritten);
	}
	return -1;
}

} // namespace rocksdb_js

#endif
