#include "transaction_log_file.h"
#include "macros.h"
#include "util.h"

#ifdef PLATFORM_WINDOWS

namespace rocksdb_js {

std::string getWindowsErrorMessage(DWORD errorCode);

TransactionLogFile::TransactionLogFile(const std::filesystem::path& p, const uint32_t seq) :
	path(p),
	sequenceNumber(seq)
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
	DEBUG_LOG(stderr, "%p TransactionLogFile::openFile File size: %zu file path: %s\n",
		this, this->size, this->path.string().c_str());
	// On Windows, we have to create the full file size for memory maps, and it is zero-padded, so the act of indexing allows us to find
	// the end, and adjust the real size accordingly.
	// TODO: Future optimization is to only do this if the file is a multiple of the page size, and ensure
	// files that are expanded to a memory page are memory page aligned, with (this->size & 0xFFF) == 0
	if (this->size > 0) {
		this->findPositionByTimestamp(0, this->size);
		DEBUG_LOG("%p TransactionLogFile::openFile New file size: %zu file path: %s\n",
			this, this->size, this->path.string().c_str());
	}
}

std::weak_ptr<MemoryMap> TransactionLogFile::getMemoryMap(uint32_t fileSize) {
	DEBUG_LOG("%p TransactionLogFile::getMemoryMap open size: %u\n", this, fileSize);
	if (this->memoryMap) {
		if (this->memoryMap->mapSize >= fileSize) {
			// existing memory map will work
			this->memoryMap->fileSize = fileSize;
			return this->memoryMap;
		}
		DEBUG_LOG("%p TransactionLogFile::getMemoryMap existing memory map was too small: %u\n", this, memoryMap->mapSize);
		// this memory map is not big enough, need to create a new one
	}
	DEBUG_LOG("%p TransactionLogFile::getMemoryMap creating new memory map: %u\n", this, fileSize);
	// In windows, we can not map beyond the size of the file (without using driver-level APIs that directly call procedures
	// in NT.DLL). So we must expand the file to the full size before we can map it.
	if (fileSize > this->size)
	{
		LARGE_INTEGER currentPos;
		LARGE_INTEGER distanceToMove;
		// First, we have to get the current position, so we can restore it (if we get to a point where no other code relies on position, could remove this)
		distanceToMove.QuadPart = 0; // We want to move 0 bytes to query current position
		if (!::SetFilePointerEx(this->fileHandle, distanceToMove, &currentPos, FILE_CURRENT)) {
			DWORD error = ::GetLastError();
			std::string errorMessage = getWindowsErrorMessage(error);
			DEBUG_LOG("%p TransactionLogFile::getMemoryMap Failed to SetFilePointerEx: %s (error=%lu: %s)\n",
			this, this->path.string().c_str(), error, errorMessage.c_str())
			return std::weak_ptr<MemoryMap>();
		}

		// Move to the new file size
		LARGE_INTEGER newSize;
		newSize.QuadPart = fileSize;
		if (!SetFilePointerEx(this->fileHandle, newSize, NULL, FILE_BEGIN)) {
			DWORD error = ::GetLastError();
			std::string errorMessage = getWindowsErrorMessage(error);
			DEBUG_LOG("%p TransactionLogFile::getMemoryMap Failed to SetFilePointerEx to new size: %s (error=%lu: %s)\n",
			this, this->path.string().c_str(), error, errorMessage.c_str())
			return std::weak_ptr<MemoryMap>();
		}

		// Set the End of File with the new file size
		if (!SetEndOfFile(this->fileHandle)) {
			DWORD error = ::GetLastError();
			std::string errorMessage = getWindowsErrorMessage(error);
			DEBUG_LOG("%p TransactionLogFile::getMemoryMap Failed to SetEndOfFile: %s (error=%lu: %s)\n",
			this, this->path.string().c_str(), error, errorMessage.c_str())
		}

		// Restore original position
		if (!SetFilePointerEx(this->fileHandle, currentPos, NULL, FILE_BEGIN)) {
			DWORD error = ::GetLastError();
			std::string errorMessage = getWindowsErrorMessage(error);
			DEBUG_LOG("%p TransactionLogFile::getMemoryMap Failed to restore position: %s (error=%lu: %s)\n",
			this, this->path.string().c_str(), error, errorMessage.c_str())
		}
	}
	HANDLE mh;
	mh = CreateFileMappingW(this->fileHandle, NULL, PAGE_READONLY, 0, fileSize, NULL);
	if (!mh)
	{
		DWORD error = ::GetLastError();
		std::string errorMessage = getWindowsErrorMessage(error);
		DEBUG_LOG("%p TransactionLogFile::getMemoryMap Failed to CreateFileMapping: %s (error=%lu: %s)\n",
		this, this->path.string().c_str(), error, errorMessage.c_str())
		return std::weak_ptr<MemoryMap>();
	}
	// map the memory object into our address space
	// note that MapViewOfFileEx can be used if we wanted to suggest an address
	void* map = MapViewOfFile(mh, FILE_MAP_READ, 0, 0, fileSize);
	if (!map) {
		DWORD error = ::GetLastError();
		std::string errorMessage = getWindowsErrorMessage(error);
		DEBUG_LOG("%p TransactionLogFile::getMemoryMap Failed to MapViewOfFile: %s (error=%lu: %s)\n",
		this, this->path.string().c_str(), error, errorMessage.c_str())
		CloseHandle(mh);
		return std::weak_ptr<MemoryMap>();
	}
	DEBUG_LOG("%p TransactionLogFile::getMemoryMap mapped to: %p\n", this, map);
	this->memoryMap = std::make_shared<MemoryMap>(map, fileSize);
	this->memoryMap->fileSize = fileSize;
	this->memoryMap->mapHandle = mh;
	return this->memoryMap;
}


int64_t TransactionLogFile::readFromFile(void* buffer, uint32_t size, int64_t offset) {
	if (offset >= 0 && ::SetFilePointer(this->fileHandle, offset, nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
		return -1;
	}

	DWORD bytesRead;
	bool success = ::ReadFile(this->fileHandle, buffer, size, &bytesRead, nullptr);
	return success ? static_cast<int64_t>(bytesRead) : -1;
}

bool TransactionLogFile::removeFile() {
	std::unique_lock<std::mutex> lock(this->fileMutex);

	if (this->fileHandle != INVALID_HANDLE_VALUE) {
		DEBUG_LOG("%p TransactionLogFile::removeFile Closing file: %s (handle=%p)\n",
			this, this->path.string().c_str(), this->fileHandle)
		::CloseHandle(this->fileHandle);
		this->fileHandle = INVALID_HANDLE_VALUE;
	}

	auto removed = std::filesystem::remove(this->path);
	if (!removed) {
		DEBUG_LOG("%p TransactionLogFile::removeFile File does not exist: %s\n",
			this, this->path.string().c_str())
		return false;
	}

	DEBUG_LOG("%p TransactionLogFile::removeFile Removed file %s\n",
		this, this->path.string().c_str())
	return true;
}

int64_t TransactionLogFile::writeBatchToFile(const iovec* iovecs, int iovcnt) {
	if (iovcnt == 0) {
		return 0;
	}

	// seek to current size before writing (file pointer may have been moved by reads)
	if (::SetFilePointer(this->fileHandle, this->size, nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
		DWORD error = ::GetLastError();
		std::string errorMessage = getWindowsErrorMessage(error);
		DEBUG_LOG("%p TransactionLogFile::writeBatchToFile SetFilePointer failed (error=%lu: %s)\n",
			this, error, errorMessage.c_str())
		return -1;
	}

	// emulate writev() by writing each buffer sequentially
	// all entry data is now owned by C++ (not Node.js buffers), so safe to access directly
	int64_t totalBytesWritten = 0;

	for (int i = 0; i < iovcnt; i++) {
		if (iovecs[i].iov_len == 0) {
			continue;
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
			DEBUG_LOG("%p TransactionLogFile::writeBatchToFile WriteFile failed (error=%lu: %s, iovec %d/%d)\n",
				this, error, errorMessage.c_str(), i, iovcnt)
			// if we've written some data but this write failed, return what we
			// wrote; otherwise return -1 to indicate error
			return totalBytesWritten > 0 ? totalBytesWritten : -1;
		}

		totalBytesWritten += bytesWritten;

		// partial write - stop here
		if (bytesWritten < iovecs[i].iov_len) {
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

MemoryMap::~MemoryMap() {
	if (this->map != nullptr) {
		::UnmapViewOfFile(this->map);
	}
	if (this->mapHandle != INVALID_HANDLE_VALUE) {
		::CloseHandle(this->mapHandle);
	}
}

} // namespace rocksdb_js

#endif
