#include "transaction_log_file.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

// Platform-specific macros for file operations
#ifdef PLATFORM_WINDOWS
	#define FILE_IS_VALID(handle) \
		((handle) != INVALID_HANDLE_VALUE)

	#define FILE_GET_SIZE(handle, size) \
		do { \
			LARGE_INTEGER fileSize; \
			if (!GetFileSizeEx(handle, &fileSize)) { \
				throw std::runtime_error("Failed to get file size"); \
			} \
			size = static_cast<size_t>(fileSize.QuadPart); \
		} while(0)

	#define FILE_MMAP(handle, size, data) \
		do { \
			HANDLE mapping = CreateFileMappingW(handle, nullptr, PAGE_READONLY, 0, 0, nullptr); \
			if (!mapping) { \
				throw std::runtime_error("Failed to create file mapping"); \
			} \
			data = static_cast<char*>(MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, 0)); \
			if (!data) { \
				CloseHandle(mapping); \
				throw std::runtime_error("Failed to map view of file"); \
			} \
			this->mappingHandle = mapping; \
		} while(0)

	#define FILE_MUNMAP(data, size) \
		do { \
			if (data) { \
				UnmapViewOfFile(data); \
				data = nullptr; \
			} \
			if (this->mappingHandle) { \
				CloseHandle(this->mappingHandle); \
				this->mappingHandle = nullptr; \
			} \
		} while(0)

	#define FILE_READ(handle, buffer, size, offset) \
		[&]() -> ssize_t { \
			if (offset >= 0) { \
				if (SetFilePointer(handle, offset, nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) { \
					return -1; \
				} \
			} \
			DWORD bytesRead; \
			if (ReadFile(handle, buffer, size, &bytesRead, nullptr)) { \
				return bytesRead; \
			} \
			return -1; \
		}()

	#define FILE_WRITE(handle, buffer, size, offset) \
		[&]() -> ssize_t { \
			if (offset >= 0) { \
				if (SetFilePointer(handle, offset, nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) { \
					return -1; \
				} \
			} \
			DWORD bytesWritten; \
			if (WriteFile(handle, buffer, size, &bytesWritten, nullptr)) { \
				return bytesWritten; \
			} \
			return -1; \
		}()

#else // PLATFORM_POSIX
	#define FILE_IS_VALID(handle) \
		((handle) >= 0)

	#define FILE_GET_SIZE(handle, size) \
		do { \
			struct stat st; \
			if (::fstat(handle, &st) < 0) { \
				throw std::runtime_error("Failed to get file size"); \
			} \
			size = st.st_size; \
		} while(0)

	#define FILE_MMAP(handle, size, data) \
		do { \
			data = static_cast<char*>(::mmap(nullptr, size, PROT_READ, MAP_SHARED, handle, 0)); \
			if (data == MAP_FAILED) { \
				throw std::runtime_error("Failed to mmap file"); \
			} \
			::madvise(data, size, MADV_SEQUENTIAL); \
		} while(0)

	#define FILE_MUNMAP(data, size) \
		do { \
			if (data) { \
				::munmap(data, size); \
				data = nullptr; \
			} \
		} while(0)

	#define FILE_READ(handle, buffer, size, offset) \
		(offset >= 0 ? ::pread(handle, buffer, size, offset) : ::read(handle, buffer, size))

	#define FILE_WRITE(handle, buffer, size, offset) \
		(offset >= 0 ? ::pwrite(handle, buffer, size, offset) : ::write(handle, buffer, size))
#endif

TransactionLogFile::~TransactionLogFile() {
	this->close();
}

void TransactionLogFile::close() {
	FILE_MUNMAP(this->mappedData, this->mappedSize);

#ifdef PLATFORM_WINDOWS
	if (FILE_IS_VALID(this->fileHandle)) {
		::CloseHandle(this->fileHandle);
		this->fileHandle = INVALID_HANDLE_VALUE;
	}
#else
	if (FILE_IS_VALID(this->fd)) {
		::close(this->fd);
		this->fd = -1;
	}
#endif
}

void TransactionLogFile::open() {
#ifdef PLATFORM_WINDOWS
	if (FILE_IS_VALID(this->fileHandle)) {
		return;
	}

	// open file for both reading and writing
	this->fileHandle = CreateFileW(this->path.c_str(), GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr,
		OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);

	if (!FILE_IS_VALID(this->fileHandle)) {
		throw std::runtime_error("Failed to open sequence file for read/write: " + this->path.string());
	}

	FILE_GET_SIZE(this->fileHandle, this->mappedSize);

	if (this->mappedSize > 0) {
		FILE_MMAP(this->fileHandle, this->mappedSize, this->mappedData);
	}

	DEBUG_LOG("TransactionLogFile::open Opened %s (handle=%p)\n", this->path.string().c_str(), this->fileHandle);
#else
	if (FILE_IS_VALID(this->fd)) {
		return;
	}

	// open file for both reading and writing
	this->fd = ::open(this->path.c_str(), O_RDWR | O_CREAT, 0644);
	if (!FILE_IS_VALID(this->fd)) {
		throw std::runtime_error("Failed to open sequence file for read/write: " + this->path.string());
	}

	FILE_GET_SIZE(this->fd, this->mappedSize);

	if (this->mappedSize > 0) {
		FILE_MMAP(this->fd, this->mappedSize, this->mappedData);
	}

	DEBUG_LOG("TransactionLogFile::open Opened %s (fd=%d)\n", this->path.string().c_str(), this->fd);
#endif
}

ssize_t TransactionLogFile::read(void* buffer, size_t size, off_t offset) {
#ifdef PLATFORM_WINDOWS
	if (!FILE_IS_VALID(this->fileHandle)) {
		this->open();
	}
	return FILE_READ(this->fileHandle, buffer, size, offset);
#else
	if (!FILE_IS_VALID(this->fd)) {
		this->open();
	}
	return FILE_READ(this->fd, buffer, size, offset);
#endif
}

ssize_t TransactionLogFile::write(const void* buffer, size_t size, off_t offset) {
#ifdef PLATFORM_WINDOWS
	if (!FILE_IS_VALID(this->fileHandle)) {
		this->open();
	}
	return FILE_WRITE(this->fileHandle, buffer, size, offset);
#else
	if (!FILE_IS_VALID(this->fd)) {
		this->open();
	}
	return FILE_WRITE(this->fd, buffer, size, offset);
#endif
}

} // namespace rocksdb_js