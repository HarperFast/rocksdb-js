#ifndef __TRANSACTION_LOG_FILE_H__
#define __TRANSACTION_LOG_FILE_H__

#include <filesystem>

#ifdef _WIN32
	#define PLATFORM_WINDOWS
#else
	#define PLATFORM_POSIX
#endif

#ifdef PLATFORM_WINDOWS
	// prevent Windows macros from interfering with our function names
	#define WIN32_LEAN_AND_MEAN
	#include <windows.h>
	#include <io.h>
#else
	#include <sys/mman.h>
	#include <fcntl.h>
	#include <unistd.h>
#endif
#include <sys/stat.h>

namespace rocksdb_js {

struct TransactionLogFile final {
	std::filesystem::path path;
	uint32_t sequenceNumber;

#ifdef PLATFORM_WINDOWS
	HANDLE fileHandle;
	HANDLE mappingHandle;
#else
	int fd;
#endif

	char* mappedData;
	size_t mappedSize;

	TransactionLogFile(const std::filesystem::path& p, uint32_t seq)
		: path(p), sequenceNumber(seq),
#ifdef PLATFORM_WINDOWS
		  fileHandle(INVALID_HANDLE_VALUE), mappingHandle(nullptr),
#else
		  fd(-1),
#endif
		  mappedData(nullptr), mappedSize(0) {}

	// prevent copying
	TransactionLogFile(const TransactionLogFile&) = delete;
	TransactionLogFile& operator=(const TransactionLogFile&) = delete;

	~TransactionLogFile();

	void close();
	void open();
	int64_t readFromFile(void* buffer, size_t size, int64_t offset = -1);
	int64_t writeToFile(const void* buffer, size_t size, int64_t offset = -1);
};

} // namespace rocksdb_js

#endif
