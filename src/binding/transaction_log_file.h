#ifndef __TRANSACTION_LOG_FILE_H__
#define __TRANSACTION_LOG_FILE_H__

#include <filesystem>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace rocksdb_js {

struct TransactionLogFile final {
	std::filesystem::path path;
	uint32_t sequenceNumber;
	int fd;
	char* mappedData;
	size_t mappedSize;

	TransactionLogFile(const std::filesystem::path& p, uint32_t seq)
		: path(p), sequenceNumber(seq),
		  fd(-1), mappedData(nullptr), mappedSize(0) {} // Remove: isOpen(false)

	// prevent copying
	TransactionLogFile(const TransactionLogFile&) = delete;
	TransactionLogFile& operator=(const TransactionLogFile&) = delete;

	~TransactionLogFile();

	void close();
	void open();
	ssize_t read(void* buffer, size_t size, off_t offset = -1);
	ssize_t write(const void* buffer, size_t size, off_t offset = -1);
};

} // namespace rocksdb_js

#endif