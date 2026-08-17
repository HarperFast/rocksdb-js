#ifdef _WIN32

#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "core/encoding.h"
#include "transaction_log/transaction_log_file.h"

struct EraseTailTestAccessor {
	static bool call(rocksdb_js::TransactionLogFile& file, uint32_t newSize, uint32_t entriesEnd) {
		std::lock_guard<std::mutex> lock(file.fileMutex);
		return file.eraseTail(newSize, entriesEnd);
	}

	static bool truncate(rocksdb_js::TransactionLogFile& file, uint32_t newSize) {
		std::lock_guard<std::mutex> lock(file.fileMutex);
		return file.truncateFile(newSize);
	}

	static void seedIndex(rocksdb_js::TransactionLogFile& file, double timestamp, uint32_t position) {
		std::lock_guard<std::mutex> lock(file.indexMutex);
		file.positionByTimestampIndex[timestamp] = position;
		file.lastIndexedPosition = position;
	}

	static bool hasResetIndex(rocksdb_js::TransactionLogFile& file) {
		std::lock_guard<std::mutex> lock(file.indexMutex);
		return file.positionByTimestampIndex.empty() &&
			file.lastIndexedPosition == rocksdb_js::TRANSACTION_LOG_FILE_TIMESTAMP_POSITION;
	}
};

class TransactionLogEraseTail : public ::testing::Test {
protected:
	void TearDown() override {
		file.reset();
		std::error_code error;
		std::filesystem::remove(path, error);
	}

	std::filesystem::path path = std::filesystem::temp_directory_path() /
		("rocksdb-js-erase-tail-" + std::to_string(::GetCurrentProcessId()) + ".txnlog");
	std::unique_ptr<rocksdb_js::TransactionLogFile> file;

	void createMappedFile(uint32_t size) {
		HANDLE handle = ::CreateFileW(
			path.wstring().c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
			nullptr, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr
		);
		ASSERT_NE(handle, INVALID_HANDLE_VALUE);
		file = std::make_unique<rocksdb_js::TransactionLogFile>(path, 1);
		file->fileHandle = handle;

		std::vector<char> image(size, 'x');
		DWORD written = 0;
		ASSERT_TRUE(::WriteFile(handle, image.data(), static_cast<DWORD>(image.size()), &written, nullptr));
		ASSERT_EQ(written, static_cast<DWORD>(image.size()));
		file->size = size;
		ASSERT_NE(file->getMemoryMap(size, true), nullptr);
	}

	void expectPhysicalSize(uint32_t expected) {
		LARGE_INTEGER physicalSize;
		ASSERT_TRUE(::GetFileSizeEx(file->fileHandle, &physicalSize));
		EXPECT_EQ(physicalSize.QuadPart, expected);
	}
};

TEST_F(TransactionLogEraseTail, PhysicallyTruncatesTheDiscardedRange) {
	constexpr uint32_t boundary = 128;
	constexpr uint32_t entriesEnd = boundary + (2 * 64 * 1024) + 100;
	createMappedFile(entriesEnd);

	EXPECT_TRUE(EraseTailTestAccessor::call(*file, boundary, entriesEnd));
	expectPhysicalSize(boundary);
}

TEST_F(TransactionLogEraseTail, PhysicallyTruncatesATornTail) {
	constexpr uint32_t boundary = 128;
	constexpr uint32_t fileSize = 512;
	createMappedFile(fileSize);

	EXPECT_TRUE(EraseTailTestAccessor::truncate(*file, boundary));
	expectPhysicalSize(boundary);
}

TEST_F(TransactionLogEraseTail, TornTailRecoveryClearsThePreRecoveryIndex) {
	constexpr uint32_t completeEnd = rocksdb_js::TRANSACTION_LOG_FILE_HEADER_SIZE +
		rocksdb_js::TRANSACTION_LOG_ENTRY_HEADER_SIZE + 4;
	constexpr uint32_t tornSize = completeEnd + rocksdb_js::TRANSACTION_LOG_ENTRY_HEADER_SIZE + 2;
	std::vector<char> image(tornSize, 0);
	rocksdb_js::writeUint32BE(image.data(), rocksdb_js::TRANSACTION_LOG_TOKEN);
	rocksdb_js::writeUint8(image.data() + 4, 1);
	rocksdb_js::writeDoubleBE(image.data() + 5, 1.0);
	rocksdb_js::writeDoubleBE(image.data() + rocksdb_js::TRANSACTION_LOG_FILE_HEADER_SIZE, 2.0);
	rocksdb_js::writeUint32BE(image.data() + rocksdb_js::TRANSACTION_LOG_FILE_HEADER_SIZE + 8, 4);
	rocksdb_js::writeUint8(image.data() + rocksdb_js::TRANSACTION_LOG_FILE_HEADER_SIZE + 12, 1);
	rocksdb_js::writeDoubleBE(image.data() + completeEnd, 3.0);
	rocksdb_js::writeUint32BE(image.data() + completeEnd + 8, 100);

	HANDLE handle = ::CreateFileW(
		path.wstring().c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
		nullptr, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr
	);
	ASSERT_NE(handle, INVALID_HANDLE_VALUE);
	DWORD written = 0;
	ASSERT_TRUE(::WriteFile(handle, image.data(), static_cast<DWORD>(image.size()), &written, nullptr));
	ASSERT_EQ(written, static_cast<DWORD>(image.size()));

	file = std::make_unique<rocksdb_js::TransactionLogFile>(path, 1);
	file->fileHandle = handle;
	file->size = tornSize;
	file->version = 1;
	EraseTailTestAccessor::seedIndex(*file, 3.0, tornSize + 100);

	file->recoverTail();

	expectPhysicalSize(completeEnd);
	EXPECT_TRUE(EraseTailTestAccessor::hasResetIndex(*file));
}

#endif
