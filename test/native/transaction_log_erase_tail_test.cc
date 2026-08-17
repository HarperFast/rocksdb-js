#ifdef _WIN32

#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
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

#endif
