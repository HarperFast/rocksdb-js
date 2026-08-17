#ifdef _WIN32

#include <gtest/gtest.h>
#include <atomic>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "transaction_log/transaction_log_file.h"

static std::atomic<int> g_writeFileCall = 0;
static std::atomic<int> g_failWriteFileCall = 0;

extern "C" BOOL WINAPI rocksdb_js_mock_write_file(
	HANDLE file, LPCVOID buffer, DWORD size, LPDWORD bytesWritten, LPOVERLAPPED overlapped)
{
	int call = ++g_writeFileCall;
	if (call == g_failWriteFileCall.load()) {
		::SetLastError(ERROR_WRITE_FAULT);
		return FALSE;
	}
	return ::WriteFile(file, buffer, size, bytesWritten, overlapped);
}

struct EraseTailTestAccessor {
	static bool call(rocksdb_js::TransactionLogFile& file, uint32_t newSize, uint32_t entriesEnd) {
		std::lock_guard<std::mutex> lock(file.fileMutex);
		return file.eraseTail(newSize, entriesEnd);
	}
};

class TransactionLogEraseTail : public ::testing::Test {
protected:
	void TearDown() override {
		g_failWriteFileCall = 0;
		g_writeFileCall = 0;
		file.reset();
		std::error_code error;
		std::filesystem::remove(path, error);
	}

	std::filesystem::path path = std::filesystem::temp_directory_path() /
		("rocksdb-js-erase-tail-" + std::to_string(::GetCurrentProcessId()) + ".txnlog");
	std::unique_ptr<rocksdb_js::TransactionLogFile> file;
};

TEST_F(TransactionLogEraseTail, TruncatesIfZeroFillFailsAfterACompletedChunk) {
	constexpr uint32_t boundary = 128;
	constexpr uint32_t entriesEnd = boundary + (2 * 64 * 1024) + 100;
	HANDLE handle = ::CreateFileW(
		path.wstring().c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
		nullptr, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr
	);
	ASSERT_NE(handle, INVALID_HANDLE_VALUE);
	file = std::make_unique<rocksdb_js::TransactionLogFile>(path, 1);
	file->fileHandle = handle;

	std::vector<char> image(entriesEnd, 'x');
	DWORD written = 0;
	ASSERT_TRUE(::WriteFile(handle, image.data(), static_cast<DWORD>(image.size()), &written, nullptr));
	ASSERT_EQ(written, static_cast<DWORD>(image.size()));

	file->size = entriesEnd;
	ASSERT_NE(file->getMemoryMap(entriesEnd, true), nullptr);

	g_writeFileCall = 0;
	g_failWriteFileCall = 3;
	EXPECT_TRUE(EraseTailTestAccessor::call(*file, boundary, entriesEnd));

	LARGE_INTEGER physicalSize;
	ASSERT_TRUE(::GetFileSizeEx(handle, &physicalSize));
	EXPECT_EQ(physicalSize.QuadPart, boundary);
}

#endif
