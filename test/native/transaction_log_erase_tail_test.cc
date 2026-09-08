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
			file.lastIndexedPosition == TRANSACTION_LOG_FILE_TIMESTAMP_POSITION;
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

	// One complete entry followed by an entry header whose declared length runs
	// past end-of-file: the torn tail shape recoverTail() truncates back to
	// `completeEnd`.
	void writeTornImage(uint32_t tornSize, uint32_t completeEnd) {
		std::vector<char> image(tornSize, 0);
		rocksdb_js::writeUint32BE(image.data(), TRANSACTION_LOG_TOKEN);
		rocksdb_js::writeUint8(image.data() + 4, 1);
		rocksdb_js::writeDoubleBE(image.data() + 5, 1.0);
		rocksdb_js::writeDoubleBE(image.data() + TRANSACTION_LOG_FILE_HEADER_SIZE, 2.0);
		rocksdb_js::writeUint32BE(image.data() + TRANSACTION_LOG_FILE_HEADER_SIZE + 8, 4);
		rocksdb_js::writeUint8(image.data() + TRANSACTION_LOG_FILE_HEADER_SIZE + 12, 1);
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
	}

	// Entry A closes a transaction, entry B does not (the prefix of a batch whose
	// final entry never landed), then a torn entry header. `recoverTail()` must
	// end this file at A, not at B.
	void writeUnclosedTornImage(uint32_t entryAEnd, uint32_t entryBEnd, uint32_t tornSize) {
		std::vector<char> image(tornSize, 0);
		rocksdb_js::writeUint32BE(image.data(), TRANSACTION_LOG_TOKEN);
		rocksdb_js::writeUint8(image.data() + 4, 1);
		rocksdb_js::writeDoubleBE(image.data() + 5, 1.0);

		rocksdb_js::writeDoubleBE(image.data() + TRANSACTION_LOG_FILE_HEADER_SIZE, 2.0);
		rocksdb_js::writeUint32BE(image.data() + TRANSACTION_LOG_FILE_HEADER_SIZE + 8, 4);
		rocksdb_js::writeUint8(image.data() + TRANSACTION_LOG_FILE_HEADER_SIZE + 12, TRANSACTION_LOG_ENTRY_LAST_FLAG);

		rocksdb_js::writeDoubleBE(image.data() + entryAEnd, 3.0);
		rocksdb_js::writeUint32BE(image.data() + entryAEnd + 8, 4);
		rocksdb_js::writeUint8(image.data() + entryAEnd + 12, 0);

		if (tornSize > entryBEnd) {
			rocksdb_js::writeDoubleBE(image.data() + entryBEnd, 3.0);
			rocksdb_js::writeUint32BE(image.data() + entryBEnd + 8, 100);
		}

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
	}

	// Entry A closes a transaction, entry B does not, and the file ends cleanly
	// there — the crash-mid-batch shape discardUnclosedTransaction erases.
	void writeUnclosedCleanImage(uint32_t entryAEnd, uint32_t entryBEnd) {
		writeUnclosedTornImage(entryAEnd, entryBEnd, entryBEnd);
	}

	void reopenReadOnly() {
		::CloseHandle(file->fileHandle);
		file->fileHandle = ::CreateFileW(
			path.wstring().c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
			nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr
		);
		ASSERT_NE(file->fileHandle, INVALID_HANDLE_VALUE);
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

// A live mapping makes Windows refuse to shrink the file (sections are
// mandatory), which is routine: a reader in this process — or another — can
// hold one while a writer opens and recovers. The torn bytes must still be
// neutralized, because appends resume at `size` and a later shorter batch would
// leave them reading as an entry rather than the zero end-of-entries marker.
TEST_F(TransactionLogEraseTail, TornTailRecoveryZeroFillsWhenTheFileCannotShrink) {
	constexpr uint32_t completeEnd = TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 4;
	constexpr uint32_t tornSize = completeEnd + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 2;
	writeTornImage(tornSize, completeEnd);

	// A second owner of the mapping is what truncateFile() refuses on.
	std::shared_ptr<rocksdb_js::MemoryMap> pinnedMap = file->getMemoryMap(tornSize, true);
	ASSERT_NE(pinnedMap, nullptr);

	file->recoverTail();

	EXPECT_EQ(file->size.load(), completeEnd);
	expectPhysicalSize(tornSize);
	std::vector<char> tail(tornSize - completeEnd, '\xff');
	DWORD read = 0;
	LARGE_INTEGER offset;
	offset.QuadPart = completeEnd;
	ASSERT_TRUE(::SetFilePointerEx(file->fileHandle, offset, nullptr, FILE_BEGIN));
	ASSERT_TRUE(::ReadFile(file->fileHandle, tail.data(), static_cast<DWORD>(tail.size()), &read, nullptr));
	ASSERT_EQ(read, static_cast<DWORD>(tail.size()));
	for (char byte : tail) {
		EXPECT_EQ(byte, 0) << "torn bytes survived recovery";
	}
}

// The zero-fill can fail part-way (a full or failing volume). Keeping the old
// logical `size` there would put the next append past the zero marker the
// partial fill left behind — invisible to every reader — so recovery retires
// the segment instead: `size` drops to the boundary and appends are refused,
// which makes the store rotate on the next write. A read-only handle is the
// deterministic stand-in for a write that cannot land.
TEST_F(TransactionLogEraseTail, TornTailRecoveryRetiresTheSegmentWhenZeroingFails) {
	constexpr uint32_t completeEnd = TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 4;
	constexpr uint32_t tornSize = completeEnd + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 2;
	writeTornImage(tornSize, completeEnd);

	reopenReadOnly();

	file->recoverTail();

	EXPECT_EQ(file->size.load(), completeEnd);
	EXPECT_TRUE(file->appendBoundaryLost.load());
	expectPhysicalSize(tornSize);
}

// A retired segment cannot erase anything, so the boundary it retires at is its
// only eraser: whole entries of an interrupted transaction left inside the
// logical extent would be glued onto the next segment's first batch by that
// batch's last-entry flag, merging two source transactions into one for every
// consumer that groups on it (AGENTS invariant 14, which spans rotations).
TEST_F(TransactionLogEraseTail, RetirementEndsOnATransactionBoundary) {
	constexpr uint32_t entrySize = TRANSACTION_LOG_ENTRY_HEADER_SIZE + 4;
	constexpr uint32_t entryAEnd = TRANSACTION_LOG_FILE_HEADER_SIZE + entrySize;
	constexpr uint32_t entryBEnd = entryAEnd + entrySize;
	constexpr uint32_t tornSize = entryBEnd + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 2;
	writeUnclosedTornImage(entryAEnd, entryBEnd, tornSize);
	reopenReadOnly();

	file->recoverTail();

	EXPECT_TRUE(file->appendBoundaryLost.load());
	EXPECT_EQ(file->size.load(), entryAEnd) << "retired inside an unclosed transaction";
	expectPhysicalSize(tornSize);
}

TEST_F(TransactionLogEraseTail, TornTailRecoveryClearsThePreRecoveryIndex) {
	constexpr uint32_t completeEnd = TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 4;
	constexpr uint32_t tornSize = completeEnd + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 2;
	writeTornImage(tornSize, completeEnd);
	EraseTailTestAccessor::seedIndex(*file, 3.0, tornSize + 100);

	file->recoverTail();

	expectPhysicalSize(completeEnd);
	EXPECT_TRUE(EraseTailTestAccessor::hasResetIndex(*file));
}

// discardUnclosedTransaction can retire too: the erase is the same Windows
// zero-fill, and when it fails part-way it drops `size` to the boundary and
// refuses appends rather than reporting success. Treating that as "nothing
// happened" left the pre-recovery timestamp index pointing at or past the new
// end, so a query would start past end-of-entries and return nothing for
// entries that still exist.
TEST_F(TransactionLogEraseTail, UnclosedTransactionDiscardRetiresAndClearsTheIndex) {
	constexpr uint32_t entrySize = TRANSACTION_LOG_ENTRY_HEADER_SIZE + 4;
	constexpr uint32_t entryAEnd = TRANSACTION_LOG_FILE_HEADER_SIZE + entrySize;
	constexpr uint32_t entryBEnd = entryAEnd + entrySize;
	writeUnclosedCleanImage(entryAEnd, entryBEnd);
	EraseTailTestAccessor::seedIndex(*file, 3.0, entryBEnd);
	reopenReadOnly();

	file->recoverTail();

	EXPECT_TRUE(file->appendBoundaryLost.load());
	EXPECT_EQ(file->size.load(), entryAEnd);
	EXPECT_TRUE(EraseTailTestAccessor::hasResetIndex(*file));
	expectPhysicalSize(entryBEnd);
}

#endif
