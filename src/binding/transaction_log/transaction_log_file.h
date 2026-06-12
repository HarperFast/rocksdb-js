#ifndef __TRANSACTION_LOG_FILE_H__
#define __TRANSACTION_LOG_FILE_H__

#include <chrono>
#include <filesystem>
#include <mutex>
#include <map>
#include <atomic>
#include "core/debug.h"
#include "core/encoding.h"
#include "core/exception.h"
#include "core/platform.h"

#ifdef _WIN32
	#define PLATFORM_WINDOWS
#else
	#define PLATFORM_POSIX
#endif

#ifdef PLATFORM_WINDOWS
	// prevent Windows macros from interfering with our function names
	#define WIN32_LEAN_AND_MEAN
	#ifndef NOMINMAX
		#define NOMINMAX
	#endif
	#include <windows.h>
	#include <io.h>

	// define iovec for Windows compatibility
	struct iovec {
		void* iov_base;
		size_t iov_len;
	};
#else
	#include <fcntl.h>
	#include <unistd.h>
	#include <sys/uio.h>
	#include <sys/mman.h>
#endif
#include <sys/stat.h>

#define TRANSACTION_LOG_ENABLE_ANONYMOUS_OVERLAY 1
#define TRANSACTION_LOG_TOKEN 0x574f4f46
#define TRANSACTION_LOG_FILE_TIMESTAMP_POSITION 5
#define TRANSACTION_LOG_FILE_HEADER_SIZE 13
#define TRANSACTION_LOG_ENTRY_HEADER_SIZE 13
#define TRANSACTION_LOG_ENTRY_LAST_FLAG 0x01

#ifdef ROCKSDB_JS_NATIVE_TESTS
// Forward declaration so that the friend designation inside namespace
// rocksdb_js can refer to the global-scope test accessor.
struct WriteBatchToFileTestAccessor;
#endif

namespace rocksdb_js {

// forward declarations
struct MemoryMap;
struct TransactionLogEntryBatch;

struct TransactionLogFile final {
	/**
	 * The path to the transaction log file.
	 */
	std::filesystem::path path;

	/**
	 * The sequence number of the transaction log file.
	 */
	uint32_t sequenceNumber;

#ifdef PLATFORM_WINDOWS
	/**
	 * The Windows file handle for the transaction log file.
	 */
	HANDLE fileHandle = INVALID_HANDLE_VALUE;
#else
	/**
	 * The POSIX file descriptor for the transaction log file.
	 */
	int fd = -1;
#endif

	/**
	 * The version of the file format.
	 */
	uint8_t version = 1;

	/**
	 * The timestamp of the most recent transaction log batch that has been
	 * written to the file.
	 */
	double timestamp;

	/**
	 * The size of the file in bytes.
	 */
	std::atomic<uint32_t> size = 0;

	/**
	 * The size of the file at the last flush operation.
	 */
	uint32_t lastFlushedSize = 0;

	/**
	 * The time of the last write to this file, kept in-memory to avoid a
	 * stat() syscall on every commit for the maxAgeThreshold check.
	 * Set once in open() (now for new files, mtime for existing ones) and
	 * updated after each successful writeEntries() call.
	 */
	std::chrono::system_clock::time_point fileLastWriteTime;

	/**
	 * The memory map of the file.
	 */
	std::shared_ptr<MemoryMap> memoryMap = nullptr;

#if TRANSACTION_LOG_ENABLE_ANONYMOUS_OVERLAY && defined(PLATFORM_POSIX)
	/**
	 * The file size at which the last MAP_FIXED overlay was applied over the
	 * anonymous base mapping. Used to avoid redundant mmap calls; only
	 * re-overlay when the file has grown past the current overlay's page
	 * boundary.
	 */
	std::atomic<uint32_t> lastOverlaySize = 0;
#endif

	/**
	 * The mutex used to protect the file (open/close, read/write, etc).
	 */
	std::mutex fileMutex;

	std::map<double, uint32_t> positionByTimestampIndex;
	uint32_t lastIndexedPosition = TRANSACTION_LOG_FILE_TIMESTAMP_POSITION;
	std::mutex indexMutex;

	/**
	 * True once an entry batch has been appended to this file since it was (re)opened.
	 * findPositionByTimestamp() only corrects this->size down to the true written extent (the
	 * reopen path, where size is seeded from a memory-map-padded on-disk size) while this is
	 * false — i.e. during startup replay, when there are no concurrent writers. Once appends
	 * begin, a zero timestamp encountered while indexing is a transient artifact of the reader's
	 * memory-map view lagging a concurrent append (size is bumped only after the bytes are
	 * written), so the read path must NOT mutate the append-owned size — doing so truncates the
	 * counter and freezes the index, intermittently hiding committed entries (HarperFast/harper#1148).
	 */
	std::atomic<bool> hasAppendedSinceOpen = false;

	TransactionLogFile(const std::filesystem::path& p, const uint32_t seq);

	// prevent copying
	TransactionLogFile(const TransactionLogFile&) = delete;
	TransactionLogFile& operator=(const TransactionLogFile&) = delete;

	~TransactionLogFile();

	/**
	 * Closes the log file.
	 */
	void close();

	/**
	 * Flushes any buffered data to disk.
	 */
	void flush();

	/**
	 * Gets the last write time of the log file or throws an error if the file
	 * does not exist.
	 */
	std::chrono::system_clock::time_point getLastWriteTime();

	/**
	 * Checks if the log file is currently open.
	 */
	inline bool isOpen() const {
#ifdef PLATFORM_WINDOWS
		return this->fileHandle != INVALID_HANDLE_VALUE;
#else
		return this->fd != -1;
#endif
	}

	/**
	 * Opens the log file for reading and writing.
	 */
 	void open(const double latestTimestamp);

	/**
	 * Open-time crash recovery for the v1 format. Scans the file's framing and,
	 * if a torn/partial entry is found at the tail (e.g. an O_APPEND short write
	 * interrupted by a crash), truncates the file back to the last valid entry
	 * boundary and flushes. If a framing break is found mid-file with valid
	 * entries still following it, the file is left intact — truncating would
	 * discard committed/replicated entries — and the break is logged so the
	 * reader's per-entry guards can surface it. Must be called after open() and
	 * before the file receives any appends; only meaningful for the active
	 * (current) log file.
	 */
	void recoverTail();

	/**
	 * Closes the log file and removes it.
	 *
	 * @returns `true` if the file was removed, `false` if it did not exist.
	 */
	bool removeFile();

	/**
	 * Writes a batch of transaction log entries to the log file.
	 *
	 * @param batch The batch of entries to write with state tracking.
	 * @param maxFileSize The maximum file size limit (0 = no limit).
	 */
	void writeEntries(TransactionLogEntryBatch& batch, const uint32_t maxFileSize = 0);

	/**
	 * Return a memory map of the file and mark it as in use
	 */
	std::shared_ptr<MemoryMap> getMemoryMap(uint32_t fileSize);

	/**
	 * Hints the kernel that this log's file-backed pages are cold (MADV_COLD),
	 * so they are reclaimed first under memory pressure without being freed.
	 * Scoped to the file-backed `[0, actualSize)` region only (page-floored) —
	 * never the MAP_PRIVATE|MAP_ANONYMOUS zero-fill overlay tail, where eviction
	 * would be destructive. Non-destructive and idempotent, so it is safe under
	 * the concurrent, not-perfectly-sequential reader pattern (replication +
	 * real-time consumers reading the same log at different offsets): a re-read
	 * of a not-yet-reclaimed cold page just re-activates it for free.
	 *
	 * No-op on kernels without MADV_COLD (< 5.4, latched on EINVAL), on macOS,
	 * and on Windows.
	 *
	 * @returns The number of bytes advised (0 if nothing was advised).
	 */
	size_t adviseCold();

	/**
	 * On POSIX, extends the MAP_FIXED file overlay to cover any new pages
	 * written since the last overlay. Called after writes that grow the file
	 * so that cached JS buffers see the new data without re-acquiring.
	 * No-op on Windows where the file is pre-extended to maxFileSize.
	 */
#if TRANSACTION_LOG_ENABLE_ANONYMOUS_OVERLAY
	void updateMemoryMapOverlay();
#endif

	/**
	 * Finds the position in this log file with the oldest transaction that is equal to, or newer than, the provided timestamp.
	 */
	uint32_t findPositionByTimestamp(double timestamp, uint32_t mapSize);

	/**
	 * Platform specific function that writes data to the log file.
	 */
	int64_t writeToFile(const void* buffer, uint32_t size, int64_t offset = -1);

#ifdef ROCKSDB_JS_NATIVE_TESTS
	// Expose writeBatchToFile to the gtest test accessor without pulling
	// gtest headers into the production build.
	friend struct ::WriteBatchToFileTestAccessor;

	/**
	 * Resets the process-global MADV_COLD-unsupported latch (see adviseCold) so
	 * that each test starts from a known state. Test-only.
	 */
	static void resetAdviseColdSupportForTests();
#endif

private:
	/**
	 * Latches `true` if madvise(MADV_COLD) ever returns EINVAL (kernel < 5.4),
	 * after which adviseCold() no-ops without issuing the syscall. Process-global
	 * because the kernel either supports the advice or it does not.
	 */
	static std::atomic<bool> madvColdUnsupported;

	/**
	 * Platform specific function that opens the log file for reading and writing.
	 */
	void openFile();

	/**
	 * Platform specific function that reads data from the log file.
	 */
	int64_t readFromFile(void* buffer, uint32_t size, int64_t offset = -1);

	/**
	 * Platform specific function that writes multiple buffers to the log file.
	 *
	 * NOTE: `iovecs` is non-const and may be mutated on partial writes (the
	 * partially-written iovec is advanced in place). Callers must not reuse
	 * the array after this returns. Taking a mutable pointer lets us avoid
	 * copying into a scratch buffer on the hot write path.
	 *
	 * POSIX advances partially-written iovecs in place; Windows writes from
	 * local state and leaves the array untouched. Callers must treat it as
	 * consumed in either case.
	 */
	int64_t writeBatchToFile(iovec* iovecs, int iovcnt);

	/**
	 * Platform specific function that truncates the file to `newSize` bytes and
	 * flushes the change to disk so a subsequent crash cannot resurrect the
	 * dropped bytes. Returns `true` on success. POSIX-only effect; a no-op
	 * returning `false` on Windows, which pre-extends and zero-pads its log
	 * files (torn tails are handled there by the zero-padding end marker).
	 */
	bool truncateFile(uint32_t newSize);

	/**
	 * Writes a batch of transaction log entries to the log file using version 1
	 * of the transaction log file format.
	 *
	 * @param batch The batch of entries to write with state tracking.
	 * @param maxFileSize The maximum file size limit (0 = no limit).
	 */
	void writeEntriesV1(TransactionLogEntryBatch& batch, const uint32_t maxFileSize);
};

struct MemoryMap final {
	/**
	 * The memory map of the file.
	 */
	void* map = nullptr;

	/**
	 * The size of the memory map that has been mapped.
	 **/
	uint32_t mapSize = 0;

	/**
	 * The size of the file (while it is being written, this is the max file size, but when done, it can't expand, so we set the file size)
	 **/
	uint32_t fileSize = 0;

	MemoryMap(void* map, uint32_t mapSize)
		: map(map), mapSize(mapSize), fileSize(mapSize) {}

	~MemoryMap() {
		DEBUG_LOG("MemoryMap::~MemoryMap map=%p, mapSize=%u\n", this->map, this->mapSize);
#ifdef PLATFORM_WINDOWS
		if (this->map != nullptr) {
			::UnmapViewOfFile(this->map);
		}
#else
		if (this->map != nullptr) {
			::munmap(this->map, this->mapSize);
		}
#endif
	}
};

} // namespace rocksdb_js

#endif
