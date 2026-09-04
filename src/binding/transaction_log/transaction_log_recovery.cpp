#include "transaction_log/transaction_log_recovery.h"
#include "transaction_log/transaction_log_file.h" // header-size constants, TransactionLogFile
#include "core/encoding.h"                         // readDoubleBE / readUint32BE
#include "core/exception.h"
#include <algorithm>
#include <cstring>
#include <mutex>
#include <vector>

namespace rocksdb_js {

namespace {

// Number of consecutive well-formed frames that, on their own, signal that real
// log data has resumed after a framing break. Combined with the "chain reaches
// EOF" signal below, this distinguishes mid-file corruption (valid entries still
// follow — must NOT truncate) from a torn tail (only partial bytes follow).
constexpr int RESYNC_MIN_FRAMES = 8;

// Sequential window for nearby success-path headers and for the corruption-only
// byte search in findFramingResumeOffset. Heap-allocated: 64 KiB on the stack is
// hostile to musl/small-stack threads.
constexpr uint32_t RESYNC_WINDOW = 65536;

struct ScanReader {
	TransactionLogReadFn read;
	void* context;
	uint32_t fileSize;
	std::vector<char> window;
	uint32_t windowStart = 0;
	uint32_t windowLen = 0;

	void readExact(uint32_t offset, void* dest, uint32_t n) const {
		if (!read(context, offset, dest, n)) {
			throw DBException("Failed to read transaction log during recovery scan");
		}
	}

	// End of the nonzero bytes: where a pre-extended file's zero padding starts. A
	// frame chain landing exactly here is as conclusive as one landing on EOF,
	// which padding never lets it reach. Computed on first use, reading backwards.
	uint32_t nonzeroEnd() {
		if (nonzeroEndKnown) {
			return nonzeroEndValue;
		}
		std::vector<char> tail(RESYNC_WINDOW);
		uint32_t end = fileSize;
		while (end > 0) {
			uint32_t len = std::min(RESYNC_WINDOW, end);
			readExact(end - len, tail.data(), len);
			for (uint32_t i = len; i > 0; --i) {
				if (tail[i - 1] != 0) {
					nonzeroEndValue = end - len + i;
					nonzeroEndKnown = true;
					return nonzeroEndValue;
				}
			}
			end -= len;
		}
		nonzeroEndValue = 0;
		nonzeroEndKnown = true;
		return 0;
	}

	bool nonzeroEndKnown = false;
	uint32_t nonzeroEndValue = 0;

	// Sequential headers within 64 KiB of the current window refill from the
	// next header. A larger gap is a payload skip: read exactly 13 bytes so
	// that payload is not pulled in.
	void readHeaderAt(uint32_t pos, char* dest) {
		if (pos >= windowStart &&
			pos + TRANSACTION_LOG_ENTRY_HEADER_SIZE <= windowStart + windowLen) {
			std::memcpy(dest, window.data() + (pos - windowStart), TRANSACTION_LOG_ENTRY_HEADER_SIZE);
			return;
		}
		const bool nearby =
			windowLen > 0 && pos <= windowStart + windowLen + RESYNC_WINDOW;
		if (nearby) {
			if (window.size() < RESYNC_WINDOW) {
				window.resize(RESYNC_WINDOW);
			}
			windowStart = pos;
			windowLen = std::min(RESYNC_WINDOW, fileSize - pos);
			readExact(windowStart, window.data(), windowLen);
			std::memcpy(dest, window.data(), TRANSACTION_LOG_ENTRY_HEADER_SIZE);
			return;
		}
		readExact(pos, dest, TRANSACTION_LOG_ENTRY_HEADER_SIZE);
		if (window.size() < TRANSACTION_LOG_ENTRY_HEADER_SIZE) {
			window.resize(TRANSACTION_LOG_ENTRY_HEADER_SIZE);
		}
		windowStart = pos;
		windowLen = TRANSACTION_LOG_ENTRY_HEADER_SIZE;
		std::memcpy(window.data(), dest, TRANSACTION_LOG_ENTRY_HEADER_SIZE);
	}
};

bool headerLooksLikeFrame(const char* header, uint32_t pos, uint32_t fileSize) {
	if (readDoubleBE(header) == 0) {
		return false;
	}
	uint32_t length = readUint32BE(header + 8);
	if (length == 0) {
		return false;
	}
	return static_cast<uint64_t>(pos) + TRANSACTION_LOG_ENTRY_HEADER_SIZE + length <= fileSize;
}

// Returns the first offset in [from, fileSize) where valid log data resumes:
// either a run of at least RESYNC_MIN_FRAMES well-formed frames, or any run that
// lands exactly on the written extent (EOF, or the start of a pre-extended
// file's zero padding). Returns 0 when nothing resumes. Sequential candidate
// offsets are served from a 64 KiB window; chain hops (HEADER+length) read a
// 13-byte header so a large payload is not pulled in. A failed read throws — it
// must not look like "no resume".
uint32_t findFramingResumeOffset(ScanReader& source, uint32_t from, bool endIsWrittenExtent) {
	std::vector<char> window(RESYNC_WINDOW);
	uint32_t windowStart = 0;
	uint32_t windowLen = 0;
	char headerBuf[TRANSACTION_LOG_ENTRY_HEADER_SIZE];

	auto reachesWrittenExtent = [&](uint32_t pos) -> bool {
		return endIsWrittenExtent && (pos == source.fileSize || pos == source.nonzeroEnd());
	};

	auto loadHeader = [&](uint32_t pos, const char*& out) -> bool {
		if (static_cast<uint64_t>(pos) + TRANSACTION_LOG_ENTRY_HEADER_SIZE > source.fileSize) {
			return false;
		}
		if (pos >= windowStart &&
			pos + TRANSACTION_LOG_ENTRY_HEADER_SIZE <= windowStart + windowLen) {
			out = window.data() + (pos - windowStart);
			return true;
		}
		source.readExact(pos, headerBuf, TRANSACTION_LOG_ENTRY_HEADER_SIZE);
		out = headerBuf;
		return true;
	};

	for (uint32_t start = from;
		 static_cast<uint64_t>(start) + TRANSACTION_LOG_ENTRY_HEADER_SIZE <= source.fileSize;
		 ++start) {
		if (start < windowStart || start + TRANSACTION_LOG_ENTRY_HEADER_SIZE > windowStart + windowLen) {
			windowStart = start;
			windowLen = std::min(RESYNC_WINDOW, source.fileSize - start);
			source.readExact(windowStart, window.data(), windowLen);
		}

		const char* header = nullptr;
		if (!loadHeader(start, header) || !headerLooksLikeFrame(header, start, source.fileSize)) {
			continue;
		}

		uint32_t pos = start + TRANSACTION_LOG_ENTRY_HEADER_SIZE + readUint32BE(header + 8);
		int frames = 1;
		if (frames >= RESYNC_MIN_FRAMES || reachesWrittenExtent(pos)) {
			return start;
		}
		while (loadHeader(pos, header) && headerLooksLikeFrame(header, pos, source.fileSize)) {
			pos += TRANSACTION_LOG_ENTRY_HEADER_SIZE + readUint32BE(header + 8);
			if (++frames >= RESYNC_MIN_FRAMES || reachesWrittenExtent(pos)) {
				return start;
			}
		}
	}
	return 0;
}

} // namespace

uint32_t findFramingResumeOffset(
	uint32_t fileSize, TransactionLogReadFn read, void* context, uint32_t from,
	bool endIsWrittenExtent
) {
	if (from == 0 || from >= fileSize) {
		return 0;
	}
	ScanReader source{ read, context, fileSize, {}, 0, 0 };
	return findFramingResumeOffset(source, from, endIsWrittenExtent);
}

RecoveryScan scanTransactionLogForRecovery(
	uint32_t fileSize, TransactionLogReadFn read, void* context
) {
	uint32_t lastCompleteEnd = 0;
	uint32_t tailEntries = 0;
	double tailTimestamp = 0;
	bool tailUniformTimestamp = true;
	uint32_t firstBreak = 0;
	auto scan = [&](RecoveryScan::Kind kind, uint32_t validEnd) {
		if (firstBreak != 0) {
			kind = RecoveryScan::Kind::MidFileCorruption;
			validEnd = firstBreak;
		}
		return RecoveryScan{ kind, validEnd, lastCompleteEnd, tailEntries,
			tailEntries > 0 && tailUniformTimestamp };
	};

	if (fileSize <= TRANSACTION_LOG_FILE_HEADER_SIZE) {
		return scan(RecoveryScan::Kind::Clean, fileSize);
	}

	ScanReader source{ read, context, fileSize, {}, 0, 0 };
	char header[TRANSACTION_LOG_ENTRY_HEADER_SIZE];
	uint32_t pos = TRANSACTION_LOG_FILE_HEADER_SIZE;
	while (true) {
		if (pos == fileSize) {
			return scan(RecoveryScan::Kind::Clean, fileSize);
		}
		if (static_cast<uint64_t>(pos) + TRANSACTION_LOG_ENTRY_HEADER_SIZE > fileSize) {
			return scan(RecoveryScan::Kind::TruncateTail, pos);
		}
		source.readHeaderAt(pos, header);
		double timestamp = readDoubleBE(header);
		if (timestamp == 0) {
			// End-of-entries marker, including the zero padding of a pre-extended file.
			return scan(RecoveryScan::Kind::Clean, pos);
		}
		uint32_t length = readUint32BE(header + 8);
		if (length == 0 ||
			static_cast<uint64_t>(pos) + TRANSACTION_LOG_ENTRY_HEADER_SIZE + length > fileSize) {
			// Intact frames after the break are mid-file corruption; truncating would
			// discard them, and the committed watermark must still reach them, so the
			// walk resumes where framing does. A torn tail has nothing valid behind it.
			uint32_t resume = findFramingResumeOffset(source, pos + 1, /*endIsWrittenExtent=*/true);
			if (resume == 0) {
				return scan(RecoveryScan::Kind::TruncateTail, pos);
			}
			if (firstBreak == 0) {
				firstBreak = pos;
			}
			pos = resume;
			tailEntries = 0;
			tailUniformTimestamp = true;
			continue;
		}
		bool closesTransaction = (readUint8(header + 12) & TRANSACTION_LOG_ENTRY_LAST_FLAG) != 0;
		if (tailEntries++ == 0) {
			tailTimestamp = timestamp;
		} else if (timestamp != tailTimestamp) {
			tailUniformTimestamp = false;
		}
		pos += TRANSACTION_LOG_ENTRY_HEADER_SIZE + length;
		if (closesTransaction) {
			lastCompleteEnd = pos;
			tailEntries = 0;
			tailUniformTimestamp = true;
		}
	}
}

namespace {

bool readFromBuffer(void* context, uint32_t offset, void* dest, uint32_t n) {
	std::memcpy(dest, static_cast<const char*>(context) + offset, n);
	return true;
}

} // namespace

RecoveryScan scanTransactionLogForRecovery(const char* data, uint32_t fileSize) {
	return scanTransactionLogForRecovery(fileSize, readFromBuffer, const_cast<char*>(data));
}

uint32_t findFramingResumeOffset(
	const char* data, uint32_t fileSize, uint32_t from, bool endIsWrittenExtent
) {
	return findFramingResumeOffset(
		fileSize, readFromBuffer, const_cast<char*>(data), from, endIsWrittenExtent);
}

RecoveryScan scanTransactionLogForRecovery(TransactionLogFile& file) {
	std::lock_guard<std::mutex> lock(file.fileMutex);
	return file.scanRecoveryLocked();
}

uint32_t countTransactionLogEntries(const char* data, uint32_t fileSize) {
	if (fileSize <= TRANSACTION_LOG_FILE_HEADER_SIZE) {
		return 0;
	}

	uint32_t count = 0;
	uint32_t pos = TRANSACTION_LOG_FILE_HEADER_SIZE;
	while (static_cast<uint64_t>(pos) + TRANSACTION_LOG_ENTRY_HEADER_SIZE <= fileSize) {
		if (readDoubleBE(data + pos) == 0) {
			break;
		}
		uint32_t length = readUint32BE(data + pos + 8);
		if (length == 0 ||
			static_cast<uint64_t>(pos) + TRANSACTION_LOG_ENTRY_HEADER_SIZE + length > fileSize) {
			break;
		}
		++count;
		pos += TRANSACTION_LOG_ENTRY_HEADER_SIZE + length;
	}
	return count;
}

} // namespace rocksdb_js
