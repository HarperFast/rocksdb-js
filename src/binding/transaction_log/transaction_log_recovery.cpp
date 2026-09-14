#include "transaction_log/transaction_log_recovery.h"
#include "transaction_log/transaction_log_file.h" // header-size constants, TransactionLogFile
#include "core/encoding.h"                         // readDoubleBE / readUint32BE
#include "core/exception.h"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <limits>
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
// byte search in validFramingResumes. Heap-allocated: 64 KiB on the stack is
// hostile to musl/small-stack threads.
constexpr uint32_t RESYNC_WINDOW = 65536;

struct ScanDeadlineReached {};

struct ScanReader {
	TransactionLogReadFn read;
	void* context;
	uint32_t fileSize;
	std::optional<std::chrono::steady_clock::time_point> deadline;
	std::vector<char> window;
	uint32_t windowStart = 0;
	uint32_t windowLen = 0;
	uint64_t bytesRead = 0;

	void readExact(uint32_t offset, void* dest, uint32_t n) {
		if (deadline && std::chrono::steady_clock::now() >= *deadline) {
			throw ScanDeadlineReached{};
		}
		if (!read(context, offset, dest, n)) {
			throw DBException("Failed to read transaction log during recovery scan");
		}
		bytesRead += n;
	}

	// True when no complete frame can begin in [from, fileSize): too short to hold
	// a header, or all zero (a frame header opens with a non-zero big-endian
	// timestamp). Fixed-block reads, so one tail never becomes one allocation.
	bool tailCannotHoldAFrame(uint32_t from) {
		if (fileSize - from < TRANSACTION_LOG_ENTRY_HEADER_SIZE) {
			return true;
		}
		if (window.size() < RESYNC_WINDOW) {
			window.resize(RESYNC_WINDOW);
		}
		for (uint32_t at = from; at < fileSize; ) {
			uint32_t chunk = std::min(RESYNC_WINDOW, fileSize - at);
			readExact(at, window.data(), chunk);
			windowStart = at;
			windowLen = chunk;
			for (uint32_t i = 0; i < chunk; ++i) {
				if (window[i] != 0) {
					return false;
				}
			}
			at += chunk;
		}
		return true;
	}

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

// Returns true if valid log data resumes at some offset in [from, fileSize):
// either a run of at least RESYNC_MIN_FRAMES well-formed frames, or any run that
// lands exactly on EOF. Sequential candidate offsets are served from a 64 KiB
// window; chain hops (HEADER+length) read a 13-byte header so a large payload is
// not pulled in. A failed read throws — it must not look like "no resume".
bool validFramingResumes(ScanReader& source, uint32_t from) {
	std::vector<char> window(RESYNC_WINDOW);
	uint32_t windowStart = 0;
	uint32_t windowLen = 0;
	char headerBuf[TRANSACTION_LOG_ENTRY_HEADER_SIZE];

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
		if (frames >= RESYNC_MIN_FRAMES || pos == source.fileSize) {
			return true;
		}
		while (loadHeader(pos, header) && headerLooksLikeFrame(header, pos, source.fileSize)) {
			pos += TRANSACTION_LOG_ENTRY_HEADER_SIZE + readUint32BE(header + 8);
			if (++frames >= RESYNC_MIN_FRAMES || pos == source.fileSize) {
				return true;
			}
		}
	}
	return false;
}

} // namespace

RecoveryScan scanTransactionLogForRecovery(
	uint32_t fileSize,
	TransactionLogReadFn read,
	void* context,
	double plausibleBound,
	std::optional<std::chrono::steady_clock::time_point> deadline,
	bool requirePaddedTail
) {
	uint32_t lastCompleteEnd = 0;
	uint32_t tailEntries = 0;
	double tailTimestamp = 0;
	bool tailUniformTimestamp = true;
	double maxTimestamp = 0;
	double maxImplausibleTimestamp = 0;
	ScanReader source{ read, context, fileSize, deadline, {}, 0, 0 };
	auto scan = [&](RecoveryScan::Kind kind, uint32_t validEnd) {
		return RecoveryScan{ kind, validEnd, lastCompleteEnd, tailEntries,
			tailEntries > 0 && tailUniformTimestamp, maxTimestamp, maxImplausibleTimestamp,
			fileSize, source.bytesRead };
	};

	if (fileSize <= TRANSACTION_LOG_FILE_HEADER_SIZE) {
		return scan(RecoveryScan::Kind::Clean, fileSize);
	}

	char header[TRANSACTION_LOG_ENTRY_HEADER_SIZE];
	uint32_t pos = TRANSACTION_LOG_FILE_HEADER_SIZE;
	// Recovery's heuristics answer where it is safe to truncate, which is not a
	// proof that no durable key remains.
	auto terminate = [&](RecoveryScan::Kind kind, uint32_t at) {
		if (requirePaddedTail && !source.tailCannotHoldAFrame(at)) {
			return scan(RecoveryScan::Kind::MidFileCorruption, at);
		}
		return scan(kind, at);
	};
	try {
		while (true) {
			if (pos == fileSize) {
				return scan(RecoveryScan::Kind::Clean, fileSize);
			}
			if (static_cast<uint64_t>(pos) + TRANSACTION_LOG_ENTRY_HEADER_SIZE > fileSize) {
				return terminate(RecoveryScan::Kind::TruncateTail, pos);
			}
			source.readHeaderAt(pos, header);
			double timestamp = readDoubleBE(header);
			if (timestamp == 0) {
				// A resume cannot hide in zeros, so the per-byte search is skipped.
				if (requirePaddedTail) {
					return terminate(RecoveryScan::Kind::Clean, pos);
				}
				if (validFramingResumes(source, pos + 1)) {
					return scan(RecoveryScan::Kind::MidFileCorruption, pos);
				}
				return scan(RecoveryScan::Kind::Clean, pos);
			}
			uint32_t length = readUint32BE(header + 8);
			if (length == 0 ||
				static_cast<uint64_t>(pos) + TRANSACTION_LOG_ENTRY_HEADER_SIZE + length > fileSize) {
				if (requirePaddedTail) {
					return terminate(RecoveryScan::Kind::TruncateTail, pos);
				}
				if (validFramingResumes(source, pos + 1)) {
					return scan(RecoveryScan::Kind::MidFileCorruption, pos);
				}
				return scan(RecoveryScan::Kind::TruncateTail, pos);
			}
			bool closesTransaction = (readUint8(header + 12) & TRANSACTION_LOG_ENTRY_LAST_FLAG) != 0;
			if (timestamp > plausibleBound) {
				if (timestamp > maxImplausibleTimestamp) {
					maxImplausibleTimestamp = timestamp;
				}
			} else if (timestamp > maxTimestamp) {
				maxTimestamp = timestamp;
			}
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
	} catch (const ScanDeadlineReached&) {
		return scan(RecoveryScan::Kind::Incomplete, pos);
	}
}

namespace {

bool readFromBuffer(void* context, uint32_t offset, void* dest, uint32_t n) {
	std::memcpy(dest, static_cast<const char*>(context) + offset, n);
	return true;
}

bool readFromStream(void* context, uint32_t offset, void* dest, uint32_t n) {
	auto& input = *static_cast<std::ifstream*>(context);
	input.clear();
	input.seekg(offset, std::ios::beg);
	if (!input) {
		return false;
	}
	input.read(static_cast<char*>(dest), n);
	return input.gcount() == static_cast<std::streamsize>(n);
}

} // namespace

RecoveryScan scanTransactionLogForRecovery(
	const char* data,
	uint32_t fileSize,
	double plausibleBound,
	std::optional<std::chrono::steady_clock::time_point> deadline,
	bool requirePaddedTail
) {
	return scanTransactionLogForRecovery(fileSize, readFromBuffer, const_cast<char*>(data),
		plausibleBound, deadline, requirePaddedTail);
}

namespace {

// The extent of an already-open stream, so the bytes classified are the bytes
// of the object the handle refers to. A stat of the path followed by an open is
// two different objects.
uint32_t streamExtent(std::ifstream& input, const std::filesystem::path& path) {
	input.clear();
	input.seekg(0, std::ios::end);
	if (!input) {
		throw DBException("Failed to size transaction log for scan: " + path.string());
	}
	std::streamoff extent = input.tellg();
	if (extent < 0) {
		throw DBException("Failed to size transaction log for scan: " + path.string());
	}
	if (static_cast<uint64_t>(extent) > std::numeric_limits<uint32_t>::max()) {
		throw DBException("Transaction log is too large to scan: " + path.string());
	}
	return static_cast<uint32_t>(extent);
}

} // namespace

RecoveryScan scanTransactionLogForFloor(
	const std::filesystem::path& path,
	uint32_t retiredAppendBoundary,
	double plausibleBound,
	std::optional<std::chrono::steady_clock::time_point> deadline
) {
	auto outOfTime = [](uint32_t extent) {
		return RecoveryScan{ RecoveryScan::Kind::Incomplete, TRANSACTION_LOG_FILE_HEADER_SIZE,
			0, 0, false, 0, 0, extent, 0 };
	};
	if (deadline && std::chrono::steady_clock::now() >= *deadline) {
		return outOfTime(0);
	}
	std::ifstream input(path, std::ios::binary | std::ios::in);
	if (!input.is_open()) {
		throw DBException("Failed to open transaction log for recovery scan: " + path.string());
	}

	uint32_t extent = streamExtent(input, path);
	if (retiredAppendBoundary > 0) {
		if (retiredAppendBoundary > extent) {
			throw TransactionLogAppendBoundaryException(
				"Transaction log append boundary exceeds physical extent: " + path.string());
		}
		extent = retiredAppendBoundary;
	}
	if (extent == 0) {
		// A just-created segment has no keys yet, and no header to validate.
		return RecoveryScan{ RecoveryScan::Kind::Clean, 0, 0, 0, false, 0, 0, 0, 0 };
	}

	char header[TRANSACTION_LOG_FILE_HEADER_SIZE];
	if (deadline && std::chrono::steady_clock::now() >= *deadline) {
		return outOfTime(extent);
	}
	if (extent < TRANSACTION_LOG_FILE_HEADER_SIZE ||
		!readFromStream(&input, 0, header, TRANSACTION_LOG_FILE_HEADER_SIZE)) {
		throw DBException("Failed to read transaction log header: " + path.string());
	}
	if (readUint32BE(header) != TRANSACTION_LOG_TOKEN) {
		throw TransactionLogFormatException("Invalid transaction log file: " + path.string());
	}
	uint8_t version = readUint8(header + 4);
	if (version != 1) {
		throw TransactionLogFormatException(
			"Unsupported transaction log file version: " + std::to_string(version));
	}

	auto scan = scanTransactionLogForRecovery(extent, readFromStream, &input, plausibleBound,
		deadline, /*requirePaddedTail=*/true);
	uint32_t remaining = extent - scan.validEnd;
	if (scan.kind == RecoveryScan::Kind::TruncateTail &&
		remaining > 0 && remaining < TRANSACTION_LOG_ENTRY_HEADER_SIZE
	) {
		// Too short to hold a frame either way, so the floor is safe. Zeros there
		// are the end-of-entries marker (Windows pads a segment to its mapped
		// size), not a torn write, and reporting a torn tail would emit a warning
		// about a healthy file.
		char padding[TRANSACTION_LOG_ENTRY_HEADER_SIZE];
		if (!readFromStream(&input, scan.validEnd, padding, remaining)) {
			throw DBException("Failed to read transaction log padding: " + path.string());
		}
		if (std::all_of(padding, padding + remaining, [](char byte) { return byte == 0; })) {
			scan.kind = RecoveryScan::Kind::Clean;
		}
	}
	return scan;
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
