#include "transaction_log/transaction_log_validation.h"
#include "transaction_log/transaction_log_file.h"     // header/entry size constants
#include "transaction_log/transaction_log_recovery.h" // scanTransactionLogForRecovery
#include "core/encoding.h"                            // readDoubleBE / readUint32BE
#include "core/exception.h"
#include <algorithm>
#include <cmath>
#include <cstring>
#include <fstream>
#include <memory>

namespace rocksdb_js {

namespace {

// Transaction log files did not exist before this date, so any timestamp that
// predates it indicates corruption. Milliseconds since the Unix epoch for
// 2026-01-27T00:00:00Z; mirrors MIN_VALID_TIMESTAMP in
// src/parse-transaction-log.ts — keep the two in sync.
constexpr double MIN_VALID_TIMESTAMP = 1769472000000.0;

// Only bit 0 (TRANSACTION_LOG_ENTRY_LAST_FLAG) is defined.
constexpr uint8_t VALID_FLAGS_MASK = TRANSACTION_LOG_ENTRY_LAST_FLAG;

// Per-entry anomalies are reported individually up to this cap, then
// summarized — a large corrupt file could otherwise produce millions of
// warning strings.
constexpr size_t MAX_REPORTED_ENTRY_ANOMALIES = 10;

// Size of the txn.state side file: one raw LogPosition (LOG_POSITION_SIZE in
// transaction_log_store.h, not included here to keep this translation unit
// free of RocksDB headers for the native test target).
constexpr uint64_t TXN_STATE_SIZE = 8;

std::string offsetHex(uint64_t offset) {
	char buffer[32];
	std::snprintf(buffer, sizeof(buffer), "0x%llx", static_cast<unsigned long long>(offset));
	return buffer;
}

void addEntryAnomaly(
	TransactionLogFileValidation& result,
	size_t& anomalyCount,
	std::string message
) {
	if (++anomalyCount <= MAX_REPORTED_ENTRY_ANOMALIES) {
		result.warnings.push_back(std::move(message));
	}
}

} // namespace

TransactionLogFileValidation validateTransactionLogImage(
	const char* data,
	uint32_t fileSize,
	bool strict
) {
	TransactionLogFileValidation result;

	if (fileSize < TRANSACTION_LOG_FILE_HEADER_SIZE) {
		result.errors.push_back(
			"File is too small to contain a header (" + std::to_string(fileSize) + " of " +
			std::to_string(TRANSACTION_LOG_FILE_HEADER_SIZE) + " bytes)"
		);
		result.valid = false;
		return result;
	}

	uint32_t token = readUint32BE(data);
	if (token != TRANSACTION_LOG_TOKEN) {
		result.errors.push_back("Invalid header token " + offsetHex(token));
	}

	uint8_t version = static_cast<uint8_t>(data[4]);
	if (version != 1) {
		result.errors.push_back(
			"Unsupported transaction log file version: " + std::to_string(version)
		);
	}

	if (!result.errors.empty()) {
		// A wrong token or version means the framing rules below don't apply;
		// walking the body would only produce misleading follow-on errors.
		result.valid = false;
		return result;
	}

	double headerTimestamp = readDoubleBE(data + TRANSACTION_LOG_FILE_TIMESTAMP_POSITION);
	if (!std::isfinite(headerTimestamp) || headerTimestamp < MIN_VALID_TIMESTAMP) {
		result.warnings.push_back(
			"Header timestamp " + std::to_string(headerTimestamp) +
			" predates 2026-01-27 (possible corruption)"
		);
	}

	// Classify the framing with the same scan used by open-time crash recovery,
	// so validation and recovery can never disagree about what is a torn tail
	// versus mid-file corruption.
	RecoveryScan scan = scanTransactionLogForRecovery(data, fileSize);
	result.validBytes = scan.validEnd;

	switch (scan.kind) {
		case RecoveryScan::Kind::Clean:
			break;
		case RecoveryScan::Kind::TruncateTail: {
			// An all-zero tail is padding, not a torn entry: Windows pre-extends and
			// zero-pads log files, and rotation can leave fewer than
			// TRANSACTION_LOG_ENTRY_HEADER_SIZE padding bytes at the end — too few
			// for the recovery scan to see a zero timestamp there. A real torn
			// header always begins with a nonzero timestamp byte.
			bool allZeroTail = true;
			for (uint32_t i = scan.validEnd; i < fileSize; i++) {
				if (data[i] != 0) {
					allZeroTail = false;
					break;
				}
			}
			if (allZeroTail) {
				break;
			}
			std::string message =
				"Torn/partial entry at offset " + offsetHex(scan.validEnd) + " (" +
				std::to_string(fileSize - scan.validEnd) +
				" trailing bytes would be truncated by open-time recovery)";
			if (strict) {
				result.errors.push_back(std::move(message));
			} else {
				result.warnings.push_back(std::move(message));
			}
			break;
		}
		case RecoveryScan::Kind::MidFileCorruption:
			result.errors.push_back(
				"Framing break at offset " + offsetHex(scan.validEnd) +
				" with valid entries following it (mid-file corruption)"
			);
			break;
	}

	// Walk the well-formed frames (everything in [header, validEnd) is framed
	// correctly by construction) counting entries and per-entry anomalies.
	// pos is uint64_t so the bounds safety is self-evident here, not implicit
	// in the recovery scan's own widened arithmetic.
	size_t anomalyCount = 0;
	uint64_t pos = TRANSACTION_LOG_FILE_HEADER_SIZE;
	while (pos + TRANSACTION_LOG_ENTRY_HEADER_SIZE <= scan.validEnd) {
		double timestamp = readDoubleBE(data + pos);
		if (timestamp == 0) {
			// zero padding marks the end of entries (matches the reader/parser)
			break;
		}
		uint32_t length = readUint32BE(data + pos + 8);
		uint8_t flags = static_cast<uint8_t>(data[pos + 12]);

		if (!std::isfinite(timestamp) || timestamp < MIN_VALID_TIMESTAMP) {
			addEntryAnomaly(result, anomalyCount,
				"Entry " + std::to_string(result.entryCount + 1) + " at offset " + offsetHex(pos) +
				": timestamp " + std::to_string(timestamp) +
				" predates 2026-01-27 (possible corruption)");
		}
		if ((flags & ~VALID_FLAGS_MASK) != 0) {
			addEntryAnomaly(result, anomalyCount,
				"Entry " + std::to_string(result.entryCount + 1) + " at offset " + offsetHex(pos) +
				": flags " + offsetHex(flags) + " contains undefined bits");
		}

		result.entryCount++;
		pos += TRANSACTION_LOG_ENTRY_HEADER_SIZE + length;
	}

	if (anomalyCount > MAX_REPORTED_ENTRY_ANOMALIES) {
		result.warnings.push_back(
			"+" + std::to_string(anomalyCount - MAX_REPORTED_ENTRY_ANOMALIES) +
			" more entry anomalies not shown"
		);
	}

	result.valid = result.errors.empty();
	return result;
}

TransactionLogStoreValidation validateTransactionLogStore(
	const std::filesystem::path& path,
	bool strict
) {
	if (!std::filesystem::is_directory(path)) {
		throw DBException(
			"Transaction log store directory does not exist or is not a directory: " + path.string()
		);
	}

	TransactionLogStoreValidation store;
	store.path = path.string();

	bool hasStateFile = false;

	for (const auto& dirEntry : std::filesystem::directory_iterator(path)) {
		const std::string filename = dirEntry.path().filename().string();

		if (!dirEntry.is_regular_file()) {
			store.warnings.push_back("Unexpected directory entry: " + filename);
			continue;
		}

		if (filename == "txn.state") {
			hasStateFile = true;
			continue;
		}

		if (dirEntry.path().extension() != ".txnlog") {
			store.warnings.push_back("Unexpected file: " + filename);
			continue;
		}

		// the file name must be `<sequence>.txnlog` where sequence is a positive
		// integer without leading zeros — anything else was not written by the
		// store (and "01.txnlog" would duplicate "1.txnlog"'s sequence)
		const std::string stem = filename.substr(0, filename.size() - 7);
		uint64_t sequence = 0;
		bool validName =
			!stem.empty() && stem.size() <= 10 && !(stem.size() > 1 && stem[0] == '0');
		for (size_t i = 0; validName && i < stem.size(); i++) {
			if (stem[i] < '0' || stem[i] > '9') {
				validName = false;
			}
		}
		if (validName) {
			sequence = std::stoull(stem);
		}
		if (!validName || sequence == 0 || sequence > UINT32_MAX) {
			store.errors.push_back("Malformed transaction log file name: " + filename);
			continue;
		}

		TransactionLogStoreFileValidation fileValidation;
		fileValidation.file = filename;
		fileValidation.sequenceNumber = static_cast<uint32_t>(sequence);

		std::error_code sizeError;
		uint64_t size = std::filesystem::file_size(dirEntry.path(), sizeError);
		if (sizeError) {
			fileValidation.result.errors.push_back("Unable to stat file: " + sizeError.message());
		} else if (size > UINT32_MAX) {
			// the v1 format addresses offsets as uint32, so a larger file cannot
			// have been written by the store
			fileValidation.size = size;
			fileValidation.result.errors.push_back(
				"File size " + std::to_string(size) + " exceeds the 4 GiB format limit"
			);
		} else {
			fileValidation.size = size;
			std::ifstream file(dirEntry.path(), std::ios::binary | std::ios::in);
			// uninitialized buffer: read() overwrites it, and pre-zeroing a
			// multi-megabyte log file would touch every byte twice
			std::unique_ptr<char[]> image(new char[static_cast<size_t>(size)]);
			if (size > 0) {
				file.read(image.get(), static_cast<std::streamsize>(size));
			}
			if (!file) {
				fileValidation.result.errors.push_back("Unable to read file");
			} else {
				fileValidation.result =
					validateTransactionLogImage(image.get(), static_cast<uint32_t>(size), strict);
			}
		}

		fileValidation.result.valid = fileValidation.result.errors.empty();
		store.files.push_back(std::move(fileValidation));
	}

	std::sort(store.files.begin(), store.files.end(),
		[](const TransactionLogStoreFileValidation& a, const TransactionLogStoreFileValidation& b) {
			return a.sequenceNumber < b.sequenceNumber;
		});

	if (store.files.empty()) {
		store.warnings.push_back("No transaction log files found");
	}

	// Sequence continuity: purge always removes the oldest files first, so the
	// surviving sequence numbers should be contiguous. A hole means a file was
	// removed out-of-band; in strict mode (backup snapshots, which capture every
	// surviving file) that is missing data, so it escalates to an error.
	for (size_t i = 1; i < store.files.size(); i++) {
		uint32_t prev = store.files[i - 1].sequenceNumber;
		uint32_t next = store.files[i].sequenceNumber;
		if (next != prev + 1) {
			(strict ? store.errors : store.warnings).push_back(
				"Gap in log file sequence: " + std::to_string(prev + 1) +
				(next - prev == 2 ? "" : ".." + std::to_string(next - 1)) + " missing"
			);
		}
	}

	if (hasStateFile) {
		const auto statePath = path / "txn.state";
		std::error_code sizeError;
		uint64_t stateSize = std::filesystem::file_size(statePath, sizeError);
		if (sizeError) {
			store.errors.push_back("Unable to stat txn.state: " + sizeError.message());
		} else if (stateSize != TXN_STATE_SIZE) {
			store.errors.push_back(
				"txn.state is " + std::to_string(stateSize) + " bytes, expected " +
				std::to_string(TXN_STATE_SIZE)
			);
		} else {
			char stateBytes[TXN_STATE_SIZE];
			std::ifstream stateFile(statePath, std::ios::binary | std::ios::in);
			stateFile.read(stateBytes, sizeof(stateBytes));
			if (!stateFile) {
				store.errors.push_back("Unable to read txn.state");
			} else {
				// txn.state is a raw host-endian LogPosition dump (uint32
				// positionInLogFile, uint32 logSequenceNumber) — the writer
				// (databaseFlushed) writes the struct's memory directly, so decoding
				// with memcpy on the same host convention is the only decode that
				// round-trips (all supported platforms are little-endian)
				uint32_t flushedOffset = 0;
				uint32_t flushedSequence = 0;
				std::memcpy(&flushedOffset, stateBytes, 4);
				std::memcpy(&flushedSequence, stateBytes + 4, 4);

				if (flushedSequence != 0 && !store.files.empty()) {
					uint32_t newestSequence = store.files.back().sequenceNumber;
					if (flushedSequence > newestSequence) {
						// A backup snapshot captures txn.state before enumerating the log
						// files, so its flushed sequence always exists in the snapshot —
						// beyond-newest there means the newest file is missing (strict).
						(strict ? store.errors : store.warnings).push_back(
							"txn.state flushed position references sequence " +
							std::to_string(flushedSequence) + ", newer than the newest log file (" +
							std::to_string(newestSequence) + ")"
						);
					} else {
						auto it = std::find_if(store.files.begin(), store.files.end(),
							[flushedSequence](const TransactionLogStoreFileValidation& f) {
								return f.sequenceNumber == flushedSequence;
							});
						if (it != store.files.end() && flushedOffset > it->size) {
							// same snapshot-ordering argument as beyond-newest above: files
							// can only have grown after txn.state was captured, so a file
							// shorter than its flushed offset means missing data (strict)
							(strict ? store.errors : store.warnings).push_back(
								"txn.state flushed offset " + std::to_string(flushedOffset) +
								" exceeds the size of " + it->file + " (" + std::to_string(it->size) +
								" bytes)"
							);
						}
					}
				}
			}
		}
	}

	store.valid = store.errors.empty() &&
		std::all_of(store.files.begin(), store.files.end(),
			[](const TransactionLogStoreFileValidation& f) { return f.result.valid; });

	return store;
}

} // namespace rocksdb_js
