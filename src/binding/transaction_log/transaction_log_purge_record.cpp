#include "transaction_log_purge_record.h"
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstring>
#include <fstream>
#include <system_error>
#include "core/encoding.h"
#include "core/platform.h"

namespace rocksdb_js {

namespace {

constexpr uint32_t PURGE_RECORD_TOKEN = 0x544C5057; // "TLPW"
constexpr uint8_t PURGE_RECORD_VERSION = 1;
constexpr uint8_t PURGE_RECORD_KNOWN_FLAG = 0x01;
constexpr uint32_t PURGE_RECORD_CHECKED_BYTES = TRANSACTION_LOG_PURGE_RECORD_SLOT_SIZE - 4;

std::atomic<uint64_t> purgeRecordTempSequence = 0;

uint32_t fnv1a(const char* data, size_t size) {
	uint32_t hash = 2166136261u;
	for (size_t i = 0; i < size; ++i) {
		hash ^= static_cast<uint8_t>(data[i]);
		hash *= 16777619u;
	}
	return hash;
}

uint32_t slotOffset(uint64_t generation) {
	return (generation & 1) ? TRANSACTION_LOG_PURGE_RECORD_SLOT_STRIDE : 0;
}

bool overwriteSlot(const std::filesystem::path& file, uint64_t generation, const char* slot) {
	std::fstream out(file, std::ios::binary | std::ios::in | std::ios::out);
	if (!out.is_open()) {
		return false;
	}
	out.seekp(slotOffset(generation));
	out.write(slot, TRANSACTION_LOG_PURGE_RECORD_SLOT_SIZE);
	out.flush();
	bool written = static_cast<bool>(out);
	out.close();
	return written && !out.fail() && syncFile(file);
}

bool createRecordFile(
	const std::filesystem::path& storePath,
	const std::filesystem::path& file,
	const TransactionLogPurgeRecord& record
) {
	auto temp = file;
	temp += ".tmp-" + std::to_string(purgeRecordTempSequence.fetch_add(1, std::memory_order_relaxed));
	std::string image = encodeTransactionLogPurgeRecordImage(record);
	{
		std::ofstream out(temp, std::ios::binary | std::ios::trunc);
		if (!out.is_open()) {
			return false;
		}
		out.write(image.data(), static_cast<std::streamsize>(image.size()));
		out.close();
		if (out.fail()) {
			std::error_code ignored;
			std::filesystem::remove(temp, ignored);
			return false;
		}
	}
	std::error_code renameError;
	if (!syncFile(temp)) {
		std::filesystem::remove(temp, renameError);
		return false;
	}
	std::filesystem::rename(temp, file, renameError);
	if (renameError) {
		std::error_code ignored;
		std::filesystem::remove(temp, ignored);
		return false;
	}
	// A filesystem without directory sync forfeits only the record's name, which reads as absent
	// (unknown) if lost.
	return syncDirectory(storePath, /*allowUnsupported=*/true);
}

} // namespace

void encodeTransactionLogPurgeRecordSlot(char* slot, const TransactionLogPurgeRecord& record) {
	std::memset(slot, 0, TRANSACTION_LOG_PURGE_RECORD_SLOT_SIZE);
	writeUint32BE(slot, PURGE_RECORD_TOKEN);
	writeUint8(slot + 4, PURGE_RECORD_VERSION);
	writeUint8(slot + 5, record.known ? PURGE_RECORD_KNOWN_FLAG : 0);
	writeUint64BE(slot + 8, record.generation);
	writeUint32BE(slot + 16, record.throughSequence);
	writeDoubleBE(slot + 20, record.watermark);
	writeUint32BE(slot + PURGE_RECORD_CHECKED_BYTES, fnv1a(slot, PURGE_RECORD_CHECKED_BYTES));
}

bool decodeTransactionLogPurgeRecordSlot(const char* slot, TransactionLogPurgeRecord& record) {
	if (readUint32BE(slot) != PURGE_RECORD_TOKEN ||
		readUint32BE(slot + PURGE_RECORD_CHECKED_BYTES) != fnv1a(slot, PURGE_RECORD_CHECKED_BYTES) ||
		readUint8(slot + 4) != PURGE_RECORD_VERSION ||
		(readUint8(slot + 5) & ~PURGE_RECORD_KNOWN_FLAG) != 0 ||
		readUint8(slot + 6) != 0 || readUint8(slot + 7) != 0) {
		return false;
	}
	uint64_t generation = readUint64BE(slot + 8);
	double watermark = readDoubleBE(slot + 20);
	// NaN would compare false against every resume key and read as "nothing purged".
	if (generation == 0 || std::isnan(watermark) || watermark < 0) {
		return false;
	}
	record.known = (readUint8(slot + 5) & PURGE_RECORD_KNOWN_FLAG) != 0;
	record.generation = generation;
	record.throughSequence = readUint32BE(slot + 16);
	record.watermark = watermark;
	return true;
}

TransactionLogPurgeRecordState decodeTransactionLogPurgeRecordImage(
	const char* data, size_t size, TransactionLogPurgeRecord& record, bool* otherSlotDamaged
) {
	TransactionLogPurgeRecord slots[2];
	bool valid[2] = { false, false };
	bool zero[2] = { true, true };
	const uint32_t offsets[2] = { 0, TRANSACTION_LOG_PURGE_RECORD_SLOT_STRIDE };
	for (size_t i = 0; i < 2; ++i) {
		if (size <= offsets[i]) {
			continue;
		}
		const char* slot = data + offsets[i];
		size_t available = std::min<size_t>(size - offsets[i], TRANSACTION_LOG_PURGE_RECORD_SLOT_SIZE);
		zero[i] = std::all_of(slot, slot + available, [](char byte) { return byte == 0; });
		if (available == TRANSACTION_LOG_PURGE_RECORD_SLOT_SIZE) {
			valid[i] = decodeTransactionLogPurgeRecordSlot(slot, slots[i]);
		}
	}
	if (!valid[0] && !valid[1]) {
		return TransactionLogPurgeRecordState::Corrupt;
	}
	size_t current = !valid[0] || (valid[1] && slots[1].generation > slots[0].generation) ? 1 : 0;
	record = slots[current];
	if (otherSlotDamaged) {
		*otherSlotDamaged = !valid[1 - current] && !zero[1 - current];
	}
	return TransactionLogPurgeRecordState::Valid;
}

std::string encodeTransactionLogPurgeRecordImage(const TransactionLogPurgeRecord& record) {
	std::string image(TRANSACTION_LOG_PURGE_RECORD_FILE_SIZE, '\0');
	encodeTransactionLogPurgeRecordSlot(image.data() + slotOffset(record.generation), record);
	return image;
}

TransactionLogPurgeRecordState readTransactionLogPurgeRecord(
	const std::filesystem::path& storePath, TransactionLogPurgeRecord& record, bool* otherSlotDamaged
) noexcept {
	try {
		auto file = storePath / TRANSACTION_LOG_PURGE_RECORD_NAME;
		std::error_code statusError;
		auto status = std::filesystem::status(file, statusError);
		if (status.type() == std::filesystem::file_type::not_found) {
			return TransactionLogPurgeRecordState::Absent;
		}
		// A read of a directory returns no bytes on some platforms, which would decode as Corrupt.
		if (statusError || status.type() != std::filesystem::file_type::regular) {
			return TransactionLogPurgeRecordState::Unreadable;
		}
		std::ifstream in(file, std::ios::binary | std::ios::in);
		if (!in.is_open()) {
			return TransactionLogPurgeRecordState::Unreadable;
		}
		char image[TRANSACTION_LOG_PURGE_RECORD_FILE_SIZE] = {};
		in.read(image, sizeof(image));
		if (in.bad()) {
			return TransactionLogPurgeRecordState::Unreadable;
		}
		return decodeTransactionLogPurgeRecordImage(image, static_cast<size_t>(in.gcount()), record, otherSlotDamaged);
	} catch (...) {
		return TransactionLogPurgeRecordState::Unreadable;
	}
}

bool writeTransactionLogPurgeRecord(
	const std::filesystem::path& storePath, const TransactionLogPurgeRecord& record
) noexcept {
	try {
		auto file = storePath / TRANSACTION_LOG_PURGE_RECORD_NAME;
		std::error_code existsError;
		bool exists = std::filesystem::exists(file, existsError);
		if (existsError) {
			return false;
		}
		if (!exists) {
			return createRecordFile(storePath, file, record);
		}
		char slot[TRANSACTION_LOG_PURGE_RECORD_SLOT_SIZE];
		encodeTransactionLogPurgeRecordSlot(slot, record);
		return overwriteSlot(file, record.generation, slot);
	} catch (...) {
		return false;
	}
}

} // namespace rocksdb_js
