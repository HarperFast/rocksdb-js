#ifndef __TRANSACTION_LOG_PURGE_RECORD_H__
#define __TRANSACTION_LOG_PURGE_RECORD_H__

#include <cstdint>
#include <filesystem>
#include <string>

namespace rocksdb_js {

#define TRANSACTION_LOG_PURGE_RECORD_NAME "txn.purged"

/**
 * A store's purge watermark as persisted in `txn.purged`. See DESIGN.md, "Purge watermark".
 */
struct TransactionLogPurgeRecord final {
	/** Every file this store's history deleted was recorded, so `watermark` bounds all of them. */
	bool known = false;
	/** Largest entry key of any file this store recorded as deleted; 0 when none. */
	double watermark = 0;
	/** Every sequence above this, below the writer's next one, must still be on disk. */
	uint32_t throughSequence = 0;
	/** Write count; the slot with the higher one is the current record. */
	uint64_t generation = 0;
};

enum class TransactionLogPurgeRecordState { Absent, Corrupt, Valid };

/** Bytes in one record slot; the file holds two, a sector apart. */
constexpr uint32_t TRANSACTION_LOG_PURGE_RECORD_SLOT_SIZE = 32;
constexpr uint32_t TRANSACTION_LOG_PURGE_RECORD_SLOT_STRIDE = 4096;
constexpr uint32_t TRANSACTION_LOG_PURGE_RECORD_FILE_SIZE =
	TRANSACTION_LOG_PURGE_RECORD_SLOT_STRIDE + TRANSACTION_LOG_PURGE_RECORD_SLOT_SIZE;

void encodeTransactionLogPurgeRecordSlot(char* slot, const TransactionLogPurgeRecord& record);

/** Returns false for a slot that is zero, torn, or semantically invalid. */
bool decodeTransactionLogPurgeRecordSlot(const char* slot, TransactionLogPurgeRecord& record);

/** The current record of a file image: the valid slot with the higher generation. */
TransactionLogPurgeRecordState decodeTransactionLogPurgeRecordImage(
	const char* data, size_t size, TransactionLogPurgeRecord& record);

/** A whole file image holding `record` in its slot and a zeroed other slot. */
std::string encodeTransactionLogPurgeRecordImage(const TransactionLogPurgeRecord& record);

/** Reads `<storePath>/txn.purged`. Any read failure other than absence is Corrupt. */
TransactionLogPurgeRecordState readTransactionLogPurgeRecord(
	const std::filesystem::path& storePath, TransactionLogPurgeRecord& record) noexcept;

/**
 * Durably writes `record` into the slot its generation selects, leaving the other slot (the previous
 * record) intact so a torn write cannot destroy it. Creates the file when absent. Returns false,
 * having made nothing durable that a caller may rely on, when any write or sync fails.
 */
bool writeTransactionLogPurgeRecord(
	const std::filesystem::path& storePath, const TransactionLogPurgeRecord& record) noexcept;

} // namespace rocksdb_js

#endif
