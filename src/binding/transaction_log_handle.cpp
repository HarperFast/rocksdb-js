#include <algorithm>
#include "db_descriptor.h"
#include "macros.h"
#include "transaction_log_handle.h"
#include "util.h"

namespace rocksdb_js {

TransactionLogHandle::TransactionLogHandle(
	const std::shared_ptr<DBHandle>& dbHandle,
	const std::string& logName,
	bool readOnly
): dbHandle(dbHandle), logName(logName), readOnly(readOnly), transactionId(0) {
	DEBUG_LOG("%p TransactionLogHandle::TransactionLogHandle Creating TransactionLogHandle \"%s\"\n", this, logName.c_str());
	this->store = dbHandle->descriptor->resolveTransactionLogStore(logName);
}

TransactionLogHandle::~TransactionLogHandle() {
	DEBUG_LOG("%p TransactionLogHandle::~TransactionLogHandle Closing TransactionLogHandle \"%s\"\n", this, this->logName.c_str());
	this->close();
}

void TransactionLogHandle::addEntry(
	uint32_t transactionId,
	char* data,
	uint32_t size
) {
	auto dbHandle = this->dbHandle.lock();
	if (!dbHandle) {
		throw rocksdb_js::DBException("Database has been closed");
	}

	auto txnHandle = dbHandle->descriptor->transactionGet(transactionId);
	if (!txnHandle) {
		DEBUG_LOG("%p TransactionLogHandle::addEntry ERROR: Transaction id %u not found\n", this, transactionId);
		throw rocksdb_js::DBException("Transaction id " + std::to_string(transactionId) + " not found");
	}

	if (!this->store || this->store->isClosing.load(std::memory_order_relaxed)) {
		// Store was destroyed or is being closed — resolve a fresh one.
		// (The actual bind+increment in addLogEntry will re-check isClosing
		// under writeMutex for a fully atomic transition.)
		DEBUG_LOG("%p TransactionLogHandle::addEntry Store was destroyed or closing, re-resolving \"%s\"\n", this, this->logName.c_str());
		this->store = dbHandle->descriptor->resolveTransactionLogStore(this->logName);
		// Files snapshot from the old store no longer applies.
		this->cachedFiles.reset();
		this->cachedFilesVersion = 0;
	}

	// check if transaction is already bound to a different log store
	auto boundStore = txnHandle->boundLogStore.lock();
	if (boundStore && boundStore.get() != this->store.get()) {
		throw rocksdb_js::DBException("Log already bound to a transaction");
	}

	auto entry = std::make_unique<TransactionLogEntry>(this->store, data, size);
	txnHandle->addLogEntry(std::move(entry));
}

void TransactionLogHandle::close() {
	// remove this handle from the `DBHandle`
	DEBUG_LOG("%p TransactionLogHandle::close Closing TransactionLogHandle \"%s\"\n", this, this->logName.c_str());

	auto dbHandle = this->dbHandle.lock();
	if (dbHandle) {
		dbHandle->unrefLog(this->logName);
	}
}

uint64_t TransactionLogHandle::getLogFileSize(uint32_t sequenceNumber) {
	if (!this->store || this->store->isClosing.load(std::memory_order_relaxed)) return 0;

	// Fast path: refresh the per-handle file snapshot if the store's
	// filesVersion has advanced (cheap atomic compare), then read the
	// file's size atomic directly. No dataSetsMutex.
	//
	// Falls through to the slow path only when:
	//   - sequenceNumber == 0 (caller wants total size across all files)
	//   - the file isn't in our snapshot yet (race: writer hasn't bumped
	//     filesVersion yet, or we just haven't observed the bump)
	//   - the file is in our snapshot but hasn't been opened yet
	//     (size atomic is still 0; the slow path will open it)
	if (sequenceNumber > 0) {
		this->refreshFilesCache(*this->store);
		auto logFile = this->lookupCachedFile(sequenceNumber);
		if (logFile) {
			uint32_t size = logFile->size.load(std::memory_order_acquire);
			if (size > 0) {
				return size;
			}
		}
	}

	return this->store->getLogFileSize(sequenceNumber);
}

std::weak_ptr<MemoryMap> TransactionLogHandle::getMemoryMap(uint32_t sequenceNumber) {
	if (this->store && !this->store->isClosing.load(std::memory_order_relaxed)) {
		return this->store->getMemoryMap(sequenceNumber);
	}
	return std::weak_ptr<MemoryMap>(); // nullptr
}

LogPosition TransactionLogHandle::findPosition(double timestamp) {
	if (!this->store || this->store->isClosing.load(std::memory_order_relaxed)) return { 0, 0 };

	this->refreshFilesCache(*this->store);

	const auto& files = *this->cachedFiles;
	if (files.empty()) {
		// No files yet — fall back to the store's slow path which handles
		// the "current file not yet created" edge case.
		return this->store->findPositionByTimestamp(timestamp);
	}

	// Walk the local snapshot newest -> oldest, skipping files whose stored
	// timestamp is strictly greater than the requested timestamp.
	uint32_t observedCurrentSeq = this->store->currentSequenceNumber.load(std::memory_order_relaxed);
	for (auto it = files.rbegin(); it != files.rend(); ++it) {
		auto& logFile = *it;
		if (logFile->timestamp > timestamp) {
			continue;
		}
		bool isCurrent = (logFile->sequenceNumber == observedCurrentSeq);
		uint32_t mapSize = isCurrent ? this->store->maxFileSize : logFile->size.load(std::memory_order_relaxed);
		uint32_t pos = logFile->findPositionByTimestamp(timestamp, mapSize);
		if (pos > 0 && pos != 0xFFFFFFFF) {
			return { pos, logFile->sequenceNumber };
		}
		if (pos == 0xFFFFFFFF) {
			// beyond end of this file — if there's a newer cached file, jump
			// to its header; otherwise position at end of this (current) file
			if (it != files.rbegin()) {
				auto newer = it;
				--newer; // newer file in chronological order
				return { TRANSACTION_LOG_FILE_HEADER_SIZE, (*newer)->sequenceNumber };
			}
			return { logFile->size.load(std::memory_order_relaxed), logFile->sequenceNumber };
		}
		// pos == 0: timestamp predates this file's first entry; walk back
	}

	// Walked past every cached file: timestamp predates everything we have.
	// Return the header position of the oldest cached file.
	return { TRANSACTION_LOG_FILE_HEADER_SIZE, files.front()->sequenceNumber };
}

LogPosition TransactionLogHandle::getLastFlushed() {
	if (this->store && !this->store->isClosing.load(std::memory_order_relaxed)) {
		return this->store->getLastFlushedPosition();
	}
	return { 0, 0 };
}

std::weak_ptr<LogPosition> TransactionLogHandle::getLastCommittedPosition() {
	if (this->store && !this->store->isClosing.load(std::memory_order_relaxed)) {
		return this->store->getLastCommittedPosition();
	}
	return std::weak_ptr<LogPosition>(); // nullptr
}

void TransactionLogHandle::refreshFilesCache(TransactionLogStore& store) {
	uint64_t storeVersion = store.filesVersion.load(std::memory_order_acquire);
	if (storeVersion != this->cachedFilesVersion || !this->cachedFiles) {
		auto snapshot = store.getFilesSnapshot();
		this->cachedFiles = snapshot.files;
		this->cachedFilesVersion = snapshot.version;
	}
}

std::shared_ptr<TransactionLogFile> TransactionLogHandle::lookupCachedFile(uint32_t sequenceNumber) {
	if (!this->cachedFiles) return nullptr;
	const auto& files = *this->cachedFiles;
	auto it = std::lower_bound(
		files.begin(), files.end(), sequenceNumber,
		[](const std::shared_ptr<TransactionLogFile>& f, uint32_t seq) {
			return f->sequenceNumber < seq;
		});
	if (it == files.end() || (*it)->sequenceNumber != sequenceNumber) {
		return nullptr;
	}
	return *it;
}

} // namespace rocksdb_js
