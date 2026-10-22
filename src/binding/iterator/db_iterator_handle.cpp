#include "iterator/db_iterator_handle.h"
#include "database/db_descriptor.h"
#include <rocksdb/version.h>
#include "core/test_seam.h"
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>

namespace rocksdb_js {

namespace {

// A transaction's write batch ignored ReadOptions bounds before RocksDB 8.10.0
// (facebook/rocksdb#11680). Either way `iterate_lower_bound` is inclusive, so
// an exclusive lower bound reached by a reverse scan is the handle's to apply.
constexpr bool WRITE_BATCH_HONORS_BOUNDS = ROCKSDB_MAJOR > 8 || (ROCKSDB_MAJOR == 8 && ROCKSDB_MINOR >= 10);

bool needsBoundCheck(const DBIteratorOptions& options, bool writeBatch) {
	return (writeBatch && !WRITE_BATCH_HONORS_BOUNDS)
		|| (options.reverse && options.exclusiveStart && options.startKeyStr != nullptr);
}

}

thread_local IteratorPoolStats iteratorPoolStats;

bool iteratorPoolEnabled() {
	if constexpr (ROCKSDB_MAJOR < 11) {
		return false;
	} else {
		// Read once: getenv races a process.env write from another thread.
		static const bool enabled = [] {
			const char* value = ::getenv("ROCKSDB_JS_ITERATOR_POOL");
			return value == nullptr || (::strcmp(value, "0") != 0 && ::strcmp(value, "false") != 0);
		}();
		return enabled;
	}
}

DBIteratorHandle::DBIteratorHandle(
	std::shared_ptr<DBHandle> dbHandle,
	DBIteratorOptions& options
) :
	dbHandle(dbHandle),
	exclusiveStart(options.exclusiveStart),
	inclusiveEnd(options.inclusiveEnd),
	reverse(options.reverse),
	values(options.values),
	needsStableValueBuffer(options.needsStableValueBuffer),
	enforceBounds(needsBoundCheck(options, false)),
	pooled(false)
{
	DEBUG_LOG("%p DBIteratorHandle::Constructor dbHandle=%p\n", this, dbHandle.get());
	this->init(options);

	this->iterator = std::unique_ptr<rocksdb::Iterator>(
		dbHandle->descriptor->db->NewIterator(
			options.readOptions,
			dbHandle->getColumnFamilyHandle()
		)
	);

	this->seek();
}

DBIteratorHandle::DBIteratorHandle(
	std::shared_ptr<TransactionHandle> txnHandle,
	DBIteratorOptions& options,
	std::shared_ptr<DBHandle> dbHandleOverride,
	bool pooled
) :
	dbHandle(dbHandleOverride ? dbHandleOverride : txnHandle->dbHandle),
	txnHandle(std::move(txnHandle)),
	exclusiveStart(options.exclusiveStart),
	inclusiveEnd(options.inclusiveEnd),
	reverse(options.reverse),
	values(options.values),
	needsStableValueBuffer(options.needsStableValueBuffer),
	enforceBounds(pooled || needsBoundCheck(options, true)),
	pooled(pooled)
{
	DEBUG_LOG("DBIteratorHandle::Constructor txnHandle=%p dbDescriptor=%p pooled=%d\n", this->txnHandle.get(), dbHandle->descriptor.get(), pooled);
	this->txnHandle->ensureSnapshot();
	if (this->txnHandle->snapshotSet) {
		options.readOptions.snapshot = this->txnHandle->txn->GetSnapshot();
	}
	if (pooled) {
		this->column = this->dbHandle->columnDescriptor;
		this->setRange(options);
		options.readOptions.max_skippable_internal_keys = POOLED_MAX_SKIPPABLE_INTERNAL_KEYS;
	} else {
		this->init(options);
	}

	this->iterator = std::unique_ptr<rocksdb::Iterator>(
		this->txnHandle->txn->GetIterator(
			options.readOptions,
			this->dbHandle->getColumnFamilyHandle()
		)
	);

	this->seek();
	if (pooled && !this->iterator->status().ok()) {
		this->fallBackToBounded(nullptr);
	}
}

DBIteratorHandle::~DBIteratorHandle() {
	this->close();
}

void DBIteratorHandle::close() {
	this->closeIfOpen();
}

bool DBIteratorHandle::closeIfOpen() {
	std::lock_guard<std::mutex> lock(this->iteratorMutex);
	DEBUG_LOG("%p DBIteratorHandle::close dbHandle=%p dbDescriptor=%p\n", this, this->dbHandle.get(), this->dbHandle->descriptor.get());
	if (!this->iterator) return false;
	this->iterator->Reset();
	this->iterator.reset();
	this->unboundedIterator.reset();
	if (this->txnHandle && this->transactionRegistered) {
		this->transactionRegistered = false;
		auto txnHandle = std::move(this->txnHandle);
		txnHandle->unregisterIterator(this);
	}
	return true;
}

void DBIteratorHandle::setRange(DBIteratorOptions& options) {
	if (options.startKeyStr != nullptr) {
		this->startKeyStr.assign(options.startKeyStr + options.startKeyStart, options.startKeyEnd - options.startKeyStart);
	} else {
		this->startKeyStr.clear();
	}
	this->startKey = rocksdb::Slice(this->startKeyStr);

	if (options.endKeyStr != nullptr) {
		this->endKeyStr.assign(options.endKeyStr + options.endKeyStart, options.endKeyEnd - options.endKeyStart);
		if (options.inclusiveEnd) {
			this->endKeyStr.push_back('\0');
		}
	} else {
		this->endKeyStr.clear();
	}
	this->endKey = rocksdb::Slice(this->endKeyStr);
}

void DBIteratorHandle::reuse(DBIteratorOptions& options) {
	this->exclusiveStart = options.exclusiveStart;
	this->inclusiveEnd = options.inclusiveEnd;
	this->reverse = options.reverse;
	this->values = options.values;
	this->needsStableValueBuffer = options.needsStableValueBuffer;
	this->setRange(options);
	this->seek();
	if (!this->iterator->status().ok()) {
		this->fallBackToBounded(nullptr);
	}
}

void DBIteratorHandle::detachFromDescriptor() noexcept {
	if (this->attached.exchange(false) && this->dbHandle && this->dbHandle->descriptor) {
		this->dbHandle->descriptor->detach(static_cast<Closable*>(this));
	}
}

void DBIteratorHandle::endRange() noexcept {
	if (this->boundedFallback) {
		this->iterator = std::move(this->unboundedIterator);
		this->boundedFallback = false;
	}
}

bool DBIteratorHandle::readsColumnOf(const std::shared_ptr<DBHandle>& target) const {
	// Control-block identity, without the atomic increment of lock(): a
	// recreated family is a new descriptor and never shares it.
	return !this->column.owner_before(target->columnDescriptor)
		&& !target->columnDescriptor.owner_before(this->column);
}

void DBIteratorHandle::advanceFrom(const rocksdb::Slice& currentKey) {
	if (!this->pooled || this->boundedFallback) {
		this->advance();
		return;
	}
	if (this->reverse) {
		this->iterator->Prev();
	} else {
		this->iterator->Next();
	}
	// Valid() implies an OK status, and is far cheaper to ask.
	if (!this->iterator->Valid() && !this->iterator->status().ok()) {
		this->fallBackToBounded(&currentKey);
	}
}

void DBIteratorHandle::fallBackToBounded(const rocksdb::Slice* resumeAfter) {
	auto column = this->column.lock();
	if (!column || !this->txnHandle || !this->txnHandle->txn) {
		// Leave the failed pooled iterator in place: its non-OK status is reported.
		return;
	}
	DBIteratorOptions boundedOptions;
	boundedOptions.setReadOptionDefaults();
	boundedOptions.readOptions.snapshot = this->txnHandle->txn->GetSnapshot();
	if (this->startKey.size() > 0) {
		boundedOptions.readOptions.iterate_lower_bound = &this->startKey;
	}
	if (this->endKey.size() > 0) {
		boundedOptions.readOptions.iterate_upper_bound = &this->endKey;
	}
	std::unique_ptr<rocksdb::Iterator> bounded(
		this->txnHandle->txn->GetIterator(boundedOptions.readOptions, column->column.get())
	);
	++iteratorPoolStats.fallbacks;
	this->txnHandle->pooledFallbacks.fetch_add(1, std::memory_order_relaxed);
	this->unboundedIterator = std::move(this->iterator);
	this->iterator = std::move(bounded);
	this->boundedFallback = true;

	if (resumeAfter == nullptr) {
		this->seek();
		return;
	}
	const rocksdb::Slice& resume = *resumeAfter;
	if (this->reverse) {
		this->iterator->SeekForPrev(resume);
		if (this->iterator->Valid() && this->iterator->key().compare(resume) == 0) {
			this->iterator->Prev();
		}
	} else {
		this->iterator->Seek(resume);
		if (this->iterator->Valid() && this->iterator->key().compare(resume) == 0) {
			this->iterator->Next();
		}
	}
}

void DBIteratorHandle::init(DBIteratorOptions& options) {
	if (options.startKeyStr != nullptr) {
		this->startKeyStr = std::string(options.startKeyStr + options.startKeyStart, options.startKeyEnd - options.startKeyStart);
		this->startKey = rocksdb::Slice(this->startKeyStr);
		options.readOptions.iterate_lower_bound = &this->startKey;

		DEBUG_LOG("%p DBIteratorHandle::init Start key:", this);
		DEBUG_LOG_KEY_LN(this->startKey);
	} else {
		DEBUG_LOG("%p DBIteratorHandle::init No start key\n", this);
	}

	if (options.endKeyStr != nullptr) {
		this->endKeyStr = std::string(options.endKeyStr + options.endKeyStart, options.endKeyEnd - options.endKeyStart);
		if (options.inclusiveEnd) {
			this->endKeyStr.push_back('\0');
		}
		this->endKey = rocksdb::Slice(this->endKeyStr);
		options.readOptions.iterate_upper_bound = &this->endKey;

		DEBUG_LOG("%p DBIteratorHandle::init End key:", this);
		DEBUG_LOG_KEY_LN(this->endKey);
	} else {
		DEBUG_LOG("%p DBIteratorHandle::init No end key\n", this);
	}
}

bool DBIteratorHandle::countRemaining(uint64_t& count) {
	std::lock_guard<std::mutex> lock(this->iteratorMutex);
	const DBDescriptor* descriptor = this->dbHandle->descriptor.get();
	// Test-only: stretch the scan so a fixture can land a foreign destroy()
	// inside it rather than only before or after.
	const int rowDelayMs = countScanDelayMsFlag().load(std::memory_order_relaxed);
	count = 0;
	while (this->valid()) {
		if (descriptor->isClosing()) {
			return false;
		}
		if (rowDelayMs > 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(rowDelayMs));
		}
		++count;
		this->advance();
	}
	return true;
}

void DBIteratorHandle::seek() {
	if (this->reverse) {
		if (this->endKey.size() > 0) {
			this->iterator->SeekForPrev(this->endKey);
			if (this->iterator->Valid() && this->iterator->key().compare(this->endKey) == 0) {
				this->iterator->Prev();
			}
		} else {
			this->iterator->SeekToLast();
		}
	} else {
		if (this->startKey.size() > 0) {
			this->iterator->Seek(this->startKey);
		} else {
			this->iterator->SeekToFirst();
		}
	}

	if (this->exclusiveStart && this->startKey.size() > 0 && this->iterator->Valid()) {
		rocksdb::Slice currentKey = this->iterator->key();
		if (currentKey.compare(this->startKey) == 0) {
			if (this->reverse) {
				this->iterator->Prev();
			} else {
				this->iterator->Next();
			}
		}
	}
}

bool DBIteratorHandle::valid() const {
	if (!this->iterator || !this->iterator->Valid()) {
		return false;
	}
	if (!this->enforceBounds) {
		return true;
	}

	const rocksdb::Slice key = this->iterator->key();
	if (this->reverse) {
		if (this->startKey.size() == 0) {
			return true;
		}
		const int comparison = key.compare(this->startKey);
		return comparison > 0 || (comparison == 0 && !this->exclusiveStart);
	}
	return this->endKey.size() == 0 || key.compare(this->endKey) < 0;
}

}
