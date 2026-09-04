#include "iterator/db_iterator_handle.h"
#include "database/db_descriptor.h"
#include <thread>

namespace rocksdb_js {

DBIteratorHandle::DBIteratorHandle(
	std::shared_ptr<DBHandle> dbHandle,
	DBIteratorOptions& options
) :
	dbHandle(dbHandle),
	exclusiveStart(options.exclusiveStart),
	inclusiveEnd(options.inclusiveEnd),
	reverse(options.reverse),
	values(options.values),
	needsStableValueBuffer(options.needsStableValueBuffer)
{
	DEBUG_LOG("%p DBIteratorHandle::Constructor dbHandle=%p\n", this, dbHandle.get());
	this->init(options);

	this->iterator = std::unique_ptr<rocksdb::Iterator>(
		dbHandle->descriptor->db->NewIterator(
			options.readOptions,
			dbHandle->getColumnFamilyHandle()
		)
	);

	this->seek(options);
}

DBIteratorHandle::DBIteratorHandle(
	std::shared_ptr<TransactionHandle> txnHandle,
	DBIteratorOptions& options,
	std::shared_ptr<DBHandle> dbHandleOverride
) :
	dbHandle(dbHandleOverride ? dbHandleOverride : txnHandle->dbHandle),
	txnHandle(std::move(txnHandle)),
	exclusiveStart(options.exclusiveStart),
	inclusiveEnd(options.inclusiveEnd),
	reverse(options.reverse),
	values(options.values),
	needsStableValueBuffer(options.needsStableValueBuffer)
{
	DEBUG_LOG("DBIteratorHandle::Constructor txnHandle=%p dbDescriptor=%p\n", this->txnHandle.get(), dbHandle->descriptor.get());
	this->txnHandle->ensureSnapshot();
	if (this->txnHandle->snapshotSet) {
		options.readOptions.snapshot = this->txnHandle->txn->GetSnapshot();
	}
	this->init(options);

	this->iterator = std::unique_ptr<rocksdb::Iterator>(
		this->txnHandle->txn->GetIterator(
			options.readOptions,
			this->dbHandle->getColumnFamilyHandle()
		)
	);

	this->seek(options);
}

DBIteratorHandle::~DBIteratorHandle() {
	this->close();
}

void DBIteratorHandle::close() {
	std::lock_guard<std::mutex> lock(this->closeMutex);
	DEBUG_LOG("%p DBIteratorHandle::close dbHandle=%p dbDescriptor=%p\n", this, this->dbHandle.get(), this->dbHandle->descriptor.get());
	if (this->iterator) {
		this->iterator->Reset();
		this->iterator.reset();
	}
	if (this->txnHandle && this->transactionRegistered) {
		this->transactionRegistered = false;
		auto txnHandle = std::move(this->txnHandle);
		txnHandle->unregisterIterator(this);
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

void DBIteratorHandle::seek(DBIteratorOptions& options) {
	if (options.reverse) {
		if (this->endKey.size() > 0) {
			this->iterator->SeekForPrev(this->endKey);
			if (!options.inclusiveEnd && this->iterator->Valid()
				&& this->iterator->key().compare(this->endKey) == 0) {
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

	if (options.exclusiveStart && options.startKeyStr != nullptr && this->iterator->Valid()) {
		rocksdb::Slice currentKey = this->iterator->key();
		if (currentKey.compare(this->startKey) == 0) {
			if (options.reverse) {
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

	const rocksdb::Slice key = this->iterator->key();
	if (this->reverse && this->startKey.size() > 0) {
		const int comparison = key.compare(this->startKey);
		return comparison > 0 || (comparison == 0 && !this->exclusiveStart);
	}
	if (!this->reverse && this->endKey.size() > 0) {
		return key.compare(this->endKey) < 0;
	}
	return true;
}

void DBIteratorHandle::advance() {
	if (this->reverse) {
		this->iterator->Prev();
	} else {
		this->iterator->Next();
	}
}

}
