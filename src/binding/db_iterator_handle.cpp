#include "db_iterator_handle.h"
#include "db_descriptor.h"
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
	values(options.values)
{
	DEBUG_LOG("%p DBIteratorHandle::Constructor dbHandle=%p\n", this, dbHandle.get());
	this->init(options);

	this->iterator = std::unique_ptr<rocksdb::Iterator>(
		dbHandle->descriptor->db->NewIterator(
			options.readOptions,
			dbHandle->column.get()
		)
	);

	this->seek(options);
}

DBIteratorHandle::DBIteratorHandle(
	TransactionHandle* txnHandle,
	DBIteratorOptions& options
) :
	dbHandle(txnHandle->dbHandle),
	exclusiveStart(options.exclusiveStart),
	inclusiveEnd(options.inclusiveEnd),
	reverse(options.reverse),
	values(options.values)
{
	DEBUG_LOG("DBIteratorHandle::Constructor txnHandle=%p dbDescriptor=%p\n", txnHandle, dbHandle->descriptor.get());
	this->init(options);

	this->iterator = std::unique_ptr<rocksdb::Iterator>(
		txnHandle->txn->GetIterator(
			options.readOptions,
			txnHandle->dbHandle->column.get()
		)
	);

	this->seek(options);
}

DBIteratorHandle::~DBIteratorHandle() {
	this->close();
}

void DBIteratorHandle::close() {
	DEBUG_LOG("%p DBIteratorHandle::close dbHandle=%p dbDescriptor=%p\n", this, this->dbHandle.get(), this->dbHandle->descriptor.get());
	if (this->iterator) {
		if (this->dbHandle && this->dbHandle->descriptor) {
			this->dbHandle->descriptor->detach(this);
		}
		this->iterator->Reset();
		this->iterator.reset();
	}
}

void DBIteratorHandle::init(DBIteratorOptions& options) {
	if (options.startKeyStr != nullptr) {
		this->startKey = rocksdb::Slice(options.startKeyStr + options.startKeyStart, options.startKeyEnd - options.startKeyStart);
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

	this->dbHandle->descriptor->attach(this);
}

void DBIteratorHandle::seek(DBIteratorOptions& options) {
	if (options.reverse) {
		this->iterator->SeekToLast();
	} else {
		this->iterator->SeekToFirst();
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

}