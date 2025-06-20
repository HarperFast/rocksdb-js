#include "db_iterator_handle.h"

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
	DEBUG_LOG("%p DBIteratorHandle::Constructor dbHandle=%p\n", this, dbHandle.get())
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
	DEBUG_LOG("DBIteratorHandle::Constructor txnHandle=%p dbDescriptor=%p\n", txnHandle, dbHandle->descriptor.get())
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
	DEBUG_LOG("%p DBIteratorHandle::close() dbHandle=%p dbDescriptor=%p\n", this, this->dbHandle.get(), this->dbHandle->descriptor.get())
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

#ifdef DEBUG
		fprintf(stderr, "[%04zu] Start Key:", std::hash<std::thread::id>{}(std::this_thread::get_id()) % 10000);
		for (size_t i = 0; i < this->startKey.size(); i++) {
			fprintf(stderr, " %02x", (unsigned char)this->startKey.data()[i]);
		}
		fprintf(stderr, "\n");
	} else {
		DEBUG_LOG("No start key\n");
#endif
	}

	if (options.endKeyStr != nullptr) {
		this->endKeyStr = std::string(options.endKeyStr + options.endKeyStart, options.endKeyEnd - options.endKeyStart);
		if (options.inclusiveEnd) {
			this->endKeyStr.push_back('\0');
		}
		this->endKey = rocksdb::Slice(this->endKeyStr);
		options.readOptions.iterate_upper_bound = &this->endKey;

#ifdef DEBUG
		fprintf(stderr, "[%04zu]   End Key:", std::hash<std::thread::id>{}(std::this_thread::get_id()) % 10000);
		for (size_t i = 0; i < this->endKey.size(); i++) {
			fprintf(stderr, " %02x", (unsigned char)this->endKey.data()[i]);
		}
		fprintf(stderr, "\n");
	} else {
		DEBUG_LOG("No end key\n");
#endif
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