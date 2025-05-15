#include "db_handle.h"
#include "db_iterator.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

struct DBIteratorOptions {
	rocksdb::ReadOptions readOptions;
};

struct DBIteratorHandle final : Closable {
	DBIteratorHandle(std::shared_ptr<DBHandle> dbHandle, DBIteratorOptions& options)
		: dbDescriptor(dbHandle->descriptor)
	{
		this->iterator = std::unique_ptr<rocksdb::Iterator>(
			dbHandle->descriptor->db->NewIterator(
				options.readOptions,
				dbHandle->column.get()
			)
		);
		dbHandle->descriptor->attach(this);
		this->iterator->SeekToFirst();
	}

	~DBIteratorHandle() {
		this->close();
	}

	void close() override {
		if (this->iterator) {
			this->dbDescriptor->detach(this);
			this->iterator->Reset();
			this->iterator.reset();
		}
	}

	std::shared_ptr<DBDescriptor> dbDescriptor;
	std::unique_ptr<rocksdb::Iterator> iterator;
};

/**
 * Initialize the constructor reference for the `NativeIterator` class. We need
 * to do this because the constructor is static and we need to access it in the
 * static methods.
 */
napi_ref DBIterator::constructor = nullptr;

napi_value DBIterator::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR_ARGV("Iterator", 2)

	// TODO: args[0] could be either a database or a transaction

	std::shared_ptr<DBHandle>* dbHandle = nullptr;
	NAPI_STATUS_THROWS(::napi_unwrap(env, args[0], reinterpret_cast<void**>(&dbHandle)))

	if (dbHandle == nullptr || !(*dbHandle)->opened()) {
		::napi_throw_error(env, nullptr, "Database not open");
		return nullptr;
	}

	napi_value options = args[1];

	rocksdb::ReadOptions readOptions;

	readOptions.adaptive_readahead = true;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "adaptiveReadahead", readOptions.adaptive_readahead));

	readOptions.async_io = true;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "asyncIO", readOptions.async_io));

	readOptions.auto_readahead_size = true;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "autoReadaheadSize", readOptions.auto_readahead_size));

	readOptions.background_purge_on_iterator_cleanup = true;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "backgroundPurgeOnIteratorCleanup", readOptions.background_purge_on_iterator_cleanup));

	readOptions.fill_cache = false;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "fillCache", readOptions.fill_cache));

	readOptions.ignore_range_deletions = false;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "ignoreRangeDeletions", readOptions.ignore_range_deletions));

	readOptions.readahead_size = 0;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "readaheadSize", readOptions.readahead_size));

	readOptions.tailing = false;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "tailing", readOptions.tailing));

	bool exclusiveStart = false;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "exclusiveStart", exclusiveStart));

	bool inclusiveEnd = false;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "inclusiveEnd", inclusiveEnd));

	rocksdb::Slice startKey = nullptr;
	rocksdb_js::getKeyFromProperty(env, options, "start", startKey, "Start key is required", exclusiveStart, inclusiveEnd);
	if (startKey != nullptr) {
		readOptions.iterate_lower_bound = &startKey;
	}

	rocksdb::Slice endKey = nullptr;
	rocksdb_js::getKeyFromProperty(env, options, "end", endKey, "End key is required", exclusiveStart, inclusiveEnd);
	if (endKey != nullptr) {
		readOptions.iterate_upper_bound = &endKey;
	}

	DBIteratorOptions itOptions = { readOptions };
	DBIteratorHandle* itHandle = new DBIteratorHandle(*dbHandle, itOptions);

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(itHandle),
			[](napi_env env, void* data, void* hint) {
				DBIteratorHandle* itHandle = reinterpret_cast<DBIteratorHandle*>(data);
				delete itHandle;
			},
			nullptr, // finalize_hint
			nullptr  // result
		));

		return jsThis;
	} catch (const std::exception& e) {
		delete itHandle;
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}
}

napi_value DBIterator::Next(napi_env env, napi_callback_info info) {
	NAPI_METHOD()

	DBIteratorHandle* itHandle = nullptr;
	NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&itHandle)))

	if (itHandle->iterator == nullptr) {
		::napi_throw_error(env, nullptr, "Iterator not initialized");
		return nullptr;
	}

	napi_value result;
	napi_value resultDone;
	napi_value resultValue;
	NAPI_STATUS_THROWS(::napi_create_object(env, &result))

	if (itHandle->iterator->Valid()) {
		NAPI_STATUS_THROWS(::napi_get_boolean(env, false, &resultDone))

		napi_value key;
		napi_value value;

		rocksdb::Slice keySlice = itHandle->iterator->key();
		NAPI_STATUS_THROWS(::napi_create_buffer_copy(
			env,
			keySlice.size(),
			keySlice.data(),
			nullptr,
			&key
		))

		rocksdb::Slice valueSlice = itHandle->iterator->value();
		// TODO: use a shared buffer
		NAPI_STATUS_THROWS(::napi_create_buffer_copy(
			env,
			valueSlice.size(),
			valueSlice.data(),
			nullptr,
			&value
		))

		NAPI_STATUS_THROWS(::napi_create_object(env, &resultValue))
		NAPI_STATUS_THROWS(::napi_set_named_property(env, resultValue, "key", key))
		NAPI_STATUS_THROWS(::napi_set_named_property(env, resultValue, "value", value))
		// TODO: add version?

		itHandle->iterator->Next();
	} else {
		NAPI_STATUS_THROWS(::napi_get_boolean(env, true, &resultDone))
		NAPI_STATUS_THROWS(::napi_get_undefined(env, &resultValue))
	}

	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "done", resultDone))
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "value", resultValue))

	return result;
}

napi_value DBIterator::Return(napi_env env, napi_callback_info info) {
	NAPI_METHOD()

	DBIteratorHandle* itHandle = nullptr;
	NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&itHandle)))

	// TODO: abort transaction?

	itHandle->close();

	napi_value result;
	napi_value done;
	napi_value value;
	NAPI_STATUS_THROWS(::napi_create_object(env, &result))
	NAPI_STATUS_THROWS(::napi_get_boolean(env, true, &done))
	NAPI_STATUS_THROWS(::napi_get_undefined(env, &value))
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "done", done))
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "value", value))

	return result;
}

napi_value DBIterator::Throw(napi_env env, napi_callback_info info) {
	NAPI_METHOD()

	fprintf(stderr, "Throw\n");

	DBIteratorHandle* itHandle = nullptr;
	NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&itHandle)))

	// TODO: abort transaction?

	itHandle->close();

	napi_value result;
	napi_value done;
	napi_value value;
	NAPI_STATUS_THROWS(::napi_create_object(env, &result))
	NAPI_STATUS_THROWS(::napi_get_boolean(env, true, &done))
	NAPI_STATUS_THROWS(::napi_get_undefined(env, &value))
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "done", done))
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "value", value))
	return result;
}

void DBIterator::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "next", nullptr, Next, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "return", nullptr, Return, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "throw", nullptr, Throw, nullptr, nullptr, nullptr, napi_default, nullptr },
	};

	auto className = "Iterator";
	constexpr size_t len = sizeof("Iterator") - 1;

	napi_value cons;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		className,    // className
		len,          // length of class name
		Constructor,  // constructor
		nullptr,      // constructor arg
		sizeof(properties) / sizeof(napi_property_descriptor), // number of properties
		properties,   // properties array
		&cons         // [out] constructor
	))

	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, cons, 1, &constructor))

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, cons))
}

}
