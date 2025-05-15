#include "db_cursor.h"
#include "db_handle.h"
#include "db_iterator.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

napi_value DBCursor::Constructor(napi_env env, napi_callback_info info) {
    NAPI_CONSTRUCTOR_ARGV("Cursor", 1)

    void* ptr = nullptr;
    NAPI_STATUS_THROWS(::napi_get_value_external(env, args[0], &ptr));
	std::shared_ptr<DBHandle> dbHandle = *reinterpret_cast<std::shared_ptr<DBHandle>*>(ptr);

	if (!dbHandle || !dbHandle->opened()) {
		::napi_throw_error(env, nullptr, "Database not open");
		return nullptr;
	}

	DBIterator* iterator = new DBIterator(dbHandle);

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(iterator),
			[](napi_env env, void* data, void* hint) {
				DBIterator* iterator = reinterpret_cast<DBIterator*>(data);
				delete iterator;
			},
			nullptr, // finalize_hint
			nullptr  // result
		));

		return jsThis;
	} catch (const std::exception& e) {
		delete iterator;
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}
}

void DBCursor::Init(napi_env env, napi_value exports) {
    napi_property_descriptor properties[] = {
		{ "next", nullptr, Next, nullptr, nullptr, nullptr, napi_default, nullptr },
	};

	constexpr auto className = "Cursor";
	napi_value cons;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		className,              // className
		sizeof(className) - 1,  // length of class name (subtract 1 for null terminator)
		Constructor,            // constructor
		nullptr,                // constructor arg
		sizeof(properties) / sizeof(napi_property_descriptor), // number of properties
		properties,             // properties array
		&cons                   // [out] constructor
	))

	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, cons, 1, &constructor))

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, cons))
}

}
