#include <node_api.h>
#include <string>
#include <sstream>
#include "util.h"

namespace rocksdb_js {

static const char* errorCodeStrings[] = {
	"ERR_UNKNOWN",
	"ERR_NOT_FOUND",
	"ERR_CORRUPTION",
	"ERR_NOT_SUPPORTED",
	"ERR_INVALID_ARGUMENT",
	"ERR_IO_ERROR",
	"ERR_MERGE_IN_PROGRESS",
	"ERR_INCOMPLETE",
	"ERR_SHUTDOWN_IN_PROGRESS",
	"ERR_TIMED_OUT",
	"ERR_ABORTED",
	"ERR_BUSY",
	"ERR_EXPIRED",
	"ERR_TRY_AGAIN",
	"ERR_COMPACTION_TOO_LARGE",
	"ERR_COLUMN_FAMILY_DROPPED"
};

/**
 * Creates a new JavaScript error object from a RocksDB status.
 */
void createRocksDBError(napi_env env, rocksdb::Status status, const char* msg, napi_value& error) {
	ROCKSDB_STATUS_FORMAT_ERROR(status, msg)

	napi_value global;
	napi_value objectCtor;
	napi_value objectCreateFn;
	napi_value errorCtor;
	napi_value errorProto;
	napi_value errorCode;
	napi_value errorMsg;
	napi_value errorName;

	NAPI_STATUS_THROWS_VOID(::napi_get_global(env, &global))
	NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, global, "Object", &objectCtor))
	NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, objectCtor, "create", &objectCreateFn))
	NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, global, "Error", &errorCtor))
	NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, errorCtor, "prototype", &errorProto))

	const char* codeStr;
	switch (status.code()) {
		case rocksdb::Status::Code::kNotFound: codeStr = errorCodeStrings[1]; break;
		case rocksdb::Status::Code::kCorruption: codeStr = errorCodeStrings[2]; break;
		case rocksdb::Status::Code::kNotSupported: codeStr = errorCodeStrings[3]; break;
		case rocksdb::Status::Code::kInvalidArgument: codeStr = errorCodeStrings[4]; break;
		case rocksdb::Status::Code::kIOError: codeStr = errorCodeStrings[5]; break;
		case rocksdb::Status::Code::kMergeInProgress: codeStr = errorCodeStrings[6]; break;
		case rocksdb::Status::Code::kIncomplete: codeStr = errorCodeStrings[7]; break;
		case rocksdb::Status::Code::kShutdownInProgress: codeStr = errorCodeStrings[8]; break;
		case rocksdb::Status::Code::kTimedOut: codeStr = errorCodeStrings[9]; break;
		case rocksdb::Status::Code::kAborted: codeStr = errorCodeStrings[10]; break;
		case rocksdb::Status::Code::kBusy: codeStr = errorCodeStrings[11]; break;
		case rocksdb::Status::Code::kExpired: codeStr = errorCodeStrings[12]; break;
		case rocksdb::Status::Code::kTryAgain: codeStr = errorCodeStrings[13]; break;
		case rocksdb::Status::Code::kCompactionTooLarge: codeStr = errorCodeStrings[14]; break;
		case rocksdb::Status::Code::kColumnFamilyDropped: codeStr = errorCodeStrings[15]; break;
		default: codeStr = errorCodeStrings[0]; break;
	}
	NAPI_STATUS_THROWS_VOID(::napi_create_string_utf8(env, codeStr, NAPI_AUTO_LENGTH, &errorCode))

	NAPI_STATUS_THROWS_VOID(::napi_create_string_utf8(env, errorStr.c_str(), errorStr.size(), &errorMsg))

	napi_value createArgs[1] = { errorProto };
	NAPI_STATUS_THROWS_VOID(::napi_call_function(env, objectCtor, objectCreateFn, 1, createArgs, &error))
	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, error, "code", errorCode))
	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, error, "message", errorMsg))
}

}