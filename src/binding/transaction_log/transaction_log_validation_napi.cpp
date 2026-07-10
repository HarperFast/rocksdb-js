#include "transaction_log/transaction_log_validation_napi.h"
#include "transaction_log/transaction_log_validation.h"
#include "database/db_handle.h"
#include "napi/async.h"
#include "napi/macros.h"
#include <memory>
#include <string>
#include <vector>

namespace rocksdb_js {

namespace {

/**
 * State for the `validateTransactionLog` async work. There is no open database
 * involved, so the base handle is null (matching the module-level backup ops).
 */
struct AsyncValidateLogState final : BaseAsyncState<std::shared_ptr<DBHandle>> {
	std::string path;
	bool strict = false;
	TransactionLogStoreValidation result;
	/** Non-empty when execution failed with a hard error (e.g. missing directory). */
	std::string errorMessage;

	AsyncValidateLogState(napi_env env, std::string path) :
		BaseAsyncState<std::shared_ptr<DBHandle>>(env, nullptr),
		path(std::move(path)) {}
};

napi_status createStringArray(napi_env env, const std::vector<std::string>& strings, napi_value& result) {
	napi_status status = ::napi_create_array_with_length(env, strings.size(), &result);
	if (status != napi_ok) {
		return status;
	}
	for (size_t i = 0; i < strings.size(); i++) {
		napi_value value;
		status = ::napi_create_string_utf8(env, strings[i].c_str(), strings[i].size(), &value);
		if (status != napi_ok) {
			return status;
		}
		status = ::napi_set_element(env, result, static_cast<uint32_t>(i), value);
		if (status != napi_ok) {
			return status;
		}
	}
	return napi_ok;
}

#define SET_VALIDATION_PROP(obj, key, createExpr) \
	do { \
		napi_value _value; \
		NAPI_STATUS_THROWS_VOID(createExpr); \
		NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, obj, key, _value)); \
	} while (0)

/**
 * Converts the plain validation result into the JS object resolved to the
 * caller (see `TransactionLogStoreValidation` in `src/validate-transaction-log.ts`).
 */
void buildValidationResult(napi_env env, const TransactionLogStoreValidation& store, napi_value& result) {
	NAPI_STATUS_THROWS_VOID(::napi_create_object(env, &result));

	SET_VALIDATION_PROP(result, "path", ::napi_create_string_utf8(env, store.path.c_str(), store.path.size(), &_value));
	SET_VALIDATION_PROP(result, "valid", ::napi_get_boolean(env, store.valid, &_value));
	SET_VALIDATION_PROP(result, "errors", createStringArray(env, store.errors, _value));
	SET_VALIDATION_PROP(result, "warnings", createStringArray(env, store.warnings, _value));

	napi_value files;
	NAPI_STATUS_THROWS_VOID(::napi_create_array_with_length(env, store.files.size(), &files));
	for (size_t i = 0; i < store.files.size(); i++) {
		const auto& file = store.files[i];
		napi_value fileObj;
		NAPI_STATUS_THROWS_VOID(::napi_create_object(env, &fileObj));
		SET_VALIDATION_PROP(fileObj, "file", ::napi_create_string_utf8(env, file.file.c_str(), file.file.size(), &_value));
		SET_VALIDATION_PROP(fileObj, "sequence", ::napi_create_uint32(env, file.sequenceNumber, &_value));
		SET_VALIDATION_PROP(fileObj, "size", ::napi_create_double(env, static_cast<double>(file.size), &_value));
		SET_VALIDATION_PROP(fileObj, "entries", ::napi_create_uint32(env, file.result.entryCount, &_value));
		SET_VALIDATION_PROP(fileObj, "validBytes", ::napi_create_uint32(env, file.result.validBytes, &_value));
		SET_VALIDATION_PROP(fileObj, "valid", ::napi_get_boolean(env, file.result.valid, &_value));
		SET_VALIDATION_PROP(fileObj, "errors", createStringArray(env, file.result.errors, _value));
		SET_VALIDATION_PROP(fileObj, "warnings", createStringArray(env, file.result.warnings, _value));
		NAPI_STATUS_THROWS_VOID(::napi_set_element(env, files, static_cast<uint32_t>(i), fileObj));
	}
	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, result, "files", files));
}

/**
 * Validates a transaction log store directory on a worker thread.
 *
 * Signature: `validateTransactionLog(resolve, reject, path, strict)`
 */
napi_value ValidateTransactionLog(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(4);
	napi_value resolve = argv[0];
	napi_value reject = argv[1];
	NAPI_GET_STRING(argv[2], path, "Transaction log store path must be a string");

	auto state = new AsyncValidateLogState(env, std::move(path));

	napi_valuetype strictType;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[3], &strictType));
	if (strictType == napi_boolean) {
		NAPI_STATUS_THROWS(::napi_get_value_bool(env, argv[3], &state->strict));
	}

	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef));
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef));

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, "transactionLog.validate", NAPI_AUTO_LENGTH, &name));

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,
		nullptr,
		name,
		[](napi_env, void* data) { // execute (worker thread, no N-API)
			auto state = reinterpret_cast<AsyncValidateLogState*>(data);
			try {
				state->result = validateTransactionLogStore(state->path, state->strict);
			} catch (const std::exception& e) {
				state->errorMessage = e.what();
			}
			state->signalExecuteCompleted();
		},
		[](napi_env env, napi_status status, void* data) { // complete (JS thread)
			auto state = reinterpret_cast<AsyncValidateLogState*>(data);
			state->deleteAsyncWork();
			if (status != napi_cancelled) {
				if (state->errorMessage.empty()) {
					napi_value result = nullptr;
					buildValidationResult(env, state->result, result);
					if (result != nullptr) {
						state->callResolve(result);
					}
				} else {
					napi_value message;
					napi_value error;
					NAPI_STATUS_THROWS_VOID(::napi_create_string_utf8(
						env, state->errorMessage.c_str(), state->errorMessage.size(), &message));
					NAPI_STATUS_THROWS_VOID(::napi_create_error(env, nullptr, message, &error));
					state->callReject(error);
				}
			}
			delete state;
		},
		state,
		&state->asyncWork
	));

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork));

	NAPI_RETURN_UNDEFINED();
}

} // namespace

void initTransactionLogValidationExports(napi_env env, napi_value exports) {
	napi_value fn;
	NAPI_STATUS_THROWS_VOID(::napi_create_function(
		env, "validateTransactionLog", NAPI_AUTO_LENGTH, ValidateTransactionLog, nullptr, &fn));
	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, "validateTransactionLog", fn));
}

} // namespace rocksdb_js
