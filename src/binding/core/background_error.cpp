#include "core/background_error.h"
#include "core/json.h"

namespace rocksdb_js {

std::string backgroundErrorToJson(
	std::string_view message,
	int severity,
	std::string_view severityName,
	bool writesDisabled,
	int reason,
	std::string_view reasonName
) {
	std::string json = "{\"type\":\"background\",\"message\":";
	appendJsonString(json, message);
	json += ",\"severity\":" + std::to_string(severity);
	json += ",\"severityName\":";
	appendJsonString(json, severityName);
	json += ",\"writesDisabled\":";
	json += writesDisabled ? "true" : "false";
	if (reason >= 0) {
		json += ",\"reason\":" + std::to_string(reason);
		json += ",\"reasonName\":";
		appendJsonString(json, reasonName);
	}
	json += '}';
	return json;
}

const char* backgroundErrorSeverityName(int severity) {
	switch (severity) {
		case rocksdb::Status::Severity::kNoError:
			return "none";
		case rocksdb::Status::Severity::kSoftError:
			return "soft";
		case rocksdb::Status::Severity::kHardError:
			return "hard";
		case rocksdb::Status::Severity::kFatalError:
			return "fatal";
		case rocksdb::Status::Severity::kUnrecoverableError:
			return "unrecoverable";
		default:
			return "unknown";
	}
}

bool backgroundErrorDisablesWrites(int severity) {
	return severity >= static_cast<int>(rocksdb::Status::Severity::kHardError);
}

const char* backgroundErrorReasonName(int reason) {
	switch (reason) {
		case static_cast<int>(rocksdb::BackgroundErrorReason::kFlush):
			return "flush";
		case static_cast<int>(rocksdb::BackgroundErrorReason::kCompaction):
			return "compaction";
		case static_cast<int>(rocksdb::BackgroundErrorReason::kWriteCallback):
			return "writeCallback";
		case static_cast<int>(rocksdb::BackgroundErrorReason::kMemTable):
			return "memtable";
		case static_cast<int>(rocksdb::BackgroundErrorReason::kManifestWrite):
			return "manifestWrite";
		case static_cast<int>(rocksdb::BackgroundErrorReason::kFlushNoWAL):
			return "flushNoWAL";
		case static_cast<int>(rocksdb::BackgroundErrorReason::kManifestWriteNoWAL):
			return "manifestWriteNoWAL";
		case static_cast<int>(rocksdb::BackgroundErrorReason::kAsyncFileOpen):
			return "asyncFileOpen";
		default:
			return "unknown";
	}
}

} // namespace rocksdb_js
